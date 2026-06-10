import hashlib
import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

from lib.config import DATA_DIR, DB_PATH, MEDIA_EXT, PHOTOS_DIR, TEMP_ROOT, UNDATED_DIR

_HASH_CHUNK = 8 * 1024 * 1024  # 8 MB for faster hashing on large files
_REINDEX_BATCH_SIZE = 1000
_REINDEX_LOG_EVERY = 1000


def sort_media():
    """Import any leftover staged media from TEMP_ROOT."""
    conn = ensure_db(DB_PATH)
    try:
        _import_from_temp(conn, TEMP_ROOT, PHOTOS_DIR)
    finally:
        conn.close()

    try:
        pruned = _prune_empty_dirs(DATA_DIR)
        if pruned:
            logging.info(f"[CLEAN] pruned {pruned} empty folders in {DATA_DIR.name}")
    except Exception as e:
        logging.warning(f"[CLEAN] prune failed for {DATA_DIR.name}: {e}")


def reindex_library():
    """Recreate the sqlite db from the current photos folder."""
    conn = ensure_db(DB_PATH)
    try:
        logging.info(f"[REINDEX] scanning {PHOTOS_DIR.name}")
        total_files, total_bytes = _scan_media_totals(PHOTOS_DIR)
        logging.info(f"[REINDEX] total_files={total_files}")
        conn.execute("PRAGMA synchronous=OFF;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("DELETE FROM files")
        conn.commit()

        added, errors, processed_bytes = 0, 0, 0
        batch: list[tuple[str, int, str, int]] = []
        conn.execute("BEGIN IMMEDIATE")
        for p in _iter_media_files(PHOTOS_DIR):
            try:
                h = _hash_file(p)
                stat = p.stat()
                batch.append((h, stat.st_size, str(p), int(stat.st_mtime)))
                added += 1
                processed_bytes += stat.st_size

                if len(batch) >= _REINDEX_BATCH_SIZE:
                    conn.executemany("INSERT INTO files(hash,size,path,mtime) VALUES(?,?,?,?)", batch)
                    batch.clear()

                if added % _REINDEX_LOG_EVERY == 0:
                    pct = 100.0 if total_bytes == 0 else (processed_bytes * 100.0 / total_bytes)
                    logging.info(f"[REINDEX] {pct:.1f}% files={added}/{total_files}")
            except Exception as e:
                logging.warning(f"[REINDEX] fail {p.name}: {e}")
                errors += 1

        if batch:
            conn.executemany("INSERT INTO files(hash,size,path,mtime) VALUES(?,?,?,?)", batch)
        conn.commit()
        logging.info(f"[REINDEX] done. indexed={added}/{total_files} errors={errors}")
    finally:
        conn.close()


def ensure_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
      CREATE TABLE IF NOT EXISTS files(
        hash TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        path TEXT NOT NULL,
        mtime INTEGER NOT NULL
      )
    """
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    logging.info(f"DB ready at {db_path.name}")
    return conn


def metadata_candidate_names(name: str) -> list[str]:
    parent = os.path.dirname(name)
    base = os.path.basename(name)
    stem, _ = os.path.splitext(base)

    def _join(filename: str) -> str:
        return os.path.join(parent, filename) if parent else filename

    base_candidates = [
        _join(base + ".json"),
        _join(stem + ".json"),
        _join(base + ".supplemental-metadata.json"),
        _join(base + ".suppl.json"),
    ]

    variants: list[str] = []
    for candidate in base_candidates:
        variants.append(candidate)
        dirname = os.path.dirname(candidate)
        filename = os.path.basename(candidate)

        if filename.endswith(".json"):
            variant = os.path.join(dirname, f"{filename[:-5]}(1).json") if dirname else f"{filename[:-5]}(1).json"
            variants.append(variant)

    seen = set()
    uniq: list[str] = []
    for candidate in variants:
        if candidate not in seen:
            seen.add(candidate)
            uniq.append(candidate)
    return uniq


def _metadata_candidates_for_path(p: Path) -> list[Path]:
    return [p.parent / os.path.basename(candidate) for candidate in metadata_candidate_names(p.name)]


def import_media_file(
    conn: sqlite3.Connection,
    src: Path,
    *,
    sidecar_path: Path | None = None,
    sidecar_bytes: bytes | None = None,
    sidecar_bytes_candidates: list[bytes] | None = None,
    lib_dir: Path = PHOTOS_DIR,
) -> str:
    h = _hash_file(src)
    size_src = src.stat().st_size
    mtime_src = int(src.stat().st_mtime)

    cur = conn.execute("SELECT path FROM files WHERE hash=?", (h,))
    row = cur.fetchone()
    if row:
        logging.info(f"[SKIP DUP] already in DB -> {Path(row[0]).name}")
        _cleanup_staged_media(src, sidecar_path=sidecar_path)
        return "duplicate"

    dt, reliable = _best_datetime_for_file(
        src,
        sidecar_path=sidecar_path,
        sidecar_bytes=sidecar_bytes,
        sidecar_bytes_candidates=sidecar_bytes_candidates,
    )
    dest_dir = _destination_dir(lib_dir, dt, reliable)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_media = _unique_dest(dest_dir, src.name)

    if reliable:
        logging.info(f"[ADD] {dest_media.name}")
    else:
        logging.info(f"[ADD-UNDATED] {dest_media.name}")

    shutil.move(str(src), str(dest_media))

    if reliable:
        ts = int(dt.timestamp())
        try:
            os.utime(dest_media, (ts, ts))
        except Exception:
            pass
        mtime_db = ts
    else:
        mtime_db = mtime_src

    if sidecar_path is not None and sidecar_path.exists():
        sidecar_path.unlink(missing_ok=True)

    conn.execute(
        "INSERT OR REPLACE INTO files(hash,size,path,mtime) VALUES(?,?,?,?)",
        (h, size_src, str(dest_media), mtime_db),
    )
    conn.commit()
    return "added"


def is_known_duplicate(conn: sqlite3.Connection, src: Path) -> bool:
    h = _hash_file(src)
    cur = conn.execute("SELECT 1 FROM files WHERE hash=? LIMIT 1", (h,))
    return cur.fetchone() is not None


def discard_staged_media(src: Path, *, sidecar_path: Path | None = None):
    _cleanup_staged_media(src, sidecar_path=sidecar_path)


def _hash_file(p: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _iter_media_files(root: Path):
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            if os.path.splitext(filename)[1].lower() not in MEDIA_EXT:
                continue
            yield Path(dirpath) / filename


def _scan_media_totals(root: Path) -> tuple[int, int]:
    total_files = 0
    total_bytes = 0
    for path in _iter_media_files(root):
        try:
            total_files += 1
            total_bytes += path.stat().st_size
        except Exception:
            pass
    return total_files, total_bytes


def _unique_dest(dest_dir: Path, name: str) -> Path:
    base, ext = os.path.splitext(name)
    cand = dest_dir / name
    i = 1
    while cand.exists():
        cand = dest_dir / f"{base}_{i}{ext}"
        i += 1
    return cand


def _load_takeout_timestamp_bytes(data: bytes):
    try:
        j = json.loads(data.decode("utf-8"))
        ts = None
        if isinstance(j, dict):
            pt = j.get("photoTakenTime") or j.get("creationTime") or {}
            ts = pt.get("timestamp") if isinstance(pt, dict) else None
            if not ts and "timestamp" in j:
                ts = j["timestamp"]
        if ts:
            return datetime.utcfromtimestamp(int(ts))
    except Exception:
        pass
    return None


def _load_takeout_timestamp(sidecar: Path):
    try:
        return _load_takeout_timestamp_bytes(sidecar.read_bytes())
    except Exception:
        return None


def _best_datetime_for_file(
    p: Path,
    *,
    sidecar_path: Path | None = None,
    sidecar_bytes: bytes | None = None,
    sidecar_bytes_candidates: list[bytes] | None = None,
):
    inline_candidates: list[bytes] = []
    if sidecar_bytes is not None:
        inline_candidates.append(sidecar_bytes)
    if sidecar_bytes_candidates:
        inline_candidates.extend(sidecar_bytes_candidates)

    for inline_sidecar in inline_candidates:
        dt = _load_takeout_timestamp_bytes(inline_sidecar)
        if dt:
            return dt, True

    if sidecar_path is not None and sidecar_path.exists():
        dt = _load_takeout_timestamp(sidecar_path)
        if dt:
            return dt, True

    for sc in _metadata_candidates_for_path(p):
        if sidecar_path is not None and sc == sidecar_path:
            continue
        if sc.exists():
            dt = _load_takeout_timestamp(sc)
            if dt:
                return dt, True

    return datetime.fromtimestamp(p.stat().st_mtime), False


def _destination_dir(lib_dir: Path, dt: datetime, reliable: bool) -> Path:
    if reliable:
        return lib_dir / dt.strftime("%Y") / dt.strftime("%m")
    return UNDATED_DIR


def _cleanup_staged_media(src: Path, *, sidecar_path: Path | None = None):
    try:
        if sidecar_path is not None and sidecar_path.exists():
            sidecar_path.unlink(missing_ok=True)
        for sc in _metadata_candidates_for_path(src):
            if sc.exists():
                sc.unlink(missing_ok=True)
        src.unlink(missing_ok=True)
        logging.info(f"[DUP] deleted temp duplicate: {src.name}")
    except Exception as e:
        logging.warning(f"[DUP] failed to delete temp duplicate {src.name}: {e}")


def _prune_empty_dirs(root: Path) -> int:
    removed = 0
    for d in sorted((p for p in root.rglob("*") if p.is_dir()), key=lambda x: len(x.as_posix()), reverse=True):
        try:
            if not any(d.iterdir()):
                d.rmdir()
                removed += 1
        except Exception:
            pass
    return removed


def _import_from_temp(conn: sqlite3.Connection, temp_root: Path, lib_dir: Path):
    extract_dirs = sorted([p for p in temp_root.glob("extract_*") if p.is_dir()], key=lambda x: x.name.lower())
    if not extract_dirs:
        logging.info("No extracted folders found.")
        return

    processed, added, dupes, errors = 0, 0, 0, 0
    for ed in extract_dirs:
        logging.info(f"[DIR] importing {ed.name}")
        media_files = [p for p in ed.rglob("*") if p.is_file() and p.suffix.lower() in MEDIA_EXT]

        for src in media_files:
            try:
                result = import_media_file(conn, src, lib_dir=lib_dir)
                if result == "added":
                    added += 1
                elif result == "duplicate":
                    dupes += 1
            except Exception as e:
                logging.warning(f"[ERR] import fail {src.name}: {e}")
                errors += 1

        try:
            removed = 0
            for j in ed.rglob("*.json"):
                try:
                    j.unlink(missing_ok=True)
                    removed += 1
                except Exception:
                    pass
            if removed:
                logging.info(f"[CLEAN] removed {removed} JSON sidecars/noise in {ed.name}")
            shutil.rmtree(ed, ignore_errors=True)
        except Exception as e:
            logging.warning(f"[CLEAN] failed for {ed.name}: {e}")

        processed += 1

    logging.info(f"[SUMMARY] dirs={processed} added={added} dupes={dupes} errors={errors}")
