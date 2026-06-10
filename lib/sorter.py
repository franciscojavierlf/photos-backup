import hashlib
import json
import logging
import os
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from lib.config import DATA_DIR, DB_PATH, MEDIA_EXT, PHOTOS_DIR, TEMP_ROOT, UNDATED_DIR

_HASH_CHUNK = 8 * 1024 * 1024  # 8 MB for faster hashing on large files
_REINDEX_BATCH_SIZE = 1000
_REINDEX_LOG_EVERY = 1000


def sort_media():
    """Import any leftover staged media from TEMP_ROOT."""
    conn = ensure_db(DB_PATH)
    try:
        logging.info("[UNDATED] finalizing staged media from temp")
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

        added, errors, processed_bytes, duplicates = 0, 0, 0, 0
        batch: list[tuple[str, int, str, int]] = []
        seen_hashes: set[str] = set()
        conn.execute("BEGIN IMMEDIATE")
        for p in _iter_media_files(PHOTOS_DIR):
            try:
                h = _hash_file(p)
                stat = p.stat()
                if h in seen_hashes:
                    duplicates += 1
                else:
                    seen_hashes.add(h)
                batch.append((h, stat.st_size, str(p), int(stat.st_mtime)))
                added += 1
                processed_bytes += stat.st_size

                if len(batch) >= _REINDEX_BATCH_SIZE:
                    conn.executemany("INSERT OR REPLACE INTO files(hash,size,path,mtime) VALUES(?,?,?,?)", batch)
                    batch.clear()

                if added % _REINDEX_LOG_EVERY == 0:
                    pct = 100.0 if total_bytes == 0 else (processed_bytes * 100.0 / total_bytes)
                    logging.info(f"[REINDEX] {pct:.1f}% files={added}/{total_files} duplicates={duplicates}")
            except Exception as e:
                logging.warning(f"[REINDEX] fail {p.name}: {e}")
                errors += 1

        if batch:
            conn.executemany("INSERT OR REPLACE INTO files(hash,size,path,mtime) VALUES(?,?,?,?)", batch)
        conn.commit()
        logging.info(f"[REINDEX] done. indexed={added}/{total_files} duplicates={duplicates} errors={errors}")
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
    conn.execute(
        """
      CREATE TABLE IF NOT EXISTS discarded_media(
        source_key TEXT NOT NULL,
        folder_path TEXT NOT NULL,
        media_name TEXT NOT NULL,
        PRIMARY KEY(source_key, folder_path, media_name)
      )
    """
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    logging.info(f"DB ready at {db_path.name}")
    return conn


def sidecar_matches_media_name(sidecar_name: str, media_name: str) -> bool:
    sidecar_base = os.path.basename(sidecar_name)
    media_base = os.path.basename(media_name)
    sidecar_lower = sidecar_base.lower()

    if not sidecar_lower.endswith(".json"):
        return False

    duplicate_parts = _split_duplicate_suffix(media_base)
    if duplicate_parts is None:
        return sidecar_base.startswith(media_base)

    base_name, duplicate_suffix = duplicate_parts
    return sidecar_base.startswith(base_name) and sidecar_lower.endswith(f"{duplicate_suffix.lower()}.json")


def sidecar_matches_media_path(sidecar_name: str, media_name: str) -> bool:
    sidecar_parent = os.path.dirname(os.path.normpath(sidecar_name))
    media_parent = os.path.dirname(os.path.normpath(media_name))
    if sidecar_parent != media_parent:
        return False
    return sidecar_matches_media_name(os.path.basename(sidecar_name), os.path.basename(media_name))


def _matching_sidecars_for_path(p: Path) -> list[Path]:
    matches: list[Path] = []
    try:
        for candidate in p.parent.iterdir():
            if not candidate.is_file():
                continue
            if sidecar_matches_media_name(candidate.name, p.name):
                matches.append(candidate)
    except FileNotFoundError:
        return []
    return matches


def import_media_file(
    conn: sqlite3.Connection,
    src: Path,
    *,
    sidecar_path: Path | None = None,
    sidecar_bytes: bytes | None = None,
    sidecar_bytes_candidates: list[bytes] | None = None,
    lib_dir: Path | None = None,
) -> str:
    if lib_dir is None:
        lib_dir = PHOTOS_DIR

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


def record_discarded_media(conn: sqlite3.Connection, source_key: str, media_rel_name: str):
    folder_path, media_name = _split_rel_path(media_rel_name)
    conn.execute(
        "INSERT OR REPLACE INTO discarded_media(source_key,folder_path,media_name) VALUES(?,?,?)",
        (source_key, folder_path, media_name),
    )
    conn.commit()


def is_discarded_sidecar(conn: sqlite3.Connection, source_key: str, sidecar_rel_name: str) -> bool:
    folder_path, sidecar_name = _split_rel_path(sidecar_rel_name)
    rows = conn.execute(
        "SELECT media_name FROM discarded_media WHERE source_key=? AND folder_path=?",
        (source_key, folder_path),
    ).fetchall()
    return any(sidecar_matches_media_name(sidecar_name, media_name) for (media_name,) in rows)


def _hash_file(p: Path) -> str:
    h = hashlib.md5(usedforsecurity=False)
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _split_rel_path(rel_name: str) -> tuple[str, str]:
    norm = os.path.normpath(rel_name)
    return os.path.dirname(norm), os.path.basename(norm)


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
            return datetime.fromtimestamp(int(ts), UTC).replace(tzinfo=None)
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

    for sc in _matching_sidecars_for_path(p):
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
        for sc in _matching_sidecars_for_path(src):
            if sc.exists():
                sc.unlink(missing_ok=True)
        src.unlink(missing_ok=True)
        logging.info(f"[DUP] deleted temp duplicate: {src.name}")
    except Exception as e:
        logging.warning(f"[DUP] failed to delete temp duplicate {src.name}: {e}")


def _split_duplicate_suffix(name: str) -> tuple[str, str] | None:
    stem, ext = os.path.splitext(name)
    if not ext:
        return None

    open_paren = stem.rfind("(")
    if open_paren == -1 or not stem.endswith(")"):
        return None

    duplicate_num = stem[open_paren + 1 : -1]
    if not duplicate_num.isdigit():
        return None

    return f"{stem[:open_paren]}{ext}", f"({duplicate_num})"


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
