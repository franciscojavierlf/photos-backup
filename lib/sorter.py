import hashlib
import json
import logging
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from lib.config import MEDIA_EXT, TEMP_ROOT, PHOTOS_DIR, DB_PATH, UNDATED_DIR, DATA_DIR

_HASH_CHUNK = 8 * 1024 * 1024  # 8 MB for faster hashing on large files

def _metadata_candidates(p: Path) -> list[Path]:
    base_candidates = [
        p.with_suffix(p.suffix + '.json'),
        p.with_suffix('.json'),
        p.with_suffix(p.suffix + '.supplemental-metadata.json'),
        p.with_suffix(p.suffix + '.suppl.json'),
    ]
    # Also consider duplicated download variants like filename.jpg(1).json etc.
    variants: list[Path] = []
    for sc in base_candidates:
        variants.append(sc)
        # Insert (1) before final .json
        if sc.suffix.lower() == '.json':
            name_no_ext = sc.stem  # e.g., filename.jpg or filename
            parent = sc.parent
            variants.append(parent / f"{name_no_ext}(1).json")
        # Insert (1) before .supplemental-metadata.json or .suppl.json
        if sc.name.endswith('.supplemental-metadata.json'):
            stem_for_ins = sc.name[:-len('.json')]
            variants.append(sc.parent / f"{stem_for_ins}(1).json")
        if sc.name.endswith('.suppl.json'):
            stem_for_ins = sc.name[:-len('.json')]
            variants.append(sc.parent / f"{stem_for_ins}(1).json")
    # Remove duplicates while preserving order
    seen = set()
    uniq: list[Path] = []
    for x in variants:
        key = x.as_posix()
        if key not in seen:
            seen.add(key)
            uniq.append(x)
    return uniq


def sort_media():
    """ Moves all the temp files into the photos by sorting them. """
    conn = _ensure_db(DB_PATH)
    _import_from_temp(conn, TEMP_ROOT, PHOTOS_DIR)

    # Prune empty directories left behind in this extracted folder
    try:
        pruned = _prune_empty_dirs(DATA_DIR)
        if pruned:
            logging.info(f"[CLEAN] pruned {pruned} empty folders in {DATA_DIR.name}")
    except Exception as e:
        logging.warning(f"[CLEAN] prune failed for {DATA_DIR.name}: {e}")


def reindex_library(lib_dir: Path):
    """ Recreates the sqlite db with the current photos folder. """
    conn = _ensure_db(DB_PATH)
    logging.info(f"[REINDEX] scanning {lib_dir.name}")
    added, errors = 0, 0
    for p in lib_dir.rglob('*'):
        if not p.is_file():
            continue
        if p.suffix.lower() not in MEDIA_EXT:
            continue
        try:
            h = _sha256_file(p)
            size = p.stat().st_size
            mtime = int(p.stat().st_mtime)
            conn.execute(
                "INSERT OR REPLACE INTO files(hash,size,path,mtime) VALUES(?,?,?,?)",
                (h, size, str(p), mtime)
            )
            added += 1
        except Exception as e:
            logging.warning(f"[REINDEX] fail {p.name}: {e}")
            errors += 1
    conn.commit()
    logging.info(f"[REINDEX] done. indexed={added} errors={errors}")


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _hash_info(path_str: str) -> tuple[str, str, int, int]:
    """Compute hash and gather basic stat for a file path string.
    Returns (path_str, sha256_hex, size, mtime_int).
    """
    p = Path(path_str)
    h = _sha256_file(p)
    st = p.stat()
    return path_str, h, st.st_size, int(st.st_mtime)


def _ensure_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS files(
        hash TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        path TEXT NOT NULL,
        mtime INTEGER NOT NULL
      )
    """)
    conn.execute("PRAGMA journal_mode=WAL;")
    logging.info(f"DB ready at {db_path.name}")
    return conn


def _unique_dest(dest_dir: Path, name: str) -> Path:
    base, ext = os.path.splitext(name)
    cand = dest_dir / name
    i = 1
    while cand.exists():
        cand = dest_dir / f"{base}_{i}{ext}"
        i += 1
    return cand


def _load_takeout_timestamp(sidecar: Path):
    try:
        with sidecar.open('r', encoding='utf-8') as f:
            j = json.load(f)
        ts = None
        if isinstance(j, dict):
            # Prefer explicit photoTakenTime, then creationTime, then top-level timestamp
            pt = j.get('photoTakenTime') or j.get('creationTime') or {}
            ts = pt.get('timestamp') if isinstance(pt, dict) else None
            if not ts and 'timestamp' in j:
                ts = j['timestamp']
        if ts:
            return datetime.utcfromtimestamp(int(ts))
    except Exception:
        pass
    return None


def _best_datetime_for_file(p: Path):
    # Returns (dt, reliable). reliable=True if dt comes from sidecar metadata.
    # Support Google Takeout sidecar variants:
    #   filename.ext.json
    #   filename.json
    #   filename.ext.supplemental-metadata.json

    for sc in _metadata_candidates(p):
        if sc.exists():
            dt = _load_takeout_timestamp(sc)
            if dt:
                return dt, True
    # Fall back to file mtime but mark as unreliable
    return datetime.fromtimestamp(p.stat().st_mtime), False


def _handle_duplicate_source(src: Path):
    # If the content hash already exists in DB, delete source (and sidecars)
    try:
        # delete sidecars first (if present)
        for sc in _metadata_candidates(src):
            if sc.exists():
                sc.unlink(missing_ok=True)
        src.unlink(missing_ok=True)
        logging.info(f"[DUP] deleted temp duplicate: {src.name}")
    except Exception as e:
        logging.warning(f"[DUP] failed to delete temp duplicate {src.name}: {e}")


def _prune_empty_dirs(root: Path) -> int:
    """Recursively remove empty directories under root. Returns count removed.
    Only operates within the provided root and ignores errors.
    """
    removed = 0
    # Walk bottom-up so parents are considered after children
    for d in sorted((p for p in root.rglob('*') if p.is_dir()), key=lambda x: len(x.as_posix()), reverse=True):
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
        # Apply faster SQLite PRAGMAs for bulk insert (session-scoped during this dir)
        try:
            conn.execute("PRAGMA synchronous=OFF;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-60000;")
            conn.execute("PRAGMA mmap_size=268435456;")
            conn.execute("PRAGMA locking_mode=EXCLUSIVE;")
        except Exception:
            pass
        batch = 0
        insert_sql = "INSERT OR REPLACE INTO files(hash,size,path,mtime) VALUES(?,?,?,?)"
        conn.execute("BEGIN")

        # Collect media files in this extracted dir (unsorted to keep overhead low)
        media_files = [p for p in ed.rglob('*') if p.is_file() and p.suffix.lower() in MEDIA_EXT]
        workers = max(1, (os.cpu_count() or 2) - 1)
        # Parallelize hashing while keeping IO moves & DB writes in main process
        with ProcessPoolExecutor(max_workers=workers) as ex:
            for path_str, h, size_src, mtime_src in ex.map(_hash_info, (str(p) for p in media_files), chunksize=8):
                src = Path(path_str)
                try:
                    cur = conn.execute("SELECT path FROM files WHERE hash=?", (h,))
                    row = cur.fetchone()
                    if row:
                        dupes += 1
                        logging.info(f"[SKIP DUP] already in DB -> {Path(row[0]).name}")
                        _handle_duplicate_source(src)
                        continue

                    dt, reliable = _best_datetime_for_file(src)
                    if reliable:
                        dest_dir = lib_dir / dt.strftime('%Y') / dt.strftime('%m')
                    else:
                        dest_dir = UNDATED_DIR
                    dest_dir.mkdir(parents=True, exist_ok=True)

                    # Use the original name; avoid collisions by suffixing
                    dest_media = _unique_dest(dest_dir, src.name)

                    if reliable:
                        logging.info(f"[ADD] {dest_media.name}")
                    else:
                        logging.info(f"[ADD-UNDATED] {dest_media.name}")
                    shutil.move(str(src), str(dest_media))

                    # Set the file's mtime only if we had a reliable datetime
                    if reliable:
                        ts = int(dt.timestamp())
                        try:
                            os.utime(dest_media, (ts, ts))
                        except Exception:
                            pass
                        mtime_db = ts
                    else:
                        mtime_db = mtime_src

                    # Remove associated sidecars from temp (treat as trash)
                    for sc in _metadata_candidates(src):
                        if sc.exists():
                            sc.unlink(missing_ok=True)

                    conn.execute(insert_sql, (h, size_src, str(dest_media), mtime_db))
                    added += 1
                    batch += 1
                    if batch >= 1000:
                        conn.commit()
                        conn.execute("BEGIN")
                        batch = 0
                except Exception as e:
                    logging.warning(f"[ERR] import fail {src.name}: {e}")
                    errors += 1

        # After processing one extracted directory, delete any leftover JSON files as noise
        try:
            removed = 0
            for j in ed.rglob('*.json'):
                try:
                    j.unlink(missing_ok=True)
                    removed += 1
                except Exception:
                    pass
            if removed:
                logging.info(f"[CLEAN] removed {removed} JSON sidecars/noise in {ed.name}")
        except Exception as e:
            logging.warning(f"[CLEAN] failed for {ed.name}: {e}")
        # Finalize any pending DB changes for this directory
        try:
            conn.commit()
        except Exception:
            pass
        processed += 1
    logging.info(f"[SUMMARY] dirs={processed} added={added} dupes={dupes} errors={errors}")
