import argparse
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from lib.config import MEDIA_EXT, TEMP_ROOT, PHOTOS_DIR, DB_PATH

HASH_CHUNK = 1024 * 1024  # 1 MB

def sort_media():
    """ Moves all the temp files into the photos by sorting them. """
    conn = _ensure_db(DB_PATH)
    _import_from_temp(conn, TEMP_ROOT, PHOTOS_DIR)


def reindex_library(conn: sqlite3.Connection, lib_dir: Path):
    """ Recreates the sqlite db with the current photos folder. """
    conn = _ensure_db(DB_PATH)
    logging.info(f"[REINDEX] scanning {lib_dir}")
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
            logging.warning(f"[REINDEX] fail {p}: {e}")
            errors += 1
    conn.commit()
    logging.info(f"[REINDEX] done. indexed={added} errors={errors}")


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(HASH_CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


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
    logging.info(f"DB ready at {db_path}")
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
    sc1 = p.with_suffix(p.suffix + '.json')
    sc2 = p.with_suffix('.json')
    dt = None
    if sc1.exists():
        dt = _load_takeout_timestamp(sc1)
    if not dt and sc2.exists():
        dt = _load_takeout_timestamp(sc2)
    return dt or datetime.fromtimestamp(p.stat().st_mtime)


def _handle_duplicate_source(conn: sqlite3.Connection, src: Path):
    # If the content hash already exists in DB, delete source (and sidecars)
    try:
        # delete sidecars first (if present)
        for sc in (src.with_suffix(src.suffix + '.json'), src.with_suffix('.json')):
            if sc.exists():
                sc.unlink(missing_ok=True)
        src.unlink(missing_ok=True)
        logging.info(f"[DUP] deleted temp duplicate: {src}")
    except Exception as e:
        logging.warning(f"[DUP] failed to delete temp duplicate {src}: {e}")


def _import_from_temp(conn: sqlite3.Connection, temp_root: Path, lib_dir: Path):
    extract_dirs = sorted([p for p in temp_root.glob("extract_*") if p.is_dir()], key=lambda x: x.name.lower())
    if not extract_dirs:
        logging.info("No extracted folders found.")
        return

    processed, added, dupes, errors = 0, 0, 0, 0

    for ed in extract_dirs:
        # Iterate media files only
        media_files = sorted([p for p in ed.rglob('*') if p.is_file() and p.suffix.lower() in MEDIA_EXT],
                             key=lambda x: (x.parent.as_posix(), x.name.lower()))
        logging.info(f"[DIR] importing {ed} | files={len(media_files)}")

        for src in media_files:
            try:
                h = _sha256_file(src)
                cur = conn.execute("SELECT path FROM files WHERE hash=?", (h,))
                row = cur.fetchone()
                if row:
                    dupes += 1
                    logging.info(f"[SKIP DUP] already in DB -> {row[0]}")
                    _handle_duplicate_source(conn, src)
                    continue

                dt = _best_datetime_for_file(src)
                dest_dir = lib_dir / dt.strftime('%Y') / dt.strftime('%m')
                dest_dir.mkdir(parents=True, exist_ok=True)

                # Use original name; avoid collisions by suffixing
                dest_media = _unique_dest(dest_dir, src.name)

                logging.info(f"[ADD] {src} -> {dest_media}")
                shutil.move(str(src), str(dest_media))

                # Move associated sidecars, matching the final base name
                base_new, ext_new = os.path.splitext(dest_media.name)
                for sc in (src.with_suffix(src.suffix + '.json'), src.with_suffix('.json')):
                    if sc.exists():
                        # prefer same base name as media's new name if ext matched
                        sc_name = sc.name
                        if sc_name.startswith(src.stem):
                            sc_name = base_new + sc_name[len(src.stem):]
                        dest_sc = _unique_dest(dest_dir, sc_name)
                        shutil.move(str(sc), str(dest_sc))

                size = dest_media.stat().st_size
                mtime = int(dest_media.stat().st_mtime)
                conn.execute(
                    "INSERT OR REPLACE INTO files(hash,size,path,mtime) VALUES(?,?,?,?)",
                    (h, size, str(dest_media), mtime)
                )
                conn.commit()
                added += 1
            except Exception as e:
                logging.warning(f"[ERR] import fail {src}: {e}")
                errors += 1

        processed += 1

    logging.info(f"[SUMMARY] dirs={processed} added={added} dupes={dupes} errors={errors}")
