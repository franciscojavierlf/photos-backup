import hashlib
import json
import logging
import os
import shutil
import sys
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterable
from lib.config import TEMP_ROOT, MEDIA_EXT, DATA_DIR, BASE_DIR, ARCHIVE_SUFFIXES

def extract_zip_files():
    archives = [p for p in DATA_DIR.iterdir() if p.is_file() and p.name.lower().endswith(ARCHIVE_SUFFIXES)]
    archives.sort(key=lambda x: x.name.lower())

    if not archives:
        logging.info("No archives found.")
        return

    logging.info(f"Found {len(archives)} archives")
    ok, fail = 0, 0
    for arc in archives:
        if _extract_one_archive(arc):
            ok += 1
        else:
            fail += 1
    logging.info(f"[SUMMARY] extracted_ok={ok} extracted_failed={fail}")


def _extract_one_archive(archive: Path) -> bool:
    out_dir = _extract_dir_for(archive)
    out_dir.mkdir(parents=True, exist_ok=True)
    fp = _archive_fingerprint(archive)

    wrote = 0
    try:
        if archive.name.lower().endswith(".zip"):
            with zipfile.ZipFile(archive, "r") as zf:
                wrote = _safe_extract_zip_resumable(zf, out_dir, zf.infolist())
        else:
            mode = "r:gz" if archive.name.lower().endswith((".tar.gz", ".tgz")) else "r"
            with tarfile.open(archive, mode) as tf:
                wrote = _safe_extract_tar_resumable(tf, out_dir, tf.getmembers())

        # If at least one media/sidecar exists in out_dir, consider extraction successful.
        # Write meta, then delete the archive to save space.
        media_count = sum(1 for p in out_dir.rglob("*") if p.is_file())
        if media_count > 0:
            archive.unlink(missing_ok=False)
            logging.info(f"[OK] extracted -> {out_dir} | wrote_new={wrote} | deleted archive: {archive.name}")
            return True
        else:
            logging.warning(f"[WARN] nothing extracted from {archive.name} (no media/sidecars found)")
            return False
    except Exception as e:
        logging.warning(f"[ERR] extraction failed for {archive.name}: {e}")
        return False


def _is_media_or_sidecar(name: str) -> bool:
    lower = name.lower()
    if lower.endswith('.json'):
        return True
    _, ext = os.path.splitext(lower)
    return ext in MEDIA_EXT


def _extract_dir_for(arc: Path) -> Path:
    safe = arc.name.replace('.', '_')
    return TEMP_ROOT / f"extract_{safe}"


def _archive_fingerprint(p: Path) -> str:
    st = p.stat()
    payload = f"{p.name}:{st.st_size}:{int(st.st_mtime)}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _safe_extract_zip_resumable(zf: zipfile.ZipFile, dest_dir: Path, names: Iterable[zipfile.ZipInfo]) -> int:
    wrote = 0
    for info in names:
        if info.is_dir():
            continue
        name = info.filename
        if not _is_media_or_sidecar(name):
            continue
        target = dest_dir / os.path.normpath(name)
        if not str(target.resolve()).startswith(str(dest_dir.resolve())):
            logging.warning(f"[zip] skip suspicious path: {name}")
            continue
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"Exttracting {info}")
        with zf.open(info, "r") as src, target.open("wb") as dst:
            shutil.copyfileobj(src, dst)
        wrote += 1
    return wrote


def _safe_extract_tar_resumable(tf: tarfile.TarFile, dest_dir: Path, members: Iterable[tarfile.TarInfo]) -> int:
    wrote = 0
    for m in members:
        if not m.isfile():
            continue
        name = m.name
        if not _is_media_or_sidecar(name):
            continue
        target = dest_dir / os.path.normpath(name)
        if not str(target.resolve()).startswith(str(dest_dir.resolve())):
            logging.warning(f"[tar] skip suspicious path: {name}")
            continue
        if target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with tf.extractfile(m) as src:
            if src is None:
                continue
            with target.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        wrote += 1
    return wrote
