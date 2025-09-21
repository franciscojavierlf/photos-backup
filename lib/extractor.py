import logging
import os
import shutil
import zipfile
from pathlib import Path
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from lib.config import TEMP_ROOT, MEDIA_EXT, DATA_DIR, ARCHIVE_SUFFIXES, MAX_ARCHIVE_WORKERS, MAX_WITHIN_ARCHIVE_WORKERS, PARALLEL_MIN_FILES

_COPY_CHUNK = 8 * 1024 * 1024  # 8 MB chunk for faster extraction I/O


def _is_media(name: str) -> bool:
    lower = name.lower()
    _, ext = os.path.splitext(lower)
    return ext in MEDIA_EXT


def _is_sidecar_json(name: str) -> bool:
    lower = name.lower()
    if not lower.endswith('.json'):
        return False
    base = os.path.basename(lower)
    # Skip typical folder/album-level metadata files which we don't need
    if base in {'metadata.json', 'metadata(1).json', 'album-metadata.json', 'album-metadata(1).json', 'albums.json'}:
        return False
    if base.startswith(('metadata', 'album', 'albums', 'shared', 'archive', 'index', 'folder', 'photos-metadata')):
        return False
    # Allow strong sidecar patterns
    if base.endswith(('.supplemental-metadata.json', '.suppl.json')):
        return True
    # Allow *.ext.json or *.ext(1).json forms
    for mext in MEDIA_EXT:
        if base.endswith(mext + '.json') or base.endswith(mext + '(1).json'):
            return True
    # Fallback: keep other JSONs (e.g., filename.json sidecar) to preserve date metadata
    return True

def extract_zip_files():
    archives = [p for p in DATA_DIR.iterdir() if p.is_file() and p.name.lower().endswith(ARCHIVE_SUFFIXES)]
    archives.sort(key=lambda x: x.name.lower())

    if not archives:
        logging.info("No archives found.")
        return

    logging.info(f"Found {len(archives)} archives")
    ok, fail = 0, 0
    # Extract multiple archives in parallel (bounded workers)
    max_workers = min(len(archives), MAX_ARCHIVE_WORKERS)
    if max_workers <= 1:
        for arc in archives:
            if _extract_one_archive(arc):
                ok += 1
            else:
                fail += 1
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(_extract_one_archive, arc): arc for arc in archives}
            for fut in as_completed(futs):
                try:
                    if fut.result():
                        ok += 1
                    else:
                        fail += 1
                except Exception:
                    fail += 1
    logging.info(f"[SUMMARY] extracted_ok={ok} extracted_failed={fail}")


def _extract_one_archive(archive: Path) -> bool:
    out_dir = _extract_dir_for(archive)
    out_dir.mkdir(parents=True, exist_ok=True)

    wrote = 0
    wrote_media = 0
    wrote_sidecar = 0
    skipped_existing = 0
    skipped_noise_json = 0
    try:
        if archive.name.lower().endswith(".zip"):
            # Precompute destination root once for traversal checks
            dest_root = str(out_dir.resolve())
            with zipfile.ZipFile(archive, "r") as zf:
                all_infos = [zi for zi in zf.infolist() if not zi.is_dir()]
                infos = [zi for zi in all_infos if _is_media_or_sidecar(zi.filename)]
                # Count JSONs we intentionally skip as noise
                skipped_noise_json = sum(1 for zi in all_infos if zi.filename.lower().endswith('.json') and not _is_sidecar_json(zi.filename))
            # Parallelize extraction of members using multiple ZipFile handles
            workers = min(MAX_WITHIN_ARCHIVE_WORKERS, len(infos))
            if workers <= 1 or len(infos) < PARALLEL_MIN_FILES:
                # Small archives: do sequential to avoid overhead
                with zipfile.ZipFile(archive, "r") as zf2:
                    wrote, wrote_media, wrote_sidecar, skipped_existing = _safe_extract_zip_resumable(zf2, out_dir, infos, dest_root)
            else:
                # Use filenames only to avoid sharing ZipInfo objects across threads
                names = [zi.filename for zi in infos]
                wrote, wrote_media, wrote_sidecar, skipped_existing = _extract_members_parallel(archive, out_dir, names, dest_root, workers)
        else:
            logging.error(f"File is not zip: {archive.name}")

        # If at least one file was written, consider extraction successful and delete archive
        if wrote > 0:
            archive.unlink(missing_ok=False)
            logging.info(f"[OK] wrote_new={wrote} | media={wrote_media} sidecars={wrote_sidecar} skipped_existing={skipped_existing} skipped_noise_json={skipped_noise_json} | deleted archive: {archive.name}")
            return True
        else:
            logging.warning(f"[WARN] nothing extracted from {archive.name} (no media/sidecars found)")
            return False
    except Exception as e:
        logging.warning(f"[ERR] extraction failed for {archive.name}: {e}")
        return False


def _is_media_or_sidecar(name: str) -> bool:
    if _is_media(name):
        return True
    if _is_sidecar_json(name):
        return True
    return False


def _extract_dir_for(arc: Path) -> Path:
    safe = arc.name.replace('.', '_')
    return TEMP_ROOT / f"extract_{safe}"


def _safe_extract_zip_resumable(zf: zipfile.ZipFile, dest_dir: Path, names: Iterable[zipfile.ZipInfo], dest_root: str) -> tuple[int, int, int, int]:
    wrote = 0
    wrote_media = 0
    wrote_sidecar = 0
    skipped_existing = 0
    for info in names:
        if info.is_dir():
            continue
        name = info.filename
        if not _is_media_or_sidecar(name):
            continue
        target = dest_dir / os.path.normpath(name)
        # Ensure the resolved path stays under the destination root (zip-slip protection)
        if not str(target.resolve()).startswith(dest_root):
            logging.warning(f"[zip] skip suspicious path: {name}")
            continue
        if target.exists():
            skipped_existing += 1
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"Extracting {info.filename}")
        with zf.open(info, "r") as src, target.open("wb") as dst:
            shutil.copyfileobj(src, dst, length=_COPY_CHUNK)
        wrote += 1
        if _is_media(name):
            wrote_media += 1
        elif _is_sidecar_json(name):
            wrote_sidecar += 1
    return wrote, wrote_media, wrote_sidecar, skipped_existing


def _extract_members_parallel(archive: Path, dest_dir: Path, names: list[str], dest_root: str, workers: int) -> tuple[int, int, int, int]:
    """
    Extract a list of member names from one zip archive in parallel.
    Each worker opens its own ZipFile handle and processes a disjoint subset.
    Returns a tuple: (wrote_total, wrote_media, wrote_sidecar, skipped_existing).
    """
    def _worker(sublist: list[str]) -> tuple[int, int, int, int]:
        wrote_local = 0
        wrote_media_local = 0
        wrote_sidecar_local = 0
        skipped_existing_local = 0
        with zipfile.ZipFile(archive, "r") as zf:
            for name in sublist:
                try:
                    info = zf.getinfo(name)
                except KeyError:
                    continue
                target = dest_dir / os.path.normpath(name)
                # Zip-slip protection
                if not str(target.resolve()).startswith(dest_root):
                    logging.warning(f"[zip] skip suspicious path: {name}")
                    continue
                if target.exists():
                    skipped_existing_local += 1
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                logging.info(f"Extracting {info.filename}")
                with zf.open(info, "r") as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst, length=_COPY_CHUNK)
                wrote_local += 1
                if _is_media(name):
                    wrote_media_local += 1
                elif _is_sidecar_json(name):
                    wrote_sidecar_local += 1
        return wrote_local, wrote_media_local, wrote_sidecar_local, skipped_existing_local

    if workers <= 1:
        return _worker(names)

    # Split names into roughly equal chunks for workers
    chunks: list[list[str]] = [[] for _ in range(workers)]
    for i, n in enumerate(names):
        chunks[i % workers].append(n)

    wrote = 0
    wrote_media = 0
    wrote_sidecar = 0
    skipped_existing = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for w, wm, ws, se in ex.map(_worker, chunks):
            wrote += w
            wrote_media += wm
            wrote_sidecar += ws
            skipped_existing += se
    return wrote, wrote_media, wrote_sidecar, skipped_existing
