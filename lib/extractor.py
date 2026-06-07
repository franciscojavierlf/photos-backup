import logging
import os
import shutil
import zipfile
from pathlib import Path

from lib.config import ARCHIVE_SUFFIXES, DATA_DIR, MEDIA_EXT, TEMP_ROOT
import lib.sorter as sorter

_COPY_CHUNK = 8 * 1024 * 1024  # 8 MB chunk for faster extraction I/O


def _is_media(name: str) -> bool:
    _, ext = os.path.splitext(name.lower())
    return ext in MEDIA_EXT


def _is_sidecar_json(name: str) -> bool:
    lower = name.lower()
    if not lower.endswith(".json"):
        return False

    base = os.path.basename(lower)
    if base in {"metadata.json", "metadata(1).json", "album-metadata.json", "album-metadata(1).json", "albums.json"}:
        return False
    if base.startswith(("metadata", "album", "albums", "shared", "archive", "index", "folder", "photos-metadata")):
        return False
    if base.endswith((".supplemental-metadata.json", ".suppl.json")):
        return True

    for mext in MEDIA_EXT:
        if base.endswith(mext + ".json") or base.endswith(mext + "(1).json"):
            return True

    return True


def _is_media_or_sidecar(name: str) -> bool:
    return _is_media(name) or _is_sidecar_json(name)


def extract_zip_files():
    archives = [p for p in DATA_DIR.iterdir() if p.is_file() and p.name.lower().endswith(ARCHIVE_SUFFIXES)]
    archives.sort(key=lambda x: x.name.lower())

    if not archives:
        logging.info("No archives found.")
        return

    logging.info(f"Found {len(archives)} archives")
    ok, fail = 0, 0
    for archive in archives:
        if _extract_and_import_one_archive(archive):
            ok += 1
        else:
            fail += 1
    logging.info(f"[SUMMARY] extracted_ok={ok} extracted_failed={fail}")


def _extract_and_import_one_archive(archive: Path) -> bool:
    if not archive.name.lower().endswith(".zip"):
        logging.error(f"Unsupported archive type: {archive.name}")
        return False

    stage_dir = _extract_dir_for(archive)
    _reset_stage_dir(stage_dir)

    wrote = 0
    wrote_media = 0
    wrote_sidecar = 0
    skipped_existing = 0
    skipped_noise_json = 0

    try:
        with zipfile.ZipFile(archive, "r") as zf:
            all_infos = [zi for zi in zf.infolist() if not zi.is_dir()]
            infos = [zi for zi in all_infos if _is_media_or_sidecar(zi.filename)]
            skipped_noise_json = sum(
                1 for zi in all_infos if zi.filename.lower().endswith(".json") and not _is_sidecar_json(zi.filename)
            )

            dest_root = stage_dir.resolve()
            for info in infos:
                target = stage_dir / os.path.normpath(info.filename)
                _ensure_within_root(target, dest_root, info.filename)
                if target.exists():
                    skipped_existing += 1
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                logging.info(f"Extracting {info.filename}")
                with zf.open(info, "r") as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst, length=_COPY_CHUNK)
                wrote += 1
                if _is_media(info.filename):
                    wrote_media += 1
                else:
                    wrote_sidecar += 1

        if wrote == 0:
            logging.warning(f"[WARN] nothing extracted from {archive.name} (no media/sidecars found)")
            return False

        sorter.sort_media()
        archive.unlink(missing_ok=False)
        logging.info(
            f"[OK] wrote_new={wrote} | media={wrote_media} sidecars={wrote_sidecar} "
            f"skipped_existing={skipped_existing} skipped_noise_json={skipped_noise_json} "
            f"| deleted archive: {archive.name}"
        )
        return True
    except Exception as e:
        logging.warning(f"[ERR] extraction/import failed for {archive.name}: {e}")
        return False
    finally:
        try:
            shutil.rmtree(stage_dir, ignore_errors=True)
        except Exception:
            pass


def _extract_dir_for(archive: Path) -> Path:
    safe = archive.name.replace(".", "_")
    return TEMP_ROOT / f"extract_{safe}"


def _reset_stage_dir(stage_dir: Path):
    shutil.rmtree(stage_dir, ignore_errors=True)
    stage_dir.mkdir(parents=True, exist_ok=True)


def _ensure_within_root(target: Path, root: Path, name: str):
    target_abs = target.resolve()
    if os.path.commonpath([str(target_abs), str(root)]) != str(root):
        raise ValueError(f"[zip] skip suspicious path: {name}")
