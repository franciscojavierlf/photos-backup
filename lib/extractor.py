import logging
import os
import shutil
import zipfile
from pathlib import Path

from lib.config import ARCHIVE_SUFFIXES, DATA_DIR, DB_PATH, MEDIA_EXT, TEMP_ROOT
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


def extract_zip_files():
    archives = [p for p in DATA_DIR.iterdir() if p.is_file() and p.name.lower().endswith(ARCHIVE_SUFFIXES)]
    archives.sort(key=lambda x: x.name.lower())

    if not archives:
        logging.info("No archives found.")
        return

    conn = sorter.ensure_db(DB_PATH)
    try:
        logging.info(f"Found {len(archives)} archives")
        ok, fail = 0, 0
        for archive in archives:
            if _extract_one_archive(conn, archive):
                ok += 1
            else:
                fail += 1
        logging.info(f"[SUMMARY] extracted_ok={ok} extracted_failed={fail}")
    finally:
        conn.close()


def _extract_one_archive(conn, archive: Path) -> bool:
    if not archive.name.lower().endswith(".zip"):
        logging.error(f"Unsupported archive type: {archive.name}")
        return False

    stage_dir = _extract_dir_for(archive)
    _reset_stage_dir(stage_dir)

    added = 0
    dupes = 0
    errors = 0
    skipped_noise_json = 0

    try:
        with zipfile.ZipFile(archive, "r") as zf:
            info_by_name = {zi.filename: zi for zi in zf.infolist() if not zi.is_dir()}
            media_infos = [zi for zi in info_by_name.values() if _is_media(zi.filename)]
            skipped_noise_json = sum(
                1 for name in info_by_name if name.lower().endswith(".json") and not _is_sidecar_json(name)
            )

            if not media_infos:
                logging.warning(f"[WARN] nothing extracted from {archive.name} (no media found)")
                return False

            for info in media_infos:
                try:
                    staged_media = _stage_member(zf, info, stage_dir)
                    sidecar_bytes = _read_sidecar_bytes(zf, info.filename, info_by_name)
                    result = sorter.import_media_file(conn, staged_media, sidecar_bytes=sidecar_bytes)
                    if result == "added":
                        added += 1
                    elif result == "duplicate":
                        dupes += 1
                except Exception as e:
                    logging.warning(f"[ERR] import fail {info.filename}: {e}")
                    errors += 1

        shutil.rmtree(stage_dir, ignore_errors=True)
        if errors == 0:
            archive.unlink(missing_ok=False)
            logging.info(
                f"[OK] added={added} dupes={dupes} errors={errors} "
                f"skipped_noise_json={skipped_noise_json} | deleted archive: {archive.name}"
            )
            return True

        logging.warning(
            f"[WARN] added={added} dupes={dupes} errors={errors} "
            f"skipped_noise_json={skipped_noise_json} | kept archive for rerun: {archive.name}"
        )
        return False
    except Exception as e:
        logging.warning(f"[ERR] extraction failed for {archive.name}: {e}")
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


def _stage_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo, stage_dir: Path) -> Path:
    target = stage_dir / os.path.normpath(info.filename)
    _ensure_within_root(target, stage_dir, info.filename)

    target.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f"Extracting {info.filename}")
    with zf.open(info, "r") as src, target.open("wb") as dst:
        shutil.copyfileobj(src, dst, length=_COPY_CHUNK)
    return target


def _read_sidecar_bytes(zf: zipfile.ZipFile, media_name: str, info_by_name: dict[str, zipfile.ZipInfo]) -> bytes | None:
    for candidate in sorter.metadata_candidate_names(media_name):
        info = info_by_name.get(candidate)
        if info is None:
            continue
        if not _is_sidecar_json(info.filename):
            continue
        with zf.open(info, "r") as src:
            return src.read()
    return None


def _ensure_within_root(target: Path, root: Path, name: str):
    target_abs = target.resolve()
    root_abs = root.resolve()
    if os.path.commonpath([str(target_abs), str(root_abs)]) != str(root_abs):
        raise ValueError(f"[zip] skip suspicious path: {name}")
