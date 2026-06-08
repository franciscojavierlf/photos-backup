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


def _is_media_or_sidecar(name: str) -> bool:
    return _is_media(name) or _is_sidecar_json(name)


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
            if _extract_and_import_one_archive(conn, archive):
                ok += 1
            else:
                fail += 1
        logging.info(f"[SUMMARY] extracted_ok={ok} extracted_failed={fail}")
    finally:
        conn.close()


def _extract_and_import_one_archive(conn, archive: Path) -> bool:
    if not archive.name.lower().endswith(".zip"):
        logging.error(f"Unsupported archive type: {archive.name}")
        return False

    stage_dir = _extract_dir_for(archive)
    _reset_stage_dir(stage_dir)

    pending_media: dict[str, Path] = {}
    pending_media_candidates: dict[str, str] = {}
    pending_sidecars: dict[str, Path] = {}
    duplicate_media_candidates: set[str] = set()

    wrote = 0
    wrote_media = 0
    wrote_sidecar = 0
    skipped_existing = 0
    skipped_noise_json = 0
    added = 0
    dupes = 0
    undated = 0
    errors = 0

    try:
        with zipfile.ZipFile(archive, "r") as zf:
            all_infos = [zi for zi in zf.infolist() if not zi.is_dir()]
            infos = [zi for zi in all_infos if _is_media_or_sidecar(zi.filename)]
            skipped_noise_json = sum(
                1 for zi in all_infos if zi.filename.lower().endswith(".json") and not _is_sidecar_json(zi.filename)
            )

            dest_root = stage_dir.resolve()
            for info in infos:
                rel_name = os.path.normpath(info.filename)
                target = stage_dir / rel_name
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
                    result = _register_media(
                        conn,
                        target,
                        rel_name,
                        pending_media,
                        pending_media_candidates,
                        pending_sidecars,
                        duplicate_media_candidates,
                    )
                else:
                    wrote_sidecar += 1
                    result = _register_sidecar(
                        conn,
                        target,
                        rel_name,
                        pending_media,
                        pending_media_candidates,
                        pending_sidecars,
                        duplicate_media_candidates,
                    )

                if result == "added":
                    added += 1
                elif result == "duplicate":
                    dupes += 1
                elif result == "error":
                    errors += 1

        if wrote == 0:
            logging.warning(f"[WARN] nothing extracted from {archive.name} (no media/sidecars found)")
            return False

        for rel_name, media_path in list(pending_media.items()):
            try:
                logging.info(f"[ZIP-END] no sidecar match for {media_path.name}; finalizing to dated folder or _undated")
                result = sorter.import_media_file(conn, media_path)
                if result == "added":
                    undated += 1
                elif result == "duplicate":
                    dupes += 1
                _unregister_media(rel_name, pending_media, pending_media_candidates)
            except Exception as e:
                logging.warning(f"[ERR] finalize undated fail {media_path.name}: {e}")
                errors += 1

        for sidecar_path in pending_sidecars.values():
            try:
                logging.info(f"[ZIP-END] discarding unmatched sidecar: {sidecar_path.name}")
                sidecar_path.unlink(missing_ok=True)
            except Exception:
                pass
        pending_sidecars.clear()

        if errors == 0:
            archive.unlink(missing_ok=False)
            logging.info(
                f"[OK] wrote_new={wrote} | media={wrote_media} sidecars={wrote_sidecar} "
                f"added={added} dupes={dupes} undated={undated} "
                f"skipped_existing={skipped_existing} skipped_noise_json={skipped_noise_json} "
                f"| deleted archive: {archive.name}"
            )
            return True

        logging.warning(
            f"[WARN] wrote_new={wrote} | media={wrote_media} sidecars={wrote_sidecar} "
            f"added={added} dupes={dupes} undated={undated} errors={errors} "
            f"skipped_existing={skipped_existing} skipped_noise_json={skipped_noise_json} "
            f"| kept archive for rerun: {archive.name}"
        )
        return False
    except Exception as e:
        logging.warning(f"[ERR] extraction/import failed for {archive.name}: {e}")
        return False
    finally:
        try:
            shutil.rmtree(stage_dir, ignore_errors=True)
        except Exception:
            pass


def _register_media(
    conn,
    media_path: Path,
    rel_name: str,
    pending_media: dict[str, Path],
    pending_media_candidates: dict[str, str],
    pending_sidecars: dict[str, Path],
    duplicate_media_candidates: set[str],
) -> str | None:
    if sorter.is_known_duplicate(conn, media_path):
        logging.info(f"[DUP-EARLY] media already in DB: {media_path.name}")
        for candidate in sorter.metadata_candidate_names(rel_name):
            duplicate_media_candidates.add(candidate)
            sidecar_path = pending_sidecars.pop(candidate, None)
            if sidecar_path is not None:
                logging.info(f"[DUP-EARLY] discarding cached sidecar for duplicate media: {sidecar_path.name}")
                sidecar_path.unlink(missing_ok=True)
        sorter.discard_staged_media(media_path)
        return "duplicate"

    for candidate in sorter.metadata_candidate_names(rel_name):
        sidecar_path = pending_sidecars.pop(candidate, None)
        if sidecar_path is None:
            continue
        try:
            logging.info(f"[MATCH] media {media_path.name} matched cached sidecar {sidecar_path.name}")
            return sorter.import_media_file(conn, media_path, sidecar_path=sidecar_path)
        finally:
            sidecar_path.unlink(missing_ok=True)

    pending_media[rel_name] = media_path
    for candidate in sorter.metadata_candidate_names(rel_name):
        pending_media_candidates[candidate] = rel_name
    logging.info(f"[CACHE] media waiting for sidecar: {media_path.name}")
    return None


def _register_sidecar(
    conn,
    sidecar_path: Path,
    rel_name: str,
    pending_media: dict[str, Path],
    pending_media_candidates: dict[str, str],
    pending_sidecars: dict[str, Path],
    duplicate_media_candidates: set[str],
) -> str | None:
    if rel_name in duplicate_media_candidates:
        logging.info(f"[DUP-EARLY] discarding sidecar for already-known duplicate media: {sidecar_path.name}")
        sidecar_path.unlink(missing_ok=True)
        return None

    media_key = pending_media_candidates.get(rel_name)
    if media_key is None:
        pending_sidecars[rel_name] = sidecar_path
        logging.info(f"[CACHE] sidecar waiting for media: {sidecar_path.name}")
        return None

    media_path = pending_media[media_key]
    _unregister_media(media_key, pending_media, pending_media_candidates)
    try:
        logging.info(f"[MATCH] sidecar {sidecar_path.name} matched cached media {media_path.name}")
        return sorter.import_media_file(conn, media_path, sidecar_path=sidecar_path)
    finally:
        sidecar_path.unlink(missing_ok=True)


def _unregister_media(rel_name: str, pending_media: dict[str, Path], pending_media_candidates: dict[str, str]):
    pending_media.pop(rel_name, None)
    for candidate in sorter.metadata_candidate_names(rel_name):
        pending_media_candidates.pop(candidate, None)


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
