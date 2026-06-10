import logging
import os
import re
import shutil
import zipfile
from pathlib import Path

from lib.config import ARCHIVE_SUFFIXES, DATA_DIR, DB_PATH, MEDIA_EXT, TEMP_ROOT
import lib.logger as logger
import lib.sorter as sorter

_COPY_CHUNK = 8 * 1024 * 1024  # 8 MB chunk for faster extraction I/O
_SOURCE_KEY_RE = re.compile(r"(\d{8}T\d{6}Z)")


def import_archives() -> dict[str, int]:
    archives = [p for p in DATA_DIR.iterdir() if p.is_file() and p.name.lower().endswith(ARCHIVE_SUFFIXES)]
    archives.sort(key=lambda x: x.name.lower())

    conn = sorter.ensure_db(DB_PATH)
    try:
        pending_media: dict[tuple[str, str], list[Path]] = {}
        pending_sidecars: dict[tuple[str, str], list[Path]] = {}
        duplicate_media: set[tuple[str, str]] = set()

        rebuilt = _rebuild_pending_cache(conn, pending_media, pending_sidecars, duplicate_media)
        pending_media_count = _count_pending_paths(pending_media)
        pending_sidecar_count = _count_pending_paths(pending_sidecars)
        logging.info(
            f"[CACHE] rebuilt media={pending_media_count} sidecars={pending_sidecar_count} "
            f"resolved={rebuilt['resolved']} dupes={rebuilt['duplicates']} errors={rebuilt['errors']}"
        )

        if not archives:
            logging.info("No archives found.")
            return {
                "archives_ok": 0,
                "archives_failed": 0,
                "pending_media": pending_media_count,
                "pending_sidecars": pending_sidecar_count,
            }

        logging.info("[SCAN] counting importable archive entries")
        global_total_infos = _count_relevant_archive_entries(archives)
        logging.info(f"[SCAN] total_entries={global_total_infos}")
        logging.info(f"Found {len(archives)} archives")

        ok = 0
        fail = 0
        global_state = {"processed": 0, "total": global_total_infos}
        for index, archive in enumerate(archives, start=1):
            logging.info(f"[ARCHIVE] {index}/{len(archives)} {archive.name}")
            if _import_one_archive(
                conn,
                archive,
                pending_media,
                pending_sidecars,
                duplicate_media,
                global_state=global_state,
            ):
                ok += 1
            else:
                fail += 1

        logger.clear_status()
        pending_media_count = _count_pending_paths(pending_media)
        pending_sidecar_count = _count_pending_paths(pending_sidecars)
        logging.info(
            f"[SUMMARY] imported_ok={ok} imported_failed={fail} "
            f"pending_media={pending_media_count} pending_sidecars={pending_sidecar_count}"
        )
        return {
            "archives_ok": ok,
            "archives_failed": fail,
            "pending_media": pending_media_count,
            "pending_sidecars": pending_sidecar_count,
        }
    finally:
        logger.clear_status()
        conn.close()


def _import_one_archive(
    conn,
    archive: Path,
    pending_media: dict[tuple[str, str], list[Path]],
    pending_sidecars: dict[tuple[str, str], list[Path]],
    duplicate_media: set[tuple[str, str]],
    *,
    global_state: dict[str, int] | None = None,
) -> bool:
    if not archive.name.lower().endswith(".zip"):
        logging.error(f"Unsupported archive type: {archive.name}")
        return False

    stage_dir = _extract_dir_for(archive)
    stage_dir.mkdir(parents=True, exist_ok=True)
    source_key = _source_key_for_name(archive.name)

    wrote = 0
    wrote_media = 0
    wrote_sidecar = 0
    skipped_existing = 0
    skipped_noise_json = 0
    added = 0
    dupes = 0
    errors = 0

    try:
        with zipfile.ZipFile(archive, "r") as zf:
            all_infos = [zi for zi in zf.infolist() if not zi.is_dir()]
            infos = [zi for zi in all_infos if _is_media_or_sidecar(zi.filename)]
            total_infos = len(infos)
            skipped_noise_json = sum(
                1 for zi in all_infos if zi.filename.lower().endswith(".json") and not _is_sidecar_json(zi.filename)
            )

            dest_root = stage_dir.resolve()
            for index, info in enumerate(infos, start=1):
                rel_name = os.path.normpath(info.filename)
                target = stage_dir / rel_name
                _ensure_within_root(target, dest_root, info.filename)
                if target.exists():
                    skipped_existing += 1
                    _bump_global_progress(global_state)
                    _set_archive_progress(
                        archive.name,
                        index,
                        total_infos,
                        global_state=global_state,
                    )
                    continue

                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(info, "r") as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst, length=_COPY_CHUNK)

                wrote += 1
                if _is_media(info.filename):
                    wrote_media += 1
                    result = _register_media(
                        conn,
                        source_key,
                        target,
                        rel_name,
                        pending_media,
                        pending_sidecars,
                        duplicate_media,
                    )
                else:
                    wrote_sidecar += 1
                    result = _register_sidecar(
                        conn,
                        source_key,
                        target,
                        rel_name,
                        pending_media,
                        pending_sidecars,
                        duplicate_media,
                    )

                if result == "added":
                    added += 1
                elif result == "duplicate":
                    dupes += 1
                elif result == "error":
                    errors += 1

                _bump_global_progress(global_state)
                _set_archive_progress(
                    archive.name,
                    index,
                    total_infos,
                    global_state=global_state,
                )

        logger.clear_status()
        if wrote == 0 and skipped_existing == 0:
            logging.warning(f"[WARN] nothing extracted from {archive.name} (no media/sidecars found)")
            return False

        if errors == 0:
            archive.unlink(missing_ok=False)
            logging.info(
                f"[OK] wrote_new={wrote} | media={wrote_media} sidecars={wrote_sidecar} "
                f"added={added} dupes={dupes} "
                f"skipped_existing={skipped_existing} skipped_noise_json={skipped_noise_json} "
                f"| deleted archive: {archive.name}"
            )
            return True

        logging.warning(
            f"[WARN] wrote_new={wrote} | media={wrote_media} sidecars={wrote_sidecar} "
            f"added={added} dupes={dupes} errors={errors} "
            f"skipped_existing={skipped_existing} skipped_noise_json={skipped_noise_json} "
            f"| kept archive for rerun: {archive.name}"
        )
        return False
    except Exception as e:
        logger.clear_status()
        logging.warning(f"[ERR] extraction/import failed for {archive.name}: {e}")
        return False


def _rebuild_pending_cache(
    conn,
    pending_media: dict[tuple[str, str], list[Path]],
    pending_sidecars: dict[tuple[str, str], list[Path]],
    duplicate_media: set[tuple[str, str]],
) -> dict[str, int]:
    resolved = 0
    duplicates = 0
    errors = 0

    for source_key, staged_path, rel_name in _iter_staged_entries():
        if not staged_path.exists():
            continue
        try:
            if _is_media(rel_name):
                result = _register_media(
                    conn,
                    source_key,
                    staged_path,
                    rel_name,
                    pending_media,
                    pending_sidecars,
                    duplicate_media,
                )
            else:
                result = _register_sidecar(
                    conn,
                    source_key,
                    staged_path,
                    rel_name,
                    pending_media,
                    pending_sidecars,
                    duplicate_media,
                )

            if result == "added":
                resolved += 1
            elif result == "duplicate":
                duplicates += 1
        except Exception as e:
            logging.warning(f"[CACHE] rebuild fail {staged_path.name}: {e}")
            errors += 1

    return {"resolved": resolved, "duplicates": duplicates, "errors": errors}


def _iter_staged_entries():
    extract_dirs = sorted((p for p in TEMP_ROOT.glob("extract_*") if p.is_dir()), key=lambda x: x.name.lower())
    for extract_dir in extract_dirs:
        source_key = _source_key_for_name(extract_dir.name)
        for path in sorted((p for p in extract_dir.rglob("*") if p.is_file()), key=lambda x: x.as_posix().lower()):
            rel_name = os.path.normpath(str(path.relative_to(extract_dir)))
            if not _is_media_or_sidecar(rel_name):
                continue
            yield source_key, path, rel_name


def _register_media(
    conn,
    source_key: str,
    media_path: Path,
    rel_name: str,
    pending_media: dict[tuple[str, str], list[Path]],
    pending_sidecars: dict[tuple[str, str], list[Path]],
    duplicate_media: set[tuple[str, str]],
) -> str | None:
    pending_key = (source_key, rel_name)
    if sorter.is_known_duplicate(conn, media_path):
        logging.info(f"[DUP-EARLY] media already in DB: {media_path.name}")
        duplicate_media.add(pending_key)
        sorter.record_discarded_media(conn, source_key, rel_name)
        for sidecar_key, sidecar_paths in list(pending_sidecars.items()):
            sidecar_source_key, sidecar_rel_name = sidecar_key
            if sidecar_source_key != source_key or not sorter.sidecar_matches_media_path(sidecar_rel_name, rel_name):
                continue
            for sidecar_path in sidecar_paths:
                logging.info(f"[DUP-EARLY] discarding cached sidecar for duplicate media: {sidecar_path.name}")
                sidecar_path.unlink(missing_ok=True)
            pending_sidecars.pop(sidecar_key, None)
        sorter.discard_staged_media(media_path)
        return "duplicate"

    match = _pop_first_matching_sidecar(source_key, rel_name, pending_sidecars)
    if match is not None:
        sidecar_source_key, sidecar_rel_name, sidecar_path = match
        keep_sidecar = False
        try:
            logging.info(f"[MATCH] media {media_path.name} matched cached sidecar {sidecar_path.name}")
            return sorter.import_media_file(conn, media_path, sidecar_path=sidecar_path)
        except Exception as e:
            logging.warning(f"[ERR] import fail {media_path.name}: {e}")
            _push_pending((sidecar_source_key, sidecar_rel_name), sidecar_path, pending_sidecars)
            _push_pending(pending_key, media_path, pending_media)
            keep_sidecar = True
            return "error"
        finally:
            if not keep_sidecar:
                sidecar_path.unlink(missing_ok=True)

    _push_pending(pending_key, media_path, pending_media)
    logging.info(f"[CACHE] media waiting for sidecar: {media_path.name}")
    return None


def _register_sidecar(
    conn,
    source_key: str,
    sidecar_path: Path,
    rel_name: str,
    pending_media: dict[tuple[str, str], list[Path]],
    pending_sidecars: dict[tuple[str, str], list[Path]],
    duplicate_media: set[tuple[str, str]],
) -> str | None:
    pending_key = (source_key, rel_name)

    for duplicate_media_key in duplicate_media:
        duplicate_source_key, duplicate_media_rel = duplicate_media_key
        if duplicate_source_key == source_key and sorter.sidecar_matches_media_path(rel_name, duplicate_media_rel):
            logging.info(f"[DUP-EARLY] discarding sidecar for already-known duplicate media: {sidecar_path.name}")
            sidecar_path.unlink(missing_ok=True)
            return None

    if sorter.is_discarded_sidecar(conn, source_key, rel_name):
        logging.info(f"[DUP-EARLY] discarding sidecar for persisted duplicate media: {sidecar_path.name}")
        sidecar_path.unlink(missing_ok=True)
        return None

    match = _pop_first_matching_media(source_key, rel_name, pending_media)
    if match is None:
        _push_pending(pending_key, sidecar_path, pending_sidecars)
        logging.info(f"[CACHE] sidecar waiting for media: {sidecar_path.name}")
        return None

    media_source_key, media_rel_name, media_path = match
    keep_sidecar = False
    try:
        logging.info(f"[MATCH] sidecar {sidecar_path.name} matched cached media {media_path.name}")
        return sorter.import_media_file(conn, media_path, sidecar_path=sidecar_path)
    except Exception as e:
        logging.warning(f"[ERR] import fail {media_path.name}: {e}")
        _push_pending((media_source_key, media_rel_name), media_path, pending_media)
        _push_pending(pending_key, sidecar_path, pending_sidecars)
        keep_sidecar = True
        return "error"
    finally:
        if not keep_sidecar:
            sidecar_path.unlink(missing_ok=True)


def _pop_first_matching_sidecar(
    source_key: str,
    media_rel_name: str,
    pending_sidecars: dict[tuple[str, str], list[Path]],
) -> tuple[str, str, Path] | None:
    for sidecar_key, sidecar_paths in list(pending_sidecars.items()):
        sidecar_source_key, sidecar_rel_name = sidecar_key
        if sidecar_source_key != source_key or not sorter.sidecar_matches_media_path(sidecar_rel_name, media_rel_name):
            continue
        sidecar_path = sidecar_paths.pop(0)
        if not sidecar_paths:
            pending_sidecars.pop(sidecar_key, None)
        return sidecar_source_key, sidecar_rel_name, sidecar_path
    return None


def _pop_first_matching_media(
    source_key: str,
    sidecar_rel_name: str,
    pending_media: dict[tuple[str, str], list[Path]],
) -> tuple[str, str, Path] | None:
    for media_key, media_paths in list(pending_media.items()):
        media_source_key, media_rel_name = media_key
        if media_source_key != source_key or not sorter.sidecar_matches_media_path(sidecar_rel_name, media_rel_name):
            continue
        media_path = media_paths.pop(0)
        if not media_paths:
            pending_media.pop(media_key, None)
        return media_source_key, media_rel_name, media_path
    return None


def _push_pending(rel_key: tuple[str, str], path: Path, pending: dict[tuple[str, str], list[Path]]):
    pending.setdefault(rel_key, []).append(path)


def _count_pending_paths(pending: dict[tuple[str, str], list[Path]]) -> int:
    return sum(len(paths) for paths in pending.values())


def _set_archive_progress(
    archive_name: str,
    index: int,
    total: int,
    *,
    global_state: dict[str, int] | None = None,
):
    pct = 100.0 if total == 0 else (index * 100.0 / total)
    global_processed = 0 if global_state is None else global_state.get("processed", 0)
    global_total = 0 if global_state is None else global_state.get("total", 0)
    global_pct = 100.0 if global_total == 0 else (global_processed * 100.0 / global_total)
    logger.set_status(
        f"[PROGRESS] {archive_name} zip={pct:.1f}% global={global_pct:.1f}% "
        f"entries={index}/{total} total={global_processed}/{global_total}"
    )


def _count_relevant_archive_entries(archives: list[Path]) -> int:
    total = 0
    for archive in archives:
        if not archive.name.lower().endswith(".zip"):
            continue
        try:
            with zipfile.ZipFile(archive, "r") as zf:
                total += sum(1 for zi in zf.infolist() if not zi.is_dir() and _is_media_or_sidecar(zi.filename))
        except Exception as e:
            logging.warning(f"[SCAN] failed counting {archive.name}: {e}")
    return total


def _bump_global_progress(global_state: dict[str, int] | None):
    if global_state is None:
        return
    global_state["processed"] = global_state.get("processed", 0) + 1


def _extract_dir_for(archive: Path) -> Path:
    safe = archive.name.replace(".", "_")
    return TEMP_ROOT / f"extract_{safe}"


def _source_key_for_name(name: str) -> str:
    match = _SOURCE_KEY_RE.search(name)
    if match:
        return match.group(1)
    return name.lower()


def _ensure_within_root(target: Path, root: Path, name: str):
    target_abs = target.resolve()
    if os.path.commonpath([str(target_abs), str(root)]) != str(root):
        raise ValueError(f"[zip] skip suspicious path: {name}")


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
