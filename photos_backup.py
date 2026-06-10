import argparse
import sys

from lib.config import DATA_DIR, PHOTOS_DIR, LOGS_DIR, TEMP_ROOT
import lib.logger as logger
import lib.extractor as extractor
import lib.sorter as sorter


def _init():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    logger.setup_logging()


def _cmd_import(args):
    summary = extractor.import_archives()
    if summary["pending_media"] <= 0 or not sys.stdin.isatty():
        return

    prompt = (
        f"{summary['pending_media']} staged media files still do not have a matched sidecar. "
        "Run `photos_backup.py undated` now? [y/N]: "
    )
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return

    if answer in {"y", "yes"}:
        sorter.sort_media()


def _cmd_reindex(args):
    sorter.reindex_library()


def _cmd_undated(args):
    sorter.sort_media()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Photo library management.")
    subparsers = ap.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser("import", help="Import archives into the photo library.")
    import_parser.set_defaults(func=_cmd_import)

    undated_parser = subparsers.add_parser(
        "undated",
        help="Finalize staged unmatched media from data/.tmp_extracted into the library.",
    )
    undated_parser.set_defaults(func=_cmd_undated)

    reindex_parser = subparsers.add_parser("reindex", help="Scan photos/ and rebuild DB entries.")
    reindex_parser.set_defaults(func=_cmd_reindex)

    args = ap.parse_args()

    _init()
    args.func(args)
