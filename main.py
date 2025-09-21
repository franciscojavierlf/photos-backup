import argparse
import logging
from lib.config import DATA_DIR, PHOTOS_DIR, TEMP_ROOT
import lib.logger as logger
import lib.extractor as extractor
import lib.sorter as sorter

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Move extracted media into photos/ with hash-dedupe.")
    ap.add_argument("--reindex", action="store_true", help="Scan photos/ and rebuild DB entries (failsafe).")
    args = ap.parse_args()
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    logger.setup_logging()

    if args.reindex:
        sorter.reindex_library()
    else:
        extractor.extract_zip_files()
        sorter.sort_media()
