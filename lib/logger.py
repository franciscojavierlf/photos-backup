import sys
import logging
from logging.handlers import RotatingFileHandler
from lib.config import DATA_DIR, PHOTOS_DIR, TEMP_ROOT

LOG_DIR = PHOTOS_DIR / ".logs"
LOG_FILE = LOG_DIR / "extract_takeout.log"
LOG_LEVEL = logging.INFO

def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(LOG_LEVEL)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(LOG_LEVEL)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(LOG_LEVEL)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    logging.info(f"DATA_DIR   = {DATA_DIR}")
    logging.info(f"PHOTOS_DIR = {PHOTOS_DIR}")
    logging.info(f"TEMP_ROOT  = {TEMP_ROOT}")
    logging.info(f"LOG_FILE   = {LOG_FILE}")
