import sys
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
from lib.config import DATA_DIR, PHOTOS_DIR, TEMP_ROOT, BASE_DIR

_LOG_FILE = BASE_DIR / ".log"
_LOG_LEVEL = logging.INFO

def setup_logging():
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(_LOG_LEVEL)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(_LOG_LEVEL)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    fh = RotatingFileHandler(_LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(_LOG_LEVEL)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    logging.info(f"DATA_DIR   = {DATA_DIR}")
    logging.info(f"PHOTOS_DIR = {PHOTOS_DIR}")
    logging.info(f"TEMP_ROOT  = {TEMP_ROOT}")
    logging.info(f"LOG_FILE   = {_LOG_FILE}")
