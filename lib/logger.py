import logging
import sys
from logging.handlers import RotatingFileHandler

from lib.config import DATA_DIR, LOGS_DIR, PHOTOS_DIR, TEMP_ROOT

_LOG_FILE = LOGS_DIR / ".log"
_LOG_LEVEL = logging.INFO
_STATUS_LINE = ""


class _StatusAwareStreamHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = self.format(record)
            self.acquire()
            try:
                if _STATUS_LINE:
                    self.stream.write("\r" + (" " * len(_STATUS_LINE)) + "\r")
                self.stream.write(msg + self.terminator)
                if _STATUS_LINE:
                    self.stream.write(_STATUS_LINE)
                self.flush()
            finally:
                self.release()
        except Exception:
            self.handleError(record)


def setup_logging():
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(_LOG_LEVEL)
    root.handlers.clear()

    ch = _StatusAwareStreamHandler(sys.stdout)
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


def set_status(message: str):
    global _STATUS_LINE
    if not sys.stdout.isatty():
        _STATUS_LINE = ""
        return

    next_line = message.rstrip()
    if _STATUS_LINE:
        sys.stdout.write("\r" + (" " * len(_STATUS_LINE)) + "\r")
    _STATUS_LINE = next_line
    if _STATUS_LINE:
        sys.stdout.write(_STATUS_LINE)
    sys.stdout.flush()


def clear_status():
    global _STATUS_LINE
    if sys.stdout.isatty() and _STATUS_LINE:
        sys.stdout.write("\r" + (" " * len(_STATUS_LINE)) + "\r")
        sys.stdout.flush()
    _STATUS_LINE = ""
