import sys
from pathlib import Path
import os

# =========================
# CONFIG: FOLDERS & LOGGING
# =========================
BASE_DIR = Path(sys.argv[0]).parent.resolve() # Assumes we run main.py
DATA_DIR = BASE_DIR / "datatest" # put your Takeout archives here
PHOTOS_DIR = BASE_DIR / "photos" # destination library (used for logs only)
TEMP_ROOT = DATA_DIR / ".tmp_extracted" # persistent extraction root (never deleted)

DB_PATH = PHOTOS_DIR / ".photo_dedupe.sqlite"
UNDATED_DIR = PHOTOS_DIR / "_undated"  # where files without reliable dates are placed

# =========================
# FILE TYPE FILTERS
# =========================
IMAGE_EXT = {
    '.jpg', '.jpeg', '.png', '.heic', '.gif', '.tif', '.tiff', '.webp',
    '.bmp', '.dng', '.cr2', '.nef', '.arw', '.raf', '.orf', '.rw2'
}
VIDEO_EXT = {
    '.mp4', '.mov', '.m4v', '.avi', '.wmv', '.mkv', '.3gp', '.mts', '.m2ts'
}
MEDIA_EXT = IMAGE_EXT | VIDEO_EXT

ARCHIVE_SUFFIXES = ('.zip', '.tar', '.tgz', '.tar.gz')

# =========================
# PERFORMANCE / PARALLELISM
# =========================
_CPUS = os.cpu_count() or 4
# Max concurrent archives to extract in parallel (bounded by number of archives)
MAX_ARCHIVE_WORKERS = max(1, min(24, _CPUS * 2))
# Max workers within a single large zip to extract members in parallel
MAX_WITHIN_ARCHIVE_WORKERS = max(1, min(24, _CPUS * 2))
# Minimum number of entries in a zip to trigger within-archive parallel extraction
PARALLEL_MIN_FILES = 32