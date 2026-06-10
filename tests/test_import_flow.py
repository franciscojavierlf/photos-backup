import json
import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

from lib import extractor, sorter


class ImportFlowTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base_dir = Path(self._tmp.name)
        self.data_dir = self.base_dir / "data"
        self.photos_dir = self.base_dir / "photos"
        self.temp_root = self.data_dir / ".tmp_extracted"
        self.db_path = self.photos_dir / ".photo_dedupe.sqlite"
        self.undated_dir = self.photos_dir / "_undated"

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.photos_dir.mkdir(parents=True, exist_ok=True)
        self.temp_root.mkdir(parents=True, exist_ok=True)

        self._orig = {
            "sorter.DATA_DIR": sorter.DATA_DIR,
            "sorter.PHOTOS_DIR": sorter.PHOTOS_DIR,
            "sorter.TEMP_ROOT": sorter.TEMP_ROOT,
            "sorter.DB_PATH": sorter.DB_PATH,
            "sorter.UNDATED_DIR": sorter.UNDATED_DIR,
            "extractor.DATA_DIR": extractor.DATA_DIR,
            "extractor.TEMP_ROOT": extractor.TEMP_ROOT,
            "extractor.DB_PATH": extractor.DB_PATH,
        }

        sorter.DATA_DIR = self.data_dir
        sorter.PHOTOS_DIR = self.photos_dir
        sorter.TEMP_ROOT = self.temp_root
        sorter.DB_PATH = self.db_path
        sorter.UNDATED_DIR = self.undated_dir

        extractor.DATA_DIR = self.data_dir
        extractor.TEMP_ROOT = self.temp_root
        extractor.DB_PATH = self.db_path

    def tearDown(self):
        sorter.DATA_DIR = self._orig["sorter.DATA_DIR"]
        sorter.PHOTOS_DIR = self._orig["sorter.PHOTOS_DIR"]
        sorter.TEMP_ROOT = self._orig["sorter.TEMP_ROOT"]
        sorter.DB_PATH = self._orig["sorter.DB_PATH"]
        sorter.UNDATED_DIR = self._orig["sorter.UNDATED_DIR"]

        extractor.DATA_DIR = self._orig["extractor.DATA_DIR"]
        extractor.TEMP_ROOT = self._orig["extractor.TEMP_ROOT"]
        extractor.DB_PATH = self._orig["extractor.DB_PATH"]

        self._tmp.cleanup()

    def test_sidecar_matching_rules(self):
        self.assertTrue(
            sorter.sidecar_matches_media_name(
                "IMG_0010.JPG.supplemental-metadata.json",
                "IMG_0010.JPG",
            )
        )
        self.assertTrue(
            sorter.sidecar_matches_media_name(
                "IMG00038-20110616-1308.jpg.supplemental-metada.json",
                "IMG00038-20110616-1308.jpg",
            )
        )
        self.assertTrue(
            sorter.sidecar_matches_media_name(
                "IMG_0010.JPG.anything(1).json",
                "IMG_0010(1).JPG",
            )
        )
        self.assertFalse(
            sorter.sidecar_matches_media_path(
                "Takeout/Google Photos/Photos from 2020/IMG_0010.JPG.supplemental-metadata.json",
                "Takeout/Google Photos/Photos from 2021/IMG_0010.JPG",
            )
        )

    def test_duplicate_media_then_late_sidecar_is_discarded_via_persistent_cache(self):
        rel = "Takeout/Google Photos/Photos from 2020/IMG_0010.JPG"
        content = b"duplicate-photo"
        self._seed_library_file("2020/01/IMG_0010.JPG", content)

        self._write_zip("takeout-20260609T142344Z-3-001.zip", {rel: content})
        extractor.import_archives()

        self._write_zip(
            "takeout-20260609T142344Z-3-002.zip",
            {f"{rel}.supplemental-metadata.json": self._sidecar_bytes(1577836800)},
        )
        extractor.import_archives()

        self.assertEqual([], self._temp_files())
        conn = sqlite3.connect(self.db_path)
        try:
            rows = conn.execute("SELECT source_key, folder_path, media_name FROM discarded_media").fetchall()
        finally:
            conn.close()
        self.assertEqual([("20260609T142344Z", "Takeout/Google Photos/Photos from 2020", "IMG_0010.JPG")], rows)

    def test_staged_sidecar_then_duplicate_media_discards_both(self):
        rel = "Takeout/Google Photos/Photos from 2020/IMG_0010.JPG"
        content = b"duplicate-photo"
        self._seed_library_file("2020/01/IMG_0010.JPG", content)

        self._write_zip(
            "takeout-20260609T142344Z-3-001.zip",
            {f"{rel}.supplemental-metadata.json": self._sidecar_bytes(1577836800)},
        )
        extractor.import_archives()
        self.assertEqual([f"extract_takeout-20260609T142344Z-3-001_zip/{rel}.supplemental-metadata.json"], self._temp_files())

        self._write_zip("takeout-20260609T142344Z-3-002.zip", {rel: content})
        extractor.import_archives()

        self.assertEqual([], self._temp_files())

    def test_new_media_then_late_sidecar_imports_to_dated_folder(self):
        rel = "Takeout/Google Photos/Photos from 2020/IMG_0020.JPG"
        content = b"new-photo-a"

        self._write_zip("takeout-20260609T142344Z-3-001.zip", {rel: content})
        extractor.import_archives()
        self.assertIn(f"extract_takeout-20260609T142344Z-3-001_zip/{rel}", self._temp_files())

        self._write_zip(
            "takeout-20260609T142344Z-3-002.zip",
            {f"{rel}.supplemental-metadata.json": self._sidecar_bytes(1577836800)},
        )
        extractor.import_archives()

        self.assertTrue((self.photos_dir / "2020" / "01" / "IMG_0020.JPG").exists())
        self.assertEqual([], self._temp_files())
        self._assert_db_has_file("IMG_0020.JPG")

    def test_sidecar_then_late_new_media_imports_to_dated_folder(self):
        rel = "Takeout/Google Photos/Photos from 2020/IMG_0030.JPG"
        content = b"new-photo-b"

        self._write_zip(
            "takeout-20260609T142344Z-3-001.zip",
            {f"{rel}.supplemental-metadata.json": self._sidecar_bytes(1577836800)},
        )
        extractor.import_archives()

        self._write_zip("takeout-20260609T142344Z-3-002.zip", {rel: content})
        extractor.import_archives()

        self.assertTrue((self.photos_dir / "2020" / "01" / "IMG_0030.JPG").exists())
        self.assertEqual([], self._temp_files())
        self._assert_db_has_file("IMG_0030.JPG")

    def test_cross_zip_same_run_media_and_sidecar_match(self):
        rel = "Takeout/Google Photos/Photos from 2020/IMG_0040.JPG"
        content = b"new-photo-c"

        self._write_zip("takeout-20260609T142344Z-3-001.zip", {rel: content})
        self._write_zip(
            "takeout-20260609T142344Z-3-002.zip",
            {f"{rel}.supplemental-metadata.json": self._sidecar_bytes(1577836800)},
        )

        summary = extractor.import_archives()

        self.assertEqual(2, summary["archives_ok"])
        self.assertTrue((self.photos_dir / "2020" / "01" / "IMG_0040.JPG").exists())
        self.assertEqual([], self._temp_files())

    def test_rerun_rebuilds_pending_cache_and_later_match_works(self):
        rel = "Takeout/Google Photos/Photos from 2020/IMG_0050.JPG"
        content = b"new-photo-d"

        self._write_zip(
            "takeout-20260609T142344Z-3-001.zip",
            {f"{rel}.supplemental-metadata.json": self._sidecar_bytes(1577836800)},
        )
        extractor.import_archives()

        summary = extractor.import_archives()
        self.assertEqual(0, summary["archives_ok"])
        self.assertEqual(1, summary["pending_sidecars"])

        self._write_zip("takeout-20260609T142344Z-3-002.zip", {rel: content})
        extractor.import_archives()

        self.assertTrue((self.photos_dir / "2020" / "01" / "IMG_0050.JPG").exists())
        self.assertEqual([], self._temp_files())

    def test_undated_discards_leftover_sidecars(self):
        rel = "Takeout/Google Photos/Photos from 2020/IMG_0060.JPG"

        self._write_zip(
            "takeout-20260609T142344Z-3-001.zip",
            {f"{rel}.supplemental-metadata.json": self._sidecar_bytes(1577836800)},
        )
        extractor.import_archives()
        self.assertNotEqual([], self._temp_files())

        sorter.sort_media()

        self.assertEqual([], self._temp_files())

    def _write_zip(self, archive_name: str, files: dict[str, bytes]):
        archive_path = self.data_dir / archive_name
        with zipfile.ZipFile(archive_path, "w") as zf:
            for rel_name, data in files.items():
                zf.writestr(rel_name, data)
        return archive_path

    def _seed_library_file(self, rel_path: str, content: bytes):
        path = self.photos_dir / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)

        conn = sorter.ensure_db(self.db_path)
        try:
            file_hash = sorter._hash_file(path)
            stat = path.stat()
            conn.execute(
                "INSERT OR REPLACE INTO files(hash,size,path,mtime) VALUES(?,?,?,?)",
                (file_hash, stat.st_size, str(path), int(stat.st_mtime)),
            )
            conn.commit()
        finally:
            conn.close()
        return path

    def _sidecar_bytes(self, timestamp: int) -> bytes:
        return json.dumps({"photoTakenTime": {"timestamp": str(timestamp)}}).encode("utf-8")

    def _temp_files(self) -> list[str]:
        return sorted(
            str(path.relative_to(self.temp_root))
            for path in self.temp_root.rglob("*")
            if path.is_file()
        )

    def _assert_db_has_file(self, filename: str):
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute("SELECT path FROM files WHERE path LIKE ?", (f"%{filename}",)).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row)


if __name__ == "__main__":
    unittest.main()
