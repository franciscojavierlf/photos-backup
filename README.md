Use Google Takeout to make constant backups of Google Photos, but have a script extract the data and only add the new ones.

**Usage**
```sh
python3 photos_backup.py
```

- Media with reliable dates (from Google sidecar JSON) are placed into photos/YYYY/MM.
- Files without a reliable date are moved to photos/_undated for manual review; their original names are preserved.

**For regenerating the index**
```sh
python3 photos_backup.py --reindex
```