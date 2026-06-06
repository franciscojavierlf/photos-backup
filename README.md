# Photos Backup

Use Google Takeout to make constant backups of Google Photos, but have a script extract the data and only add the new ones.

## Usage

The script has multiple usages.

### Extracting photos

Add the zip files into the `data` folder. Then run:

```sh
python3 photos_backup.py extract
```

- Media with reliable dates (from Google sidecar JSON) are placed into `photos/YYYY/MM`.
- Files without a reliable date are moved to `photos/_undated` for manual review; their original names are preserved.

### Regenerating the index

There's a SQL index that saves the hash of each photo already backed up, located inside the `photos` folder. This makes
it increadibly fast to skip duplicates. This is saved locally, so if you ever need to run this in another machine or
for some reason the photos directory was modified without running the script, you can reindex it with:

```sh
python3 photos_backup.py reindex
```