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

### How extraction works

The extractor now processes Google Takeout archives one zip at a time to keep temporary disk usage bounded.

For each zip file:

1. Files are extracted incrementally into `data/.tmp_extracted/extract_<zip-name>`.
2. Media files and their Google sidecar JSON files are matched in memory using the same filename rules as the sorter.
3. When a media file and sidecar pair are both available, the media is imported immediately:
   - duplicates are deleted
   - dated files are moved into `photos/YYYY/MM`
4. If a media file never finds a matching sidecar by the end of that zip, it is finalized at zip end and will usually land in `photos/_undated`.
5. Unmatched sidecar JSON files are deleted at zip end.
6. The temporary extracted folder is removed, the processed zip is deleted, and the script continues to the next zip.

This means the script no longer extracts every Takeout archive before sorting. Peak temporary usage is limited to one archive plus any unmatched files from that archive while it is being processed.

### Logging

The extractor logs the pairing lifecycle so you can see what is happening:

- `[CACHE] media waiting for sidecar: ...`
- `[CACHE] sidecar waiting for media: ...`
- `[MATCH] ... matched ...`
- `[ZIP-END] no sidecar match ...`
- `[ZIP-END] discarding unmatched sidecar: ...`

The full log is written to `logs/.log` in addition to stdout.

### Regenerating the index

There's a SQL index that saves the hash of each photo already backed up, located inside the `photos` folder. This makes
it increadibly fast to skip duplicates. This is saved locally, so if you ever need to run this in another machine or
for some reason the photos directory was modified without running the script, you can reindex it with:

```sh
python3 photos_backup.py reindex
```

`reindex` now rebuilds the database in bulk and uses a faster MD5 content hash, so your first reindex after this change will rewrite every DB entry.
