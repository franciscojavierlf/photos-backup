# Photos Backup

Use Google Takeout to make constant backups of Google Photos, but have a script extract the data and only add the new ones.

## Usage

The script has multiple usages.

### Extracting photos

Add the zip files into the `data` folder. Then run:

```sh
python3 photos_backup.py import
```

- Matched media+sidecar pairs are imported immediately into `photos/YYYY/MM`.
- Unmatched media and sidecars stay staged in `data/.tmp_extracted` until you explicitly finalize them.

When you are ready to finalize whatever still has no match, run:

```sh
python3 photos_backup.py undated
```

- `undated` imports any remaining staged media.
- If a staged media file now has a sidecar beside it, it still goes to the dated folder.
- If it still has no reliable date, it goes to `photos/_undated`.

### How extraction works

The importer now processes Google Takeout archives one zip at a time, but it keeps unmatched staged files between zips and between runs so media can still pair with sidecars that appear later.

For each `import` run:

1. The script scans the existing `data/.tmp_extracted/extract_*` folders, rebuilds the in-memory cache, and resolves any pairs that were already staged before the current run.
2. Each new zip is extracted into its own persistent staging folder: `data/.tmp_extracted/extract_<zip-name>`.
3. Media files and their Google sidecar JSON files are matched in memory using the same filename rules as the sorter.
   - Standard-name rule: `IMG_0010.JPG` matches JSON files that start with `IMG_0010.JPG` and end with `.json`.
   - Duplicate-name rule: `IMG_0010(1).JPG` matches JSON files that start with `IMG_0010.JPG` and end with `(1).json`.
   - The text between the media filename and `.json` can be anything, so truncated names like `.supplemental-metada.json` still match.
4. When a media file and sidecar pair are both available, the media is imported immediately:
   - duplicates are deleted
   - dated files are moved into `photos/YYYY/MM`
5. If a file is already known to be a duplicate from the DB, it and any matching staged sidecar are deleted as soon as that is known.
6. Unmatched media and unmatched sidecars are left in `data/.tmp_extracted` so later zip files, or later runs, can still complete the pair.
7. Processed zip files are deleted, but the staging folders are intentionally preserved until you run `undated`.

This means the script no longer needs to hold every extracted Takeout archive at once, but it also does not prematurely send unmatched files to `_undated` just because their pair was not in the current zip.

### Logging

The importer keeps a live progress line at the bottom of the terminal for the current archive and also writes the full log to `logs/.log`.

Typical log messages include:

- `[ARCHIVE] 2/7 takeout-002.zip`
- `[PROGRESS] takeout-002.zip zip=41.3% global=18.7% entries=812/1967 total=4231/22618`
- `[CACHE] media waiting for sidecar: ...`
- `[CACHE] sidecar waiting for media: ...`
- `[MATCH] ... matched ...`
- `[CACHE] rebuilt media=... sidecars=... resolved=...`
- `[SUMMARY] imported_ok=... imported_failed=... pending_media=... pending_sidecars=...`

At the end of `import`, the CLI can ask whether you want to run `undated` immediately for the remaining unmatched staged media.

### Regenerating the index

There's a SQL index that saves the hash of each photo already backed up, located inside the `photos` folder. This makes
it increadibly fast to skip duplicates. This is saved locally, so if you ever need to run this in another machine or
for some reason the photos directory was modified without running the script, you can reindex it with:

```sh
python3 photos_backup.py reindex
```

`reindex` now rebuilds the database in bulk and uses a faster MD5 content hash, so your first reindex after this change will rewrite every DB entry.
