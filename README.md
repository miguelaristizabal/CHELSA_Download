## CHELSA Download Pipeline

The legacy scripts have been refactored into a single, testable CLI called `chelsa-download`. The tool prepares file lists, caches downloads with retry-aware rclone calls, fills masked pixels correctly, and writes AOI-clipped GeoTIFFs with consistent logging/progress output driven by `rich`.

### 1. Install dependencies

```bash
python -m venv .venv
. .venv/bin/activate        # Windows: .\.venv\Scripts\activate
pip install -e .[dev]
```

### 2. Configure shared paths

Copy the sample config and adjust the paths/remotes for your environment:

```bash
cp chelsa-download.example.toml ~/.chelsa-download.toml
# or point CHELSA_DOWNLOAD_CONFIG to a custom path
```

Key fields:

- `paths.aoi` — AOI vector file (GeoJSON, GeoPackage, etc.)
- `paths.lists_dir` — base directory for generated `trace_*.txt` / `present_*.txt`
- `paths.cache_dir` — SSD scratch space for temporary downloads
- `paths.trace_filelist_json` — cached `rclone lsjson` output for TraCE21k
- `trace.*` / `present.*` — remote aliases, optional prefixes, nodata values, output folders
- `rclone.config` — custom rclone config file (e.g., `envicloud.conf`)

### 3. Prepare list files + metadata

```
chelsa-download prepare-lists --kind trace \
  --source-json lists/raw/chelsatrace_filelist.json

chelsa-download prepare-lists --kind present
```

Each `.txt` now has a sibling `.meta.json` describing variable, counts, sizes, and time ranges. The downloader verifies SHA-1 digests to ensure freshness.

### 4. Download & clip rasters

```
chelsa-download download-trace --var bio01 --max-workers 4
chelsa-download download-present --quiet
```

Flags:

- `--var` filters variables.
- `--limit` processes only the first N files (useful for smoke tests).
- `--force` re-downloads and overwrites AOI outputs.
- `--quiet` / `--verbose` tune logging levels globally.

The CLI drives a resilient rclone layer that retries transfers, tracks byte-level progress, and fills masked cells to the configured nodata value before writing GeoTIFFs. Temporary downloads are cached/resumed in `paths.cache_dir`.

### 5. Run tests

```
pytest
```

### Legacy scripts

The original helper scripts remain in the repository for reference, but the preferred workflow is the `chelsa-download` CLI described above.
