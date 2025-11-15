## CHELSA Download CLI

CHELSA rasters are published as global GeoTIFFs in cloud buckets. Downloading the full planet every time you need a small study area quickly becomes unmanageable—especially for the TraCE21k archive that holds hundreds of timesteps per variable. `chelsa-download` solves this by combining four capabilities:

- Shared rclone remotes for CHELSA v2.1 present-day and TraCE21k paleoclimate data.
- Automatic list generation with metadata (counts, sizes, time ranges, checksums) so you always know what is available.
- AOI-centric downloading that clips, rescales, and fills masked pixels before writing GeoTIFFs, avoiding full-global mosaics.
- Resilient transfers with retries, resumable cache files, and Rich-powered progress bars/logs so you can monitor large jobs.

Follow the steps below to set up the tool and fetch AOI-clipped CHELSA rasters.

---

### 1. Prerequisites

1. **Python 3.10+** in a virtual environment or Conda env.
2. **rclone** installed. The repository ships with `envicloud.conf`, which defines:
   - `chelsa02_bioclim`: CHELSA v2.1 present-day climatology (`chelsa/global/bioclim`).
   - `chelsa01_trace21k_bioclim`: CHELSA TraCE21k paleoclimate (`chelsa_trace21k/global/bioclim`).
3. **Area of Interest (AOI)** stored as GeoJSON, GeoPackage, or another format supported by GeoPandas.

Install the CLI (editable mode is convenient while iterating):

```bash
python -m pip install -e .[dev]
```

---

### 2. Configure the tool

All shared paths and options live in a TOML file. Copy the sample and edit the values:

```bash
cp chelsa-download.example.toml ~/.chelsa-download.toml
```

Key fields:

- `paths.aoi`: AOI geometry file to clip against.
- `paths.lists_dir`: directory for generated `trace_*.txt` / `present_*.txt` and their metadata.
- `paths.cache_dir`: fast SSD space for temporary downloads (files are deleted after clipping).
- `paths.trace_filelist_json`: optional cached `rclone lsjson` output for TraCE21k.
- `rclone.config`: path to the rclone configuration file (e.g., `envicloud.conf`).
- `present.*` / `trace.*`: remote alias, optional prefix, output directory, and nodata value.

Set `CHELSA_DOWNLOAD_CONFIG=/path/to/file` if you want to keep multiple configs.

---

### 3. Generate the download lists

CHELSA stores rasters per variable/time. The CLI converts bucket listings into digest-verified text files so downloads can be batched and validated later.

#### TraCE21k

1. Snapshot the bucket using rclone:

   ```bash
   rclone lsjson chelsa01_trace21k_bioclim: --recursive > lists/raw/chelsatrace_filelist.json
   ```

2. Build the trace lists and metadata:

   ```bash
   chelsa-download prepare-lists --kind trace \
     --source-json lists/raw/chelsatrace_filelist.json
   ```

#### Present-day (1981–2010 climatology)

The CLI lists the remote directly—no cached JSON required:

```bash
chelsa-download prepare-lists --kind present
```

Each `.txt` file now has a `.meta.json` neighbor with SHA-1 digests, record counts, date ranges, and per-file sizes.

---

### 4. Download AOI-clipped rasters

The core commands download to the cache, clip to your AOI, apply scale/offset metadata, fill masked cells to the configured nodata value, and write tiled/deflated GeoTIFFs. All flags support `--quiet`, `--verbose`, `--var`, `--limit`, `--force`, and `--max-workers`.

**TraCE21k example (bio01 only, 4 workers):**

```bash
chelsa-download download-trace --var bio01 --max-workers 4
```

**Present-day example (all variables, quiet logging):**

```bash
chelsa-download download-present --quiet
```

While running you will see Rich progress bars with byte counts and ETA per file. Completed rasters are written to the `output_dir` defined in your config with `_AOI` appended to the filename. Already-processed files are skipped unless `--force` is provided.

---

### 5. Tips for large pulls

- **Cache size:** Make sure `paths.cache_dir` has enough free space for at least one un-clipped raster (0.5–2+ GB depending on the variable).
- **Parallelism:** Adjust `downloads.max_workers` gradually to avoid overwhelming your bandwidth or the remote.
- **List freshness:** If the CHELSA bucket changes, rerun the `prepare-lists` commands so the metadata digests match the new `.txt` contents.
- **AOI CRS:** If your AOI has no CRS, the tool assumes EPSG:4326. Set it explicitly to ensure reprojection happens correctly.

With these steps you can extract only the data you need from the massive CHELSA archives—perfect for quickly producing AOI-specific climate layers without pulling multi-gigabyte global files. Enjoy the downloads!
