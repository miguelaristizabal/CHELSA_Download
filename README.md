# chelsa-download

A Python CLI to efficiently download, clip, and normalize [CHELSA](https://www.chelsa-climate.org/) climate rasters for any Area of Interest (AOI). Supports both present-day bioclim climatologies (CHELSA v2.1, 1981–2010) and paleoclimate time series (CHELSA-TraCE21k, 20 ka BP to present), as well as monthly variables (pr, tasmin, tasmax).

Instead of pulling multi-GB global GeoTIFFs, `chelsa-download` uses cloud-optimized GeoTIFF (COG) windowed reads to fetch only the pixels you need — clipped to your AOI, converted to physical units, and written as analysis-ready float32 GeoTIFFs.

## Quick Start

### 1. Install rclone

**Windows:**
```powershell
winget install Rclone.Rclone
```
**macOS / Linux:**
```bash
sudo -v ; curl https://rclone.org/install.sh | sudo bash
```
Verify: `rclone version`

### 2. Install chelsa-download

```bash
pip install "https://github.com/miguelaristizabal/CHELSA_Download/archive/refs/tags/0.4.0.tar.gz"
```

### 3. Download your first raster

```bash
chelsa-download --aoi path/to/AOI.geojson download-present --var bio01 --limit 1
```

That's it. The clipped raster lands in `outputs/present/` in your current directory, in physical units (°C), ready for analysis.

---

## Table of Contents

- [Installation](#installation)
- [How It Works](#how-it-works)
- [Configuration](#configuration)
- [CLI Reference](#cli-reference)
  - [Global Options](#global-options)
  - [download-present](#download-present)
  - [download-trace](#download-trace)
  - [download-present-monthly](#download-present-monthly)
  - [download-trace-monthly](#download-trace-monthly)
  - [prepare-lists](#prepare-lists)
- [Variables & Units](#variables--units)
  - [Why unit normalization matters](#why-unit-normalization-matters)
  - [CHELSA v2.1 conversions](#chelsa-v21-present-day-bioclim-conversions)
  - [CHELSA-TraCE21k conversions](#chelsa-trace21k-paleoclimate-bioclim-conversions)
  - [Monthly variable conversions](#monthly-variable-conversions-both-datasets)
  - [Derived variable recomputation](#derived-variable-recomputation)
- [Windowed COG Reads](#windowed-cog-reads)
- [Generating File Lists](#generating-file-lists)
- [Tips for Large Downloads](#tips-for-large-downloads)
- [Troubleshooting](#troubleshooting)
- [Related Tools](#related-tools)
- [Citations](#citations)
- [License](#license)

---

## Installation

**Requirements:** Python 3.10+ and [rclone](https://rclone.org/install/).

### From GitHub release (recommended)

```bash
pip install "https://github.com/miguelaristizabal/CHELSA_Download/archive/refs/tags/0.4.0.tar.gz"
```

Replace `0.4.0` with the desired release tag.

### Development install (from source)

```bash
git clone https://github.com/miguelaristizabal/CHELSA_Download.git
cd CHELSA_Download
python -m venv .venv

# Activate the virtual environment
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell:
.venv\Scripts\Activate.ps1

pip install -e .[dev]
```

### Verify installation

```bash
chelsa-download --help
rclone version
```

---

## How It Works

1. **File lists** — The package ships with pre-generated `.txt` manifests (and `.meta.json` metadata) that list every file on the CHELSA remotes. These tell the CLI exactly what to download.
2. **AOI clipping** — You supply a GeoJSON, GeoPackage, or Shapefile. The CLI reads only the COG tiles that overlap your AOI (via HTTP range requests), clips to the geometry, and fills masked pixels with nodata (`-9999`).
3. **Unit normalization** — Raw CHELSA values (scaled integers in Kelvin, mm×10, etc.) are automatically converted to physical units (°C, mm) as float32. Derived variables `bio07` and `bio03` are recomputed from their dependencies when available.
4. **Output** — Tiled, DEFLATE-compressed GeoTIFFs with a configurable suffix (default `_AOI`) appended to the filename, written to the configured output directory. The cache directory used for full downloads is automatically cleaned up after completion.

On first launch, the CLI copies bundled assets (rclone config, file lists) to `~/.chelsa-download/` so everything works out of the box even when installed with pip.

---

## Configuration

### Zero-config usage

Just pass `--aoi` and the CLI handles everything else using bundled defaults:

```bash
chelsa-download --aoi my_region.geojson download-present
```

### TOML configuration file

For full control, create a TOML config (see the bundled example):

```bash
cp chelsa-download.example.toml ~/.chelsa-download.toml
```

```toml
[paths]
aoi = "./AOI.geojson"                   # default AOI when --aoi is not passed
lists_dir = "./lists"                   # directory containing .txt file lists
cache_dir = "./chelsa_cache"            # scratch space for full downloads
trace_filelist_json = "./lists/raw/chelsatrace_filelist.json"

[rclone]
config = "./envicloud.conf"             # rclone remote definitions

[downloads]
max_workers = 6                         # parallel workers
suffix = "_AOI"                         # appended to output filenames

[present]
remote = "chelsa02_bioclim"
prefix = ""
lists_subdir = "present"
output_dir = "./outputs/present"
nodata_value = -9999.0

[trace]
remote = "chelsa01_trace21k_bioclim"
prefix = ""
lists_subdir = "."
output_dir = "./outputs/trace"
nodata_value = -9999.0

[present_monthly]
remote = "chelsa02_climatologies"
prefix = ""
lists_subdir = "present_monthly"
output_dir = "./outputs/present_monthly"
nodata_value = -9999.0

[trace_monthly]
remote = "chelsa01_trace_centennial"
prefix = ""
lists_subdir = "trace_monthly"
output_dir = "./outputs/trace_monthly"
nodata_value = -9999.0
```

**Config resolution order:**
1. `--config` explicitly provided → use that TOML file
2. `--aoi` provided (without `--config`) → use bundled defaults with that AOI
3. Neither provided → auto-discover `CHELSA_DOWNLOAD_CONFIG` env var or `~/.chelsa-download.toml`, then prompt for AOI if neither exists

### AOI file

Supply any geometry file readable by GeoPandas (GeoJSON, GeoPackage, Shapefile with all sidecar files). If the file has no CRS, WGS 84 (EPSG:4326) is assumed.

---

## CLI Reference

```
chelsa-download [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]
```

### Global Options

These apply to **all** commands and must appear before the subcommand name.

| Option | Short | Type | Default | Description |
|---|---|---|---|---|
| `--config` | `-c` | Path | `None` | Path to a TOML configuration file. Falls back to env var `CHELSA_DOWNLOAD_CONFIG`, then `~/.chelsa-download.toml`, then bundled defaults. |
| `--aoi` | | Path | `None` | Path to AOI geometry file (GeoJSON, GeoPackage, Shapefile). Required for download commands, but not needed for `--help`. |
| `--max-workers` | | int | `6` | Number of parallel download/processing workers. |
| `--suffix` | | text | `_AOI` | Suffix appended to output filenames (e.g. `--suffix _myregion` produces `*_myregion.tif`). |
| `--quiet` | | flag | `False` | Suppress informational messages. Only warnings and errors are shown. |
| `--verbose` | `-v` | flag | `False` | Enable debug logging. |
| `--help` | | | | Show help and exit. |

> **Note:** `--aoi` is not required for viewing help. You can run `chelsa-download --help` or `chelsa-download download-present --help` without providing an AOI.

---

### `download-present`

Download and clip **CHELSA v2.1 present-day bioclim climatology** (1981–2010) for your AOI.

```bash
chelsa-download --aoi region.geojson download-present [OPTIONS]
```

| Option | Short | Type | Default | Description |
|---|---|---|---|---|
| `--var` | `-v` | text (repeatable) | all | Filter to specific variables (e.g. `--var bio01 --var bio12`). Omit to download all available. |
| `--limit` | | int | all | Process only the first N files. Useful for testing. |
| `--force` | | flag | `False` | Re-download and overwrite existing outputs. |
| `--max-workers` | | int | global | Override the global `--max-workers` for this command. |
| `--windowed` / `--no-windowed` | | | `--windowed` | Use COG windowed HTTP reads (default) or force full-file downloads. |
| `--unit-normalize` / `--no-unit-normalize` | | | `--unit-normalize` | Convert to physical units (default) or keep raw GeoTIFF values. |

**Available variables:** `bio01`–`bio19`, `scd`

**Examples:**

```bash
# Download all bioclim variables for your AOI
chelsa-download --aoi aoi.geojson download-present

# Download only bio01 and bio12
chelsa-download --aoi aoi.geojson download-present --var bio01 --var bio12

# Test with a single file, full download mode
chelsa-download --aoi aoi.geojson download-present --var bio01 --limit 1 --no-windowed

# Overwrite existing files
chelsa-download --aoi aoi.geojson download-present --force
```

**Output directory:** `outputs/present/` (default)

---

### `download-trace`

Download and clip **CHELSA-TraCE21k paleoclimate bioclim** rasters for your AOI. These cover 20,000 years of centennial bioclimatic variables.

```bash
chelsa-download --aoi region.geojson download-trace [OPTIONS]
```

| Option | Short | Type | Default | Description |
|---|---|---|---|---|
| `--var` | `-v` | text (repeatable) | all | Filter to specific variables (e.g. `--var bio01`). |
| `--time-slice` | `-t` | int (repeatable) | all | Specific time slices in years BP (e.g. `--time-slice -200 --time-slice 0`). Range: -200 to 20. |
| `--time-range` | | text | all | Inclusive time range (e.g. `--time-range -200--100`). |
| `--time-interval` | | int | `None` | Step size within `--time-range` (e.g. `--time-interval 10` for every 10th slice). Must be positive. |
| `--limit` | | int | all | Process only the first N files. |
| `--force` | | flag | `False` | Re-download and overwrite existing outputs. |
| `--max-workers` | | int | global | Override parallel workers. |
| `--windowed` / `--no-windowed` | | | `--windowed` | Windowed COG reads (default) or full downloads. |
| `--unit-normalize` / `--no-unit-normalize` | | | `--unit-normalize` | Physical unit conversion (default) or raw values. |

**Available variables:** `bio01`–`bio19`, `glz`, `orog`, `scd`, and others from the TraCE21k archive.

**Time coverage:** -200 to 20 years BP (Before Present, where 0 = 1950 CE) in centennial steps.

**Examples:**

```bash
# Download bio01 paleoclimate series (limited to 10 files for testing)
chelsa-download --aoi aoi.geojson download-trace --var bio01 --limit 10

# Download bio01 for specific time slices
chelsa-download --aoi aoi.geojson download-trace --var bio01 --time-slice -200 --time-slice -100 --time-slice 0

# Download bio01 every 10th time step from -200 to 0 BP
chelsa-download --aoi aoi.geojson download-trace --var bio01 --time-range -200-0 --time-interval 10

# Download everything with 8 parallel workers
chelsa-download --aoi aoi.geojson download-trace --max-workers 8

# Raw values, no unit conversion
chelsa-download --aoi aoi.geojson download-trace --var bio01 --limit 5 --no-unit-normalize
```

**Output directory:** `outputs/trace/` (default)

---

### `download-present-monthly`

Download and clip **CHELSA v2.1 present-day monthly climatologies** (1981–2010) for your AOI. Includes precipitation and temperature at monthly resolution.

```bash
chelsa-download --aoi region.geojson download-present-monthly [OPTIONS]
```

| Option | Short | Type | Default | Description |
|---|---|---|---|---|
| `--var` | `-v` | text (repeatable) | all | Monthly variable: `pr`, `tasmin`, `tasmax`. |
| `--month` | `-m` | int (repeatable) | 1–12 | Specific months (e.g. `--month 1 --month 7` for Jan & Jul). |
| `--month-range` | | text | all | Inclusive month range (e.g. `--month-range 1-6` for Jan–Jun). |
| `--limit` | | int | all | Process only the first N files. |
| `--force` | | flag | `False` | Overwrite existing outputs. |
| `--max-workers` | | int | global | Override parallel workers. |
| `--windowed` / `--no-windowed` | | | `--windowed` | Windowed COG reads or full downloads. |
| `--unit-normalize` / `--no-unit-normalize` | | | `--unit-normalize` | Physical unit conversion or raw values. |

**Available variables:** `pr` (precipitation, mm/month), `tasmin` (minimum temperature, °C), `tasmax` (maximum temperature, °C)

**Examples:**

```bash
# Download all monthly variables, all months
chelsa-download --aoi aoi.geojson download-present-monthly

# Only precipitation for January through June
chelsa-download --aoi aoi.geojson download-present-monthly --var pr --month-range 1-6

# Only July tasmin and tasmax
chelsa-download --aoi aoi.geojson download-present-monthly --var tasmin --var tasmax --month 7
```

**Output directory:** `outputs/present_monthly/` (default)

---

### `download-trace-monthly`

Download and clip **CHELSA-TraCE21k monthly centennial** data for your AOI. Covers -200 to 20 years BP in centennial steps, with 12 months per time slice.

```bash
chelsa-download --aoi region.geojson download-trace-monthly [OPTIONS]
```

| Option | Short | Type | Default | Description |
|---|---|---|---|---|
| `--var` | `-v` | text (repeatable) | all | Monthly variable: `pr`, `tasmin`, `tasmax`. |
| `--time-slice` | `-t` | int (repeatable) | all | Specific time slices in years BP (e.g. `--time-slice -200 --time-slice 0`). Range: -200 to 20. |
| `--time-range` | | text | all | Inclusive time range (e.g. `--time-range -200--100`). |
| `--time-interval` | | int | `None` | Step size within `--time-range` (e.g. `--time-interval 10` for every 10th slice). Must be positive. |
| `--month` | `-m` | int (repeatable) | 1–12 | Specific months. |
| `--month-range` | | text | all | Inclusive month range (e.g. `--month-range 6-8`). |
| `--limit` | | int | all | Process only the first N files. |
| `--force` | | flag | `False` | Overwrite existing outputs. |
| `--max-workers` | | int | global | Override parallel workers. |
| `--windowed` / `--no-windowed` | | | `--windowed` | Windowed COG reads or full downloads. |
| `--unit-normalize` / `--no-unit-normalize` | | | `--unit-normalize` | Physical unit conversion or raw values. |

**Available variables:** `pr` (precipitation, mm/month), `tasmin` (minimum temperature, °C), `tasmax` (maximum temperature, °C)

**Time coverage:** -200 to 20 years BP (Before Present, where 0 = 1950 CE) in centennial steps.

**Examples:**

```bash
# Download all monthly variables for all time slices
chelsa-download --aoi aoi.geojson download-trace-monthly

# Precipitation only, every 10th time slice from -200 to 0 BP
chelsa-download --aoi aoi.geojson download-trace-monthly --var pr --time-range -200-0 --time-interval 10

# Specific time slices, summer months only
chelsa-download --aoi aoi.geojson download-trace-monthly --time-slice -200 --time-slice -100 --time-slice 0 --month-range 6-8

# tasmin for January at time 0 BP
chelsa-download --aoi aoi.geojson download-trace-monthly --var tasmin --time-slice 0 --month 1
```

**Output directory:** `outputs/trace_monthly/` (default)

---

### `prepare-lists`

Generate the `.txt` file lists and `.meta.json` metadata files that the download commands consume. The package ships with pre-generated lists, so you only need this if the CHELSA buckets change or you want to refresh.

```bash
chelsa-download prepare-lists [OPTIONS]
```

| Option | Short | Type | Default | Description |
|---|---|---|---|---|
| `--kind` | `-k` | text | **required** | Which dataset to list: `trace`, `present`, `present_monthly`, or `trace_monthly`. |
| `--source-json` | | Path | config default | Path to a pre-cached `rclone lsjson` output (only used with `--kind trace`). |

**Step-by-step for TraCE21k lists:**

```bash
# 1. Snapshot the remote (produces a ~30-60 MB JSON)
rclone lsjson chelsa01_trace21k_bioclim: --recursive > lists/raw/chelsatrace_filelist.json

# 2. Build per-variable lists from the snapshot
chelsa-download prepare-lists --kind trace --source-json lists/raw/chelsatrace_filelist.json
```

**For other datasets:**

```bash
# Present-day bioclim (queries the remote live)
chelsa-download prepare-lists --kind present

# Present-day monthly
chelsa-download prepare-lists --kind present_monthly

# TraCE21k monthly
chelsa-download prepare-lists --kind trace_monthly
```

---

## Variables & Units

### Why unit normalization matters

CHELSA v2.1 (present-day) and CHELSA-TraCE21k (paleoclimate) store bioclimatic variables in **different raw encodings**. Both datasets use scaled integers to save space, but the scale factors and offsets are not always the same between versions. Without normalization, combining present-day and paleoclimate rasters in a single analysis would produce incorrect results because the same raw integer means different things in each dataset.

By default, `chelsa-download` converts all outputs to **float32 in physical units** (°C for temperature, mm or kg m⁻² for precipitation) and strips GeoTIFF scale/offset metadata tags. This ensures that present-day and TraCE21k rasters are directly interoperable — you can stack, compare, or feed them into models without worrying about encoding differences.

Additionally, CHELSA-TraCE21k has suspected inconsistencies in its `bio07` (temperature annual range) values. Rather than trusting the raw `bio07` field, the tool recomputes it from `bio05 − bio06` whenever those dependencies are available, producing more reliable results.

All outputs are written as float32 GeoTIFFs with nodata = -9999.

### CHELSA v2.1 (present-day) bioclim conversions

Raw values in CHELSA v2.1 are stored as scaled integers: temperatures in Kelvin × 10, precipitation in kg m⁻² (or × 0.1).

| Variable | Description | Units | Conversion |
|---|---|---|---|
| `bio01` | Annual mean temperature | °C | `raw × 0.1 − 273.15` |
| `bio02` | Mean diurnal range | °C | `raw × 0.1` |
| `bio03` | Isothermality | % | `raw × 0.1` (or recomputed as `100 × bio02 / bio07`) |
| `bio04` | Temperature seasonality (std dev) | °C | `raw × 0.1` |
| `bio05` | Max temperature of warmest month | °C | `raw × 0.1 − 273.15` |
| `bio06` | Min temperature of coldest month | °C | `raw × 0.1 − 273.15` |
| `bio07` | Temperature annual range | °C | `raw × 0.1` (or recomputed as `bio05 − bio06`) |
| `bio08` | Mean temperature of wettest quarter | °C | `raw × 0.1 − 273.15` |
| `bio09` | Mean temperature of driest quarter | °C | `raw × 0.1 − 273.15` |
| `bio10` | Mean temperature of warmest quarter | °C | `raw × 0.1 − 273.15` |
| `bio11` | Mean temperature of coldest quarter | °C | `raw × 0.1 − 273.15` |
| `bio12` | Annual precipitation | kg m⁻² year⁻¹ | Pass-through (no conversion) |
| `bio13`–`bio19` | Monthly/seasonal precipitation | kg m⁻² month⁻¹ | `raw × 0.1` |

### CHELSA-TraCE21k (paleoclimate) bioclim conversions

TraCE21k uses the same general encoding for most variables, but **`bio07` is stored differently** — as an absolute temperature (Kelvin × 10) rather than a range. This is a suspected inconsistency in the dataset. The tool detects this and applies a different fallback conversion for TraCE21k `bio07`, or preferably recomputes it from the already-normalized `bio05` and `bio06`.

| Variable | Description | Units | Conversion |
|---|---|---|---|
| `bio01` | Annual mean temperature | °C | `raw × 0.1 − 273.15` |
| `bio02` | Mean diurnal range | °C | `raw × 0.1` |
| `bio03` | Isothermality | % | `raw × 0.1` (prefer recompute: `100 × bio02 / bio07`) |
| `bio04` | Temperature seasonality (std dev) | °C | `raw × 0.1` |
| `bio05` | Max temperature of warmest month | °C | `raw × 0.1 − 273.15` |
| `bio06` | Min temperature of coldest month | °C | `raw × 0.1 − 273.15` |
| `bio07` | Temperature annual range | °C | **Fallback:** `raw × 0.1 − 273.15` — **Preferred:** recomputed as `bio05 − bio06` |
| `bio08`–`bio11` | Quarterly temperatures | °C | `raw × 0.1 − 273.15` |
| `bio12` | Annual precipitation | kg m⁻² year⁻¹ | Pass-through |
| `bio13`–`bio19` | Monthly/seasonal precipitation | kg m⁻² month⁻¹ | `raw × 0.1` |

### Monthly variable conversions (both datasets)

Monthly variables use the same encoding in both present-day and TraCE21k:

| Variable | Description | Units | Conversion |
|---|---|---|---|
| `pr` | Precipitation | mm | `raw / 10.0` |
| `tasmin` | Minimum temperature | °C | `raw / 10.0 − 273.15` |
| `tasmax` | Maximum temperature | °C | `raw / 10.0 − 273.15` |

### Derived variable recomputation

When all dependencies are present in the same download batch, `bio07` and `bio03` are **automatically recomputed** from their already-normalized source variables instead of relying on the raw conversion. This is especially important for TraCE21k where `bio07` raw values have suspected encoding inconsistencies.

- **`bio07`** = `bio05 − bio06` (temperature annual range in °C, computed from normalized outputs)
- **`bio03`** = `100 × (bio02 / bio07)` (isothermality in %, computed from normalized outputs)

Recomputation is triggered automatically when you download `bio05`, `bio06`, and `bio07` together (or `bio02`, `bio07`, and `bio03` together). If dependencies are missing, the tool falls back to the per-variable raw conversion with a logged warning.

Use `--no-unit-normalize` to skip all conversions and write raw integer values.

---

## Windowed COG Reads

CHELSA GeoTIFFs are cloud-optimized (tiled + overviews). By default, the CLI resolves rclone remotes to public HTTPS URLs and uses GDAL's `/vsicurl/` driver to request only the byte ranges covering your AOI. This avoids downloading the full multi-hundred-MB global files.

**When to use `--no-windowed`:**
- Large AOIs that cover most of the globe (windowed reads become less efficient).
- If the remote can't be mapped to a public HTTP URL (the CLI falls back automatically).
- When you want to keep a local cache of full files.

**GDAL tuning (set automatically):**
- `GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR`
- `GDAL_HTTP_MERGE_CONSECUTIVE_RANGES=YES`
- `GDAL_HTTP_MULTIPLEX=YES` (HTTP/2)
- `CPL_VSIL_CURL_CHUNK_SIZE=1048576` (1 MB chunks)

### Benchmark

Using `scripts/benchmark_windowed.py` with a ~10°×10° AOI:

| Dataset | Remote file size | Windowed time | Full download time | Speedup |
|---|---|---|---|---|
| Present (`CHELSA_bio01_1981-2010_V.2.1.tif`) | 145.2 MB | 2.63 s | 28.81 s | **11×** |
| TraCE21k (`CHELSA_TraCE21k_bio01_-200_V.1.0.tif`) | 121.5 MB | 1.82 s | 24.92 s | **14×** |

---

## Generating File Lists

The CLI uses `.txt` + `.meta.json` pairs to know exactly which files exist, their sizes, and time ranges. The package ships with a set generated on 14-Nov-2025.

| Step | Command | What it creates |
|---|---|---|
| 1 | `rclone lsjson chelsa01_trace21k_bioclim: --recursive > lists/raw/chelsatrace_filelist.json` | Raw JSON snapshot of TraCE21k bucket (~30–60 MB) |
| 2 | `chelsa-download prepare-lists --kind trace --source-json lists/raw/chelsatrace_filelist.json` | `trace_*.txt` + `.meta.json` in lists/ |
| 3 | `chelsa-download prepare-lists --kind present` | `present_*.txt` + `.meta.json` in lists/present/ |
| 4 | `chelsa-download prepare-lists --kind present_monthly` | `present_monthly_*.txt` + `.meta.json` |
| 5 | `chelsa-download prepare-lists --kind trace_monthly` | `trace_monthly_*.txt` + `.meta.json` |

---

## Tips for Large Downloads

- **Cache behavior:** In windowed mode (default), no cache directory is created. In `--no-windowed` mode, `chelsa_cache/` is used as scratch space but is automatically deleted after all jobs complete. Keep the cache on an SSD — each raw GeoTIFF can be 0.5–2 GB.
- **Parallelism:** Tune `--max-workers` to match your network throughput. Default is 6.
- **List freshness:** If the CHELSA bucket changes, regenerate lists with `prepare-lists` so metadata hashes match.
- **AOI CRS:** If your AOI file lacks a CRS, WGS 84 (EPSG:4326) is assumed. Set it explicitly in your GIS software to avoid surprises.
- **Resume:** If downloads stop mid-way, simply rerun the command — existing outputs are skipped unless `--force` is used.

---

## Troubleshooting

| Problem | Solution |
|---|---|
| **"Config file not found"** | Pass `--aoi path/to/AOI.geojson` to use bundled defaults, or create a TOML config. |
| **"rclone: command not found"** | Install rclone from [rclone.org/install](https://rclone.org/install/) and ensure it's on your PATH. |
| **GeoPandas can't open AOI** | Use GeoJSON or GeoPackage. For Shapefiles, keep all sidecar files (`.shx`, `.dbf`, `.prj`) in the same directory. |
| **Downloads stop mid-way** | Check disk space, then rerun with `--force` to retry failed files. |
| **Values look wrong** | You may have used `--no-unit-normalize`. Rerun without it to get physical units. |
| **Slow downloads** | Ensure you're using the default windowed mode (don't pass `--no-windowed`). Reduce `--max-workers` if the server throttles connections. |

---

## Progress Display

During downloads, a single Rich progress bar shows:

- Files completed / total
- Cumulative bytes downloaded
- Live transfer speed
- Elapsed and estimated remaining time

Use `--quiet` to suppress the progress bar, or `--verbose` for per-file debug logging.

---

## Bundled Remotes

The `envicloud.conf` file ships with anonymous S3 remotes for CHELSA data:

| Remote name | Dataset | Endpoint |
|---|---|---|
| `chelsa02_bioclim` | CHELSA v2.1 bioclim (present-day) | `os.unil.cloud.switch.ch` |
| `chelsa02_climatologies` | CHELSA v2.1 monthly climatologies | `os.unil.cloud.switch.ch` |
| `chelsa01_trace21k_bioclim` | CHELSA-TraCE21k bioclim | `os.zhdk.cloud.switch.ch` |
| `chelsa01_trace_centennial` | CHELSA-TraCE21k monthly centennial | `os.zhdk.cloud.switch.ch` |

No credentials required — all buckets use anonymous access.

---

## Related Tools

- **[rchelsa](https://gitlabext.wsl.ch/karger/rchelsa)** — An R package by the CHELSA team for accessing CHELSA data directly from R. If you work primarily in R, check it out.

---

## Citations

If you use CHELSA data in your work, please cite the original datasets and papers:

### CHELSA v2.1 (present-day climatologies)

Karger, D. N., Conrad, O., Böhner, J., Kawohl, T., Kreft, H., Soria-Auza, R. W., Zimmermann, N. E., Linder, H. P., & Kessler, M. (2017). Climatologies at high resolution for the earth's land surface areas. *Scientific Data, 4*(1), 170122. [https://doi.org/10.1038/sdata.2017.122](https://doi.org/10.1038/sdata.2017.122)

Karger, D. N., Conrad, O., Böhner, J., Kawohl, T., Kreft, H., Soria-Auza, R. W., Zimmermann, N. E., Linder, H. P., & Kessler, M. (2021). Climatologies at high resolution for the earth's land surface areas [Dataset]. *EnviDat*. [https://doi.org/10.16904/envidat.228](https://doi.org/10.16904/envidat.228)

### CHELSA-TraCE21k (paleoclimate)

Karger, D. N., Nobis, M. P., Normand, S., Graham, C. H., & Zimmermann, N. E. (2023). CHELSA-TraCE21k – high-resolution (1 km) downscaled transient temperature and precipitation data since the Last Glacial Maximum. *Climate of the Past, 19*(2), 439–456. [https://doi.org/10.5194/cp-19-439-2023](https://doi.org/10.5194/cp-19-439-2023)

Karger, D. N., Nobis, M. P., Normand, S., Graham, C. H., & Zimmermann, N. E. (2020). CHELSA-TraCE21k: Downscaled transient temperature and precipitation data since the last glacial maximum [Dataset]. *EnviDat*. [https://doi.org/10.16904/envidat.211](https://doi.org/10.16904/envidat.211)

---

## License

[MIT](LICENSE)
