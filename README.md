## CHELSA Download CLI

A Python CLI to efficiently download present and past bioclim rasters from [CHELSA](https://www.chelsa-climate.org/) only for a region of your choosing.

CHELSA distributes global GeoTIFFs in public cloud buckets. Pulling those multi‑GB rasters every time you need a small Area of Interest (AOI) is wasteful, especially for the massive TraCE21k archive. `chelsa-download` bundles rclone remotes, curated file lists, and an AOI-first download pipeline so that you only fetch, clip, and keep what you actually need.

---

### Quick start


#### 1) (Recommended) create and activate a virtual environment
```bash
python -m venv .venv
```
##### macOS/Linux
```bash
source .venv/bin/activate
```
##### Windows PowerShell
```powershell
.venv\Scripts\Activate.ps1
```

#### 2) Ensure Python and rclone are available
- If you do not yet have Python installed, see the [prerequisites](#prerequisites--recommended-setup) section.
> **Install rclone (required for downloads):**
##### Windows
```powershell
winget install Rclone.Rclone
```
 ##### macOS/Linux
```bash
sudo -v ; curl https://rclone.org/install.sh | sudo bash
```
  - Verify: `rclone version`

#### 3) Install CHELSA_Download v0.2.3 from the GitHub tag tarball
```bash
python -m pip install "https://github.com/miguelaristizabal/CHELSA_Download/archive/refs/tags/v0.2.3.tar.gz"
```
#### 4) Run your first download (uses bundled lists and default remotes)
```bash
chelsa-download --aoi path/to/AOI.geojson download-present --var bio01 --limit 1
```

This uses the pre generated lists bundled with the package, the envicloud rclone remotes, and writes clipped rasters to `outputs/present` in the current working directory.

To install a different version, replace `v0.2.3` in the URL with the tag you want.

#### Development install (from source)

```bash
# 1) Clone and enter the project
git clone https://github.com/miguelaristizabal/CHELSA_Download.git
cd CHELSA_Download

# 2) Create and activate a virtual environment
python -m venv .venv
# macOS/Linux
source .venv/bin/activate
# Windows PowerShell
.venv\Scripts\Activate.ps1

# 3) Install the CLI (plus dev extras if you plan to hack on it)
python -m pip install -e .[dev]

# 4) (Optional) Copy and edit the sample config, or supply --aoi later
cp chelsa-download.example.toml ~/.chelsa-download.toml

# 5) Run your first download (uses bundled lists and default remotes)
chelsa-download --aoi path/to/AOI.geojson download-present --var bio01 --limit 1
```
---

### Why this tool?

- **Pre-baked remotes & lists.** The repo ships with `envicloud.conf` and `lists/` so you can run downloads immediately. If the included lists are out of date, regenerate them later with `prepare-lists`. See the official dataset pages for bucket links & variable descriptions: [CHELSA-TraCE21k bioclim](https://www.chelsa-climate.org/datasets/chelsa-trace21k-centennial-bioclim) and [CHELSA Bioclim+](https://www.chelsa-climate.org/datasets/chelsa_bioclim).
- **AOI-centric downloads.** Files land in a cache, get clipped to your AOI, masked cells are filled with the declared nodata, and only the AOI raster is written.
- **Resilient transfers.** rclone copy/retry logic is wrapped in a reusable helper. Cache files allow resuming long downloads without redownloading everything.
- **Structured logging & progress.** A single Rich progress bar shows total files, cumulative download size, and live transfer speeds, while the logger records per-file results.

---

### Prerequisites & recommended setup

1. **Python 3.10+**
   - macOS/Linux: `python3 --version`, install via [python.org](https://www.python.org/downloads/) or `brew install python`.
   - Windows: [Download the official installer](https://www.python.org/downloads/windows/) and enable "Add python.exe to PATH".
   - Preferred Conda path: install [Miniforge](https://conda-forge.org/miniforge/) (optimized for conda-forge) and create an env: `conda create -n chelsa python=3.11 && conda activate chelsa`.
   - Or create a venv: `python -m venv .venv && source .venv/bin/activate` (Linux/macOS) or `.venv\Scripts\Activate.ps1` (PowerShell).

     _If you already have your own Python setup, stick with that!_

2. **rclone**
   - Windows: `winget install Rclone.Rclone`
   - macOS/Linux: `sudo -v ; curl https://rclone.org/install.sh | sudo bash`
   - Verify: `rclone version`
   - The bundled `envicloud.conf` already contains anonymous remotes for CHELSA v2.1 (`chelsa02_bioclim`) and TraCE21k (`chelsa01_trace21k_bioclim`).

3. **AOI geometry**
   - Supply a GeoJSON/GeoPackage/Shapefile readable by GeoPandas.
   - To try downloading with the default config, just pass `--aoi path/to/aoi.geojson` the first time you run the CLI.

---

### Configure the tool

You can run entirely from the built-in defaults (just pass `--aoi path/to/aoi`). On first launch the CLI copies the bundled lists/config into `~/.chelsa-download/` so you get a writable workspace even when installing from PyPI or a GitHub release. For more control, copy the sample TOML and tweak the paths—everything is relative to the project directory by default.

```toml
[paths]
aoi = "./AOI.geojson"                     # user-provided AOI when not using --aoi
lists_dir = "./lists"                     # defaults copied to ~/.chelsa-download/lists
cache_dir = "./chelsa_cache"              # SSD scratch space
trace_filelist_json = "./lists/raw/chelsatrace_filelist.json"

[rclone]
config = "./envicloud.conf"               # provided remotes (envicloud S3)

[present]
remote = "chelsa02_bioclim"
lists_subdir = "present"
output_dir = "./outputs/present"          # AOI-clipped present-day GeoTIFFs

[trace]
remote = "chelsa01_trace21k_bioclim"
lists_subdir = "."
output_dir = "./outputs/trace"            # AOI-clipped TraCE21k GeoTIFFs
```

Clipped rasters land in the `output_dir` for each section with `_AOI` appended to the filename.

---

### Generate file lists

The CLI uses `.txt` + `.meta.json` pairs to know exactly which files exist, their sizes, and time ranges. The repo already ships with a set from 14-Nov-2025, but here’s how to regenerate them:

| Step | Command | Files created | Location | Notes |
| --- | --- | --- | --- | --- |
| 1 | `rclone lsjson chelsa01_trace21k_bioclim: --recursive > lists/raw/chelsatrace_filelist.json` | `chelsatrace_filelist.json` | `lists/raw/` | ~30–60 MB JSON snapshot |
| 2 | `chelsa-download prepare-lists --kind trace --source-json lists/raw/chelsatrace_filelist.json` | `trace_*.txt` + `.meta.json` | `lists/` | Sorted chronologically with SHA-1 digests |
| 3 | `chelsa-download prepare-lists --kind present` | `present_*.txt` + `.meta.json` | `lists/present/` | Uses live `rclone lsjson` via `chelsa02_bioclim` |

Expect a few minutes for the TraCE21k snapshot and less than a minute for the present-day listing. Disk usage is modest (<100 MB total).

---

### Downloading data

Each download command consumes the list metadata, pulls the needed files via rclone, writes them to the cache, clips to your AOI, fills nodata (-9999), and writes tiled/deflated GeoTIFFs. All commands honor global flags such as `--quiet/--verbose`, `--limit`, `--var`, `--force`, and `--max-workers`.

| Command | Purpose | Common flags | Typical use | More info |
| --- | --- | --- | --- | --- |
| `chelsa-download download-present` | CHELSA v2.1 climatology (1981-2010) | `--var bio01`, `--limit 5`, `--force`, `--max-workers 6` | Clip modern climatology layers for your AOI | [dataset info](https://www.chelsa-climate.org/datasets/chelsa_bioclim), [citation](https://www.doi.org/10.16904/envidat.332) |
| `chelsa-download download-trace` | CHELSA TraCE21k paleoclimate | `--var bio01`, `--limit 50`, `--max-workers 4` | Pull long paleoclimate series for model training | [dataset info](https://www.chelsa-climate.org/datasets/chelsa-trace21k-centennial-bioclim), [citation](https://www.doi.org/10.16904/envidat.211) |

Example:

```bash
# Quiet present-day fetch for all bioclims (using defaults)
chelsa-download --aoi data/my_aoi.geojson download-present --max-workers 4

# TraCE subset with limited files for testing
chelsa-download --aoi data/my_aoi.geojson download-trace --var bio01 --limit 10
```

During downloads you’ll see a single progress bar with:
- Total files completed vs. total in the list
- Aggregate bytes downloaded (using list metadata)
- Live download speed (averaged since the job started)
- Elapsed and estimated remaining time

---

### Tips for large pulls

- **Cache sizing:** Keep `./chelsa_cache` on an SSD with at least a few GB free; each raw TIFF can be 0.5–2 GB.
- **Parallelism:** Tune `--max-workers` (or `downloads.max_workers`) to match your network/storage throughput.
- **List freshness:** If the CHELSA bucket changes, rerun the `prepare-lists` commands so metadata hashes match the `.txt` files.
- **AOI CRS:** If your AOI lacks a CRS the tool assumes WGS84 (EPSG:4326). Set it explicitly in your GIS before running downloads.

---

### Troubleshooting / FAQ

- **“Config file not found”** – Pass `--aoi path/to/AOI.geojson` and the CLI will use the bundled defaults. Copy the sample TOML later if you need custom paths.
- **“rclone: command not found”** – Install rclone from [rclone.org/install](https://rclone.org/install/) and make sure it’s on your PATH. You can also point `rclone.config` at a different config file if needed.
- **GeoPandas can’t open my AOI** – Convert the AOI to GeoJSON or GeoPackage. Shapefiles must include all sidecar files in the same directory.
- **Downloads stop mid-way** – Check disk space in the cache/output folders and rerun the command with `--force` to retry failed files.
- **Wrong files got clipped** – Confirm you regenerated the lists after changing remotes, and verify the AOI path (logged at start-up).

With the bundled lists, default remotes, and AOI override, you can go from zero to clipped CHELSA rasters in a couple of commands—then fine-tune via the TOML whenever you need more control. Happy downloading!
