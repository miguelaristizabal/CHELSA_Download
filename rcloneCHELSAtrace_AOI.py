#!/usr/bin/env python3
import os
import re
import sys
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import subprocess
import geopandas as gpd
import rioxarray as rxr
import rasterio

# =======================
# Defaults (your paths)
# =======================
DEFAULT_REMOTE = "envicloud"
DEFAULT_PREFIX = "chelsav1/chelsa_trace"          # remote root for TraCE21k
DEFAULT_AOI    = "/penguin/local/paramo/paramo_aoi.geojson"
DEFAULT_LISTS  = "/penguin/local/paramo/lists"    # contains trace_bioXX.txt, trace_glz.txt, ...
DEFAULT_SSD    = "/penguin/local/paramo/chelsacache"
DEFAULT_OUT    = "/penguin/newdh/CHELSA-TraCE21k"

NODATA_VALUE = -9999.0
MAX_WORKERS_DEFAULT = 6  # tune for your network/SSD

# =======================
# Helpers
# =======================
def apply_scale_offset(dataarray, scale, offset):
    x = dataarray.astype("float32")
    if scale is None:
        scale = 1.0
    if offset is None:
        offset = 0.0
    return x * float(scale) + float(offset)

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def read_list_file(fp):
    with open(fp, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

def infer_time_id(filename):
    m = re.search(r'_(\-?\d+)_', filename)
    return int(m.group(1)) if m else 0

def remote_subdir_for(var: str) -> str:
    v = var.lower()
    if v.startswith("bio") or v in {"scd", "swe", "epot", "fcf", "gdd0", "gdd5", "gdd10", "gdd30",
                                    "gsl", "gst", "gts0", "gts5", "gts10", "gts30",
                                    "end0", "end5", "end10", "end30", "lgd"}:
        return "bio"     # bioclims + snow/etc under bio/
    if v in {"dem", "gle", "glz"}:
        return "orog"    # topography & glacier
    if v in {"pr", "tasmin", "tasmax", "tz"}:
        return v         # monthly raws each in their own folder
    return v             # fallback

def process_one(var, filename, aoi_gdf, args):
    """
    var: e.g., 'bio01', 'bio19', 'glz'
    filename: e.g., 'CHELSA_TraCE21k_bio01_-159_V1.0.tif'
    """
    remote_folder = remote_subdir_for(var)
    remote_prefix = f"{args.prefix}/{remote_folder}/"
    src_remote    = f"{args.remote}:{remote_prefix}{filename}"

    ensure_dir(args.ssd)
    var_out_dir = os.path.join(args.out, var)
    ensure_dir(var_out_dir)

    temp_path = os.path.join(args.ssd, filename)         # SSD temp (exact filename)
    out_name  = filename.replace(".tif", "_AOI.tif")
    out_path  = os.path.join(var_out_dir, out_name)

    if os.path.exists(out_path):
        return f"Skipped (exists): {var}/{filename}"

    try:
        # 1) download file -> exact file (copyto avoids directory confusion)
        cmd = ["rclone", "copyto", src_remote, temp_path, "--progress"]
        proc = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0 or not os.path.exists(temp_path):
            err = proc.stderr.decode(errors="ignore") if proc.stderr else ""
            raise RuntimeError(f"rclone copyto failed or file missing. stderr: {err[:200]}")

        # 2) read scale/offset
        with rasterio.open(temp_path) as src:
            src_scale  = src.scales[0]  if src.scales  else 1.0
            src_offset = src.offsets[0] if src.offsets else 0.0

        # 3) clip to AOI, apply scale/offset, write compressed float32
        with rxr.open_rasterio(temp_path, masked=True) as rds:
            clipped = rds.rio.clip(aoi_gdf.to_crs(rds.rio.crs).geometry, from_disk=True)
            if "band" in clipped.dims and clipped.sizes["band"] == 1:
                clipped = clipped.squeeze("band", drop=True)
            clipped = (clipped.astype("float32") * float(src_scale)) + float(src_offset)
            clipped = clipped.fillna(NODATA_VALUE)
            clipped.rio.write_nodata(NODATA_VALUE, inplace=True)
            clipped.rio.to_raster(
                out_path,
                dtype="float32",
                compress="DEFLATE",
                tiled=True,
                blockxsize=256,
                blockysize=256,
                BIGTIFF="IF_NEEDED",
                windowed=True,
            )

        # 4) clean temp
        try: os.remove(temp_path)
        except FileNotFoundError: pass

        return f"Processed: {var}/{filename}"

    except Exception as e:
        if os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass
        return f"❌ Error {var}/{filename}: {e}"


def collect_jobs(lists_dir, only_var=None):
    """
    Returns list of (var, filename) using files trace_{var}.txt in lists_dir.
    """
    jobs = []
    for base in sorted(os.listdir(lists_dir)):
        if not base.startswith("trace_") or not base.endswith(".txt"):
            continue
        var = base[len("trace_"):-len(".txt")]  # e.g., 'bio19', 'glz'
        if only_var and var != only_var:
            continue
        flist_path = os.path.join(lists_dir, base)
        filenames = read_list_file(flist_path)
        filenames.sort(key=infer_time_id)  # ensure chronological
        for fn in filenames:
            jobs.append((var, fn))
    return jobs

# =======================
# Main
# =======================
def main():
    parser = argparse.ArgumentParser(
        description="Download & clip CHELSA-TraCE21k to AOI (fast, parallel)."
    )
    parser.add_argument("--aoi",    default=DEFAULT_AOI,    help="AOI GeoJSON/GeoPackage path")
    parser.add_argument("--lists",  default=DEFAULT_LISTS,  help="Directory with trace_{var}.txt lists")
    parser.add_argument("--ssd",    default=DEFAULT_SSD,    help="SSD cache directory (temp downloads)")
    parser.add_argument("--out",    default=DEFAULT_OUT,    help="HDD output root directory")
    parser.add_argument("--remote", default=DEFAULT_REMOTE, help="rclone remote name")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help="Remote prefix for TraCE data")
    parser.add_argument("--var",    default=None,           help="Process a single variable (e.g., bio19)")
    parser.add_argument("--max-workers", type=int, default=MAX_WORKERS_DEFAULT, help="Parallel workers")

    args = parser.parse_args()

    # preflight
    ensure_dir(args.ssd)
    ensure_dir(args.out)
    if not os.path.exists(args.lists):
        print(f"Lists dir not found: {args.lists}", file=sys.stderr); sys.exit(1)
    if not os.path.exists(args.aoi):
        print(f"AOI file not found: {args.aoi}", file=sys.stderr); sys.exit(1)

    # load AOI
    aoi_gdf = gpd.read_file(args.aoi)
    if aoi_gdf.crs is None:
        aoi_gdf.set_crs(epsg=4326, inplace=True)

    # collect jobs
    jobs = collect_jobs(args.lists, only_var=args.var)
    if not jobs:
        if args.var:
            print(f"No jobs found for var='{args.var}'. Check trace_{args.var}.txt in {args.lists}.", file=sys.stderr)
        else:
            print(f"No jobs found in {args.lists}. Expect files like trace_bio19.txt, trace_glz.txt.", file=sys.stderr)
        sys.exit(1)

    # run
    print(f"Found {len(jobs)} files to process "
          f"({len(set([v for v,_ in jobs]))} variables).")
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {ex.submit(process_one, var, fn, aoi_gdf, args): (var, fn) for var, fn in jobs}
        with tqdm(total=len(futures), desc="CHELSA-TraCE21k", dynamic_ncols=True) as pbar:
            for fut in as_completed(futures):
                msg = fut.result()
                tqdm.write(msg)
                pbar.update(1)
    print("✅ Done.")

if __name__ == "__main__":
    main()
