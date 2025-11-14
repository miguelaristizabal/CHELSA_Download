#!/usr/bin/env python3
import os, re, sys, argparse, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import geopandas as gpd
import rioxarray as rxr
import rasterio

# =======================
# Defaults (edit if you want)
# =======================
DEFAULT_REMOTE = "envicloud"
DEFAULT_PREFIX = "chelsav2/GLOBAL/climatologies/1981-2010"   # CHELSA v2.1 present
DEFAULT_AOI    = "/penguin/local/paramo/paramo_aoi/paramo_aoi.geojson"
DEFAULT_LISTS  = "/penguin/local/paramo/lists/present"       # your present lists dir
DEFAULT_SSD    = "/penguin/local/paramo/chelsacache"
DEFAULT_OUT    = "/penguin/newdh/CHELSA-V2.1/present"

NODATA_VALUE = -9999.0
MAX_WORKERS_DEFAULT = 6

# =======================
# Helpers
# =======================
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def read_list_file(fp):
    with open(fp, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]

def parse_var_from_listname(fname):
    # Accept names like: trace_bio.txt, present_bio.txt, trace_bio01.txt, trace_scd.txt
    m = re.match(r'^(?:trace|present)_(.+)\.txt$', fname)
    return m.group(1) if m else None

def remote_subdir_for(var: str) -> str:
    v = var.lower()
    # All present-day bioclims live under 'bio/'
    if v == "bio" or re.match(r'^bio\d{1,2}$', v):
        return "bio"
    if v == "scd":
        return "scd"
    # Add others if you ever need them (rare for v2.1 present)
    raise ValueError(f"Unknown present-day variable group '{var}'. Expected 'bio'/'bioXX' or 'scd'.")

def apply_scale_offset(dataarray, scale, offset):
    x = dataarray.astype("float32")
    if scale is None:  scale = 1.0
    if offset is None: offset = 0.0
    return x * float(scale) + float(offset)

def process_one(var, filename, aoi_gdf, args):
    """
    var: 'bio' | 'bio01' | ... | 'scd'
    filename: e.g., 'CHELSA_bio01_1981-2010_V.2.1.tif'
    """
    remote_folder = remote_subdir_for(var)
    src_remote = f"{args.remote}:{args.prefix}/{remote_folder}/{filename}"

    ensure_dir(args.ssd)
    group_out = remote_folder  # write under 'bio/' or 'scd/'
    out_dir = os.path.join(args.out, group_out)
    ensure_dir(out_dir)

    temp_path = os.path.join(args.ssd, filename)                 # SSD temp
    out_name  = filename.replace(".tif", "_AOI.tif")
    out_path  = os.path.join(out_dir, out_name)

    if os.path.exists(out_path):
        return f"Skipped (exists): {group_out}/{filename}"

    try:
        # 1) download file -> exact path
        cmd = ["rclone", "copyto", src_remote, temp_path, "--progress"]
        proc = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if proc.returncode != 0 or not os.path.exists(temp_path):
            err = proc.stderr.decode(errors="ignore") if proc.stderr else ""
            raise RuntimeError(f"rclone copyto failed or file missing. stderr: {err[:200]}")

        # 2) read scale/offset then clip & write
        with rasterio.open(temp_path) as src:
            src_scale  = src.scales[0]  if src.scales  else 1.0
            src_offset = src.offsets[0] if src.offsets else 0.0

        with rxr.open_rasterio(temp_path, masked=True) as rds:
            clipped = rds.rio.clip(aoi_gdf.to_crs(rds.rio.crs).geometry, from_disk=True)
            if "band" in clipped.dims and clipped.sizes["band"] == 1:
                clipped = clipped.squeeze("band", drop=True)
            clipped = apply_scale_offset(clipped, src_scale, src_offset)
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

        try: os.remove(temp_path)
        except FileNotFoundError: pass

        return f"Processed: {group_out}/{filename}"

    except Exception as e:
        if os.path.exists(temp_path):
            try: os.remove(temp_path)
            except: pass
        return f"❌ Error {group_out}/{filename}: {e}"

def collect_jobs(lists_dir, only_var=None):
    """
    Read any present_* or trace_* list in lists_dir and return [(var, filename), ...]
    For present-day v2.1, filenames do NOT contain timeIDs — order is irrelevant.
    """
    jobs = []
    for base in sorted(os.listdir(lists_dir)):
        if not base.endswith(".txt"):
            continue
        var = parse_var_from_listname(base)
        if not var:
            continue
        if only_var and var != only_var:
            continue
        names = read_list_file(os.path.join(lists_dir, base))
        for fn in names:
            jobs.append((var, fn))
    return jobs

def main():
    p = argparse.ArgumentParser(description="Download & clip CHELSA v2.1 (1981–2010) to AOI (fast, parallel).")
    p.add_argument("--aoi",    default=DEFAULT_AOI,    help="AOI path (GeoJSON/GeoPackage/Shapefile)")
    p.add_argument("--lists",  default=DEFAULT_LISTS,  help="Directory with present_* or trace_* .txt lists")
    p.add_argument("--ssd",    default=DEFAULT_SSD,    help="SSD cache directory")
    p.add_argument("--out",    default=DEFAULT_OUT,    help="Output root directory")
    p.add_argument("--remote", default=DEFAULT_REMOTE, help="rclone remote name")
    p.add_argument("--prefix", default=DEFAULT_PREFIX, help="Remote prefix (v2.1 present)")
    p.add_argument("--var",    default=None,           help="Only process a given var key (e.g., bio, bio01, scd)")
    p.add_argument("--max-workers", type=int, default=MAX_WORKERS_DEFAULT)
    args = p.parse_args()

    ensure_dir(args.ssd); ensure_dir(args.out)
    if not os.path.exists(args.lists):
        print(f"Lists dir not found: {args.lists}", file=sys.stderr); sys.exit(1)
    if not os.path.exists(args.aoi):
        print(f"AOI file not found: {args.aoi}", file=sys.stderr); sys.exit(1)

    aoi_gdf = gpd.read_file(args.aoi)
    if aoi_gdf.crs is None:
        aoi_gdf.set_crs(epsg=4326, inplace=True)

    jobs = collect_jobs(args.lists, only_var=args.var)
    if not jobs:
        print(f"No jobs found in {args.lists}. Expect files like present_bio.txt or trace_bio01.txt.", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(jobs)} files to process ({len(set(v for v,_ in jobs))} list-groups).")
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futures = {ex.submit(process_one, var, fn, aoi_gdf, args): (var, fn) for var, fn in jobs}
        with tqdm(total=len(futures), desc="CHELSA v2.1", dynamic_ncols=True) as pbar:
            for fut in as_completed(futures):
                msg = fut.result()
                tqdm.write(msg)
                pbar.update(1)
    print("✅ Done.")

if __name__ == "__main__":
    main()
