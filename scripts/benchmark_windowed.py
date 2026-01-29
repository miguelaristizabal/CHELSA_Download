"""
Benchmark windowed COG reads vs full-file HTTP downloads.

Example:
  python scripts/benchmark_windowed.py --url "https://.../CHELSA_bio01_1981-2010_V.2.1.tif" --bounds -10 35 10 45 --full-download
"""

from __future__ import annotations

import argparse
import os
import time
import urllib.request
from typing import Iterable, Optional, Sequence

import rasterio
from rasterio.env import Env
from rasterio.windows import Window, from_bounds


def _set_http_debug() -> None:
    os.environ["CPL_CURL_VERBOSE"] = "YES"
    os.environ["CPL_VSIL_CURL_VERBOSE"] = "YES"
    os.environ["CPL_VSIL_CURL_STATS"] = "YES"
    os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif"
    os.environ["GDAL_HTTP_MULTIRANGE"] = "YES"


def _parse_window(values: Sequence[str]) -> Window:
    if len(values) != 4:
        raise argparse.ArgumentTypeError("Window requires 4 values: col row width height")
    col, row, width, height = (float(v) for v in values)
    return Window(col, row, width, height)


def _parse_bounds(values: Sequence[str]) -> tuple[float, float, float, float]:
    if len(values) != 4:
        raise argparse.ArgumentTypeError("Bounds require 4 values: minx miny maxx maxy")
    return tuple(float(v) for v in values)  # type: ignore[return-value]


def _bounds_from_aoi(path: str, target_crs) -> tuple[float, float, float, float]:
    try:
        import geopandas as gpd
    except ModuleNotFoundError as exc:
        raise SystemExit("geopandas is required for --aoi") from exc

    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    if target_crs and gdf.crs != target_crs:
        gdf = gdf.to_crs(target_crs)
    minx, miny, maxx, maxy = gdf.total_bounds
    return float(minx), float(miny), float(maxx), float(maxy)


def _head_content_length(url: str) -> Optional[int]:
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=60) as resp:
            length = resp.headers.get("Content-Length")
            return int(length) if length else None
    except Exception:
        return None


def _human_bytes(num: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TB"


def _download_full(url: str) -> tuple[int, float]:
    start = time.perf_counter()
    total = 0
    with urllib.request.urlopen(url, timeout=60) as resp:
        while True:
            chunk = resp.read(1024 * 1024 * 8)
            if not chunk:
                break
            total += len(chunk)
    elapsed = time.perf_counter() - start
    return total, elapsed


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark windowed reads vs full downloads for a COG.")
    parser.add_argument("--url", required=True, help="Remote GeoTIFF URL (HTTP/HTTPS).")
    parser.add_argument("--bounds", nargs=4, metavar=("minx", "miny", "maxx", "maxy"))
    parser.add_argument("--window", nargs=4, metavar=("col", "row", "width", "height"))
    parser.add_argument("--aoi", help="AOI file path (GeoJSON/GPKG/etc).")
    parser.add_argument("--full-download", action="store_true", help="Also download the full file over HTTP.")
    parser.add_argument("--show-http", action="store_true", help="Enable GDAL HTTP verbose logging.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.show_http:
        _set_http_debug()

    with Env():
        with rasterio.open(args.url) as ds:
            window = None
            if args.window:
                window = _parse_window(args.window)
            elif args.bounds:
                minx, miny, maxx, maxy = _parse_bounds(args.bounds)
                window = from_bounds(minx, miny, maxx, maxy, transform=ds.transform)
            elif args.aoi:
                minx, miny, maxx, maxy = _bounds_from_aoi(args.aoi, ds.crs)
                window = from_bounds(minx, miny, maxx, maxy, transform=ds.transform)
            if window is None:
                window = Window(0, 0, 1, 1)
            window = window.round_offsets().round_lengths()

            print("Opened:", ds.name)
            print("Size:", ds.width, ds.height)
            print("CRS:", ds.crs)
            print("Tiled:", ds.is_tiled)
            print("Block shapes:", ds.block_shapes)
            print("Overviews:", ds.overviews(1))
            print("Window:", window)

            start = time.perf_counter()
            data = ds.read(1, window=window)
            window_time = time.perf_counter() - start
            print("Window read:", data.shape, "time", f"{window_time:.3f}s")

    full_len = _head_content_length(args.url)
    if full_len:
        print("Remote file size:", _human_bytes(full_len))

    if args.full_download:
        total, elapsed = _download_full(args.url)
        print("Full download:", _human_bytes(total), "time", f"{elapsed:.3f}s")
        if window_time > 0:
            print("Speedup (time):", f"{elapsed / window_time:.1f}x")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
