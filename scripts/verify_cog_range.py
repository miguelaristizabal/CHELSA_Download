"""
Verify that a remote GeoTIFF supports HTTP range reads and windowed access.

Examples:
  python scripts/verify_cog_range.py --url "https://.../CHELSA_bio01_1981-2010_V.2.1.tif" --bounds -10 35 10 45 --show-http
  python scripts/verify_cog_range.py --url "https://.../CHELSA_TraCE21k_bio01_-200_V.1.0.tif" --window 0 0 1 1 --show-http
  python scripts/verify_cog_range.py --url "https://.../CHELSA_bio01_1981-2010_V.2.1.tif" --aoi path/to/aoi.geojson
"""

from __future__ import annotations

import argparse
import os
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


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Verify partial range reads for remote GeoTIFFs.")
    parser.add_argument("--url", required=True, help="Remote GeoTIFF URL (HTTP/HTTPS).")
    parser.add_argument("--bounds", nargs=4, metavar=("minx", "miny", "maxx", "maxy"))
    parser.add_argument("--window", nargs=4, metavar=("col", "row", "width", "height"))
    parser.add_argument("--aoi", help="AOI file path (GeoJSON/GPKG/etc).")
    parser.add_argument("--show-http", action="store_true", help="Enable GDAL HTTP verbose logging.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.show_http:
        _set_http_debug()

    with Env():
        with rasterio.open(args.url) as ds:
            print("Opened:", ds.name)
            print("Size:", ds.width, ds.height)
            print("CRS:", ds.crs)
            print("Tiled:", ds.is_tiled)
            print("Block shapes:", ds.block_shapes)
            print("Overviews:", ds.overviews(1))

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
                # Default to a tiny read at the top-left to force a minimal range request.
                window = Window(0, 0, 1, 1)

            window = window.round_offsets().round_lengths()
            data = ds.read(1, window=window)
            print("Window:", window)
            print("Read shape:", data.shape)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
