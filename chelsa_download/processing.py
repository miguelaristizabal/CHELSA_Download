from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import rioxarray  # type: ignore


def load_aoi(path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf.set_crs(epsg=4326, inplace=True)
    return gdf


def fill_mask(dataarray, nodata: float):
    data = dataarray.data
    mask = getattr(data, "mask", None)
    if mask is not None and mask is not np.ma.nomask and np.any(mask):
        dataarray.data = np.ma.filled(np.ma.array(data, mask=mask), nodata)
        return dataarray

    # Some DataArrays store masked cells as NaN floats; handle those too.
    if np.issubdtype(getattr(dataarray, "dtype", np.float32), np.floating):
        arr = np.asarray(dataarray.data)
        nan_mask = np.isnan(arr)
        if np.any(nan_mask):
            dataarray.data = np.where(nan_mask, nodata, arr)
    return dataarray


def clip_scale_and_fill(temp_path: Path, aoi_gdf: gpd.GeoDataFrame, nodata: float):
    with rioxarray.open_rasterio(temp_path, masked=True) as rds:
        clipped = rds.rio.clip(aoi_gdf.to_crs(rds.rio.crs).geometry, from_disk=True)
        if "band" in clipped.dims and clipped.sizes.get("band") == 1:
            clipped = clipped.squeeze("band", drop=True)
        clipped = fill_mask(clipped, nodata)
        clipped = clipped.fillna(nodata)
        clipped = clipped.astype("float32")
        clipped.rio.write_nodata(nodata, inplace=True)
        return clipped


def write_raster(dataarray, destination: Path):
    dataarray.rio.to_raster(
        destination,
        dtype="float32",
        compress="DEFLATE",
        tiled=True,
        blockxsize=256,
        blockysize=256,
        BIGTIFF="IF_NEEDED",
        windowed=True,
    )
