from __future__ import annotations

from pathlib import Path
from typing import Union

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


SourcePath = Union[str, Path]


def clip_raster(source: SourcePath, aoi_gdf: gpd.GeoDataFrame):
    """Clip a raster to the AOI without applying scale/offset metadata."""
    with rioxarray.open_rasterio(source, masked=True) as rds:
        clipped = rds.rio.clip(aoi_gdf.to_crs(rds.rio.crs).geometry, from_disk=True)
        if "band" in clipped.dims and clipped.sizes.get("band") == 1:
            clipped = clipped.squeeze("band", drop=True)
        return clipped


def write_raster(dataarray, destination: Path, tags: dict | None = None):
    # Avoid conflicts between attrs and encoding (xarray _FillValue handling).
    dataarray = dataarray.copy(deep=False)
    for key in ("_FillValue", "scale_factor", "add_offset", "scale", "offset"):
        dataarray.attrs.pop(key, None)
        if hasattr(dataarray, "encoding"):
            dataarray.encoding.pop(key, None)
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
    if tags:
        import rasterio

        with rasterio.open(destination, "r+") as ds:
            ds.update_tags(**tags)
