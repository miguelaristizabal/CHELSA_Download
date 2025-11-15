import numpy as np
import xarray as xr

from chelsa_download.processing import fill_mask


def test_fill_mask_replaces_values():
    masked = np.ma.array([[1, 2], [3, 4]], mask=[[False, True], [False, False]])
    arr = xr.DataArray(masked)
    filled = fill_mask(arr, nodata=-9999.0)
    assert filled.data[0, 1] == -9999.0
