import numpy as np
import xarray as xr

from chelsa_download.processing import apply_scale_offset, fill_mask


def test_apply_scale_offset():
    arr = xr.DataArray(np.array([1, 2, 3], dtype=np.float32))
    scaled = apply_scale_offset(arr, scale=0.5, offset=1.0)
    assert float(scaled[0]) == 1.5
    assert float(scaled[-1]) == 2.5


def test_fill_mask_replaces_values():
    masked = np.ma.array([[1, 2], [3, 4]], mask=[[False, True], [False, False]])
    arr = xr.DataArray(masked)
    filled = fill_mask(arr, nodata=-9999.0)
    assert filled.data[0, 1] == -9999.0
