from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import numpy as np

NORMALIZATION_VERSION = "1"

ABS_TEMP_VARS = {"bio01", "bio05", "bio06", "bio08", "bio09", "bio10", "bio11"}
TEMP_RANGE_VARS = {"bio02", "bio04", "bio07"}
PRECIP_ANNUAL_VARS = {"bio12"}
PRECIP_MONTHLY_VARS = {"bio13", "bio14", "bio15", "bio16", "bio17", "bio18", "bio19"}


@dataclass(frozen=True)
class NormalizationContext:
    unit_normalize: bool
    recompute_bio07: bool
    recompute_bio03: bool


def _var_units_and_tags(var: str, unit_normalize: bool) -> Dict[str, str]:
    if not unit_normalize:
        return {
            "unit_normalized": "false",
            "unit_normalization_version": NORMALIZATION_VERSION,
        }

    tags: Dict[str, str] = {
        "unit_normalized": "true",
        "unit_normalization_version": NORMALIZATION_VERSION,
    }
    if var in ABS_TEMP_VARS or var in TEMP_RANGE_VARS:
        tags["units"] = "degC"
    elif var == "bio03":
        tags["units"] = "%"
    elif var in PRECIP_ANNUAL_VARS:
        tags["units"] = "kg m-2 year-1"
    elif var in PRECIP_MONTHLY_VARS:
        tags["units"] = "kg m-2 month-1"

    if var in {"bio02", "bio07"}:
        tags["quantity"] = "temperature_range"
    elif var == "bio04":
        tags["quantity"] = "temperature_stdev"
    elif var == "bio03":
        tags["quantity"] = "isothermality"

    return tags


def _clear_scale_offset(dataarray) -> None:
    """Clear scale/offset attributes but preserve nodata (_FillValue)."""
    for key in ("scale_factor", "add_offset", "scale", "offset"):
        dataarray.attrs.pop(key, None)
        if hasattr(dataarray, "encoding"):
            dataarray.encoding.pop(key, None)


def _data_and_mask(dataarray, nodata_value: float) -> Tuple[np.ndarray, np.ndarray]:
    data = dataarray.data
    if np.ma.isMaskedArray(data):
        mask = np.ma.getmaskarray(data).astype(bool)
        arr = np.ma.filled(data, nodata_value).astype("float32")
    else:
        arr = np.asarray(data).astype("float32")
        mask = np.zeros(arr.shape, dtype=bool)

    if np.issubdtype(arr.dtype, np.floating):
        nan_mask = np.isnan(arr)
        if np.any(nan_mask):
            mask |= nan_mask
            arr = np.where(nan_mask, nodata_value, arr)

    if nodata_value is not None:
        mask |= arr == nodata_value

    source_nodata = getattr(dataarray, "rio", None)
    if source_nodata is not None:
        src = dataarray.rio.nodata
        if src is not None and not np.isnan(src):
            mask |= arr == float(src)

    return arr, mask


def _apply_mask(arr: np.ndarray, mask: np.ndarray, nodata_value: float) -> np.ndarray:
    arr = arr.astype("float32", copy=False)
    arr[mask] = nodata_value
    return arr


def normalize_units(
    product: str,
    varname: str,
    dataarray,
    nodata_value: float,
    logger,
    context: NormalizationContext,
) -> Tuple[object, Dict[str, str]]:
    """Normalize raw raster values into physical units (float32, no scale/offset)."""
    var = varname.lower()
    arr, mask = _data_and_mask(dataarray, nodata_value)
    valid = ~mask

    if context.unit_normalize:
        if var in ABS_TEMP_VARS:
            arr[valid] = arr[valid] * 0.1 - 273.15
        elif var in TEMP_RANGE_VARS:
            if product == "trace" and var == "bio07":
                arr[valid] = arr[valid] * 0.1 - 273.15
                if not context.recompute_bio07:
                    logger.warning(
                        "Trace bio07 fallback scaling used (raw*0.1 - 273.15). Prefer recompute from bio05/bio06."
                    )
            else:
                arr[valid] = arr[valid] * 0.1
        elif var == "bio03":
            # Fallback conversion when recompute is not possible.
            arr[valid] = arr[valid] * 0.1
            if product == "trace" and not context.recompute_bio03:
                logger.warning(
                    "Bio03 fallback scaling used (raw*0.1). Prefer recompute from bio02/bio07."
                )
        elif var in PRECIP_ANNUAL_VARS:
            arr[valid] = arr[valid]
        elif var in PRECIP_MONTHLY_VARS:
            arr[valid] = arr[valid] * 0.1

    arr = _apply_mask(arr, mask, nodata_value)
    dataarray.data = arr
    dataarray = dataarray.astype("float32")
    dataarray.rio.write_nodata(nodata_value, inplace=True)
    _clear_scale_offset(dataarray)

    if context.unit_normalize:
        _validate_normalized(var, arr, nodata_value, logger)

    tags = _var_units_and_tags(var, context.unit_normalize)
    return dataarray, tags


def _median_valid(arr: np.ndarray, nodata_value: float) -> Optional[float]:
    valid = np.isfinite(arr) & (arr != nodata_value)
    if not np.any(valid):
        return None
    return float(np.median(arr[valid]))


def _validate_normalized(var: str, arr: np.ndarray, nodata_value: float, logger) -> None:
    median = _median_valid(arr, nodata_value)
    if median is None:
        return

    if var in ABS_TEMP_VARS:
        if median > 100:
            logger.warning("Median for %s looks like Kelvin (%.2f). Expected degC.", var, median)
        return

    if var in {"bio02", "bio04", "bio07"}:
        if median > 100:
            logger.warning("Median for %s looks too high (%.2f). Expected degC ranges.", var, median)
        if np.nanmin(arr[arr != nodata_value]) < 0:
            logger.warning("Negative values found for %s after normalization.", var)
        return

    if var == "bio03":
        if median > 100 or median < 0:
            logger.warning("Median for bio03 out of expected range (%.2f).", median)
        return

    if var in PRECIP_ANNUAL_VARS | PRECIP_MONTHLY_VARS:
        if np.nanmin(arr[arr != nodata_value]) < 0:
            logger.warning("Negative precipitation values found for %s.", var)


def recompute_bio07(
    bio05_array: np.ndarray,
    bio06_array: np.ndarray,
    nodata_value: float,
    logger,
) -> np.ndarray:
    mask = ~np.isfinite(bio05_array) | ~np.isfinite(bio06_array)
    mask |= bio05_array == nodata_value
    mask |= bio06_array == nodata_value
    result = bio05_array - bio06_array
    result = _apply_mask(result, mask, nodata_value)
    _validate_normalized("bio07", result, nodata_value, logger)
    return result


def recompute_bio03(
    bio02_array: np.ndarray,
    bio07_array: np.ndarray,
    nodata_value: float,
    logger,
) -> np.ndarray:
    mask = ~np.isfinite(bio02_array) | ~np.isfinite(bio07_array)
    mask |= bio02_array == nodata_value
    mask |= bio07_array == nodata_value
    mask |= bio07_array == 0
    result = np.zeros_like(bio02_array, dtype="float32")
    valid = ~mask
    result[valid] = 100.0 * (bio02_array[valid] / bio07_array[valid])
    nonfinite = ~np.isfinite(result) & valid
    if np.any(nonfinite):
        logger.warning("Non-finite values encountered during bio03 recompute; masking them.")
        mask |= nonfinite
    result = _apply_mask(result, mask, nodata_value)
    _validate_normalized("bio03", result, nodata_value, logger)
    return result
