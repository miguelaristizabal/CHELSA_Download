import logging

import numpy as np
import rioxarray  # noqa: F401
import xarray as xr

from chelsa_download.units import NormalizationContext, normalize_units, recompute_bio03, recompute_bio07


def _logger():
    logger = logging.getLogger("chelsa-download-test")
    logger.setLevel(logging.DEBUG)
    return logger


def test_normalize_present_absolute_temperature():
    raw = xr.DataArray(np.array([[2731.0, -9999.0]], dtype=np.float32))
    ctx = NormalizationContext(unit_normalize=True, recompute_bio07=False, recompute_bio03=False)
    normalized, tags = normalize_units("present", "bio01", raw, -9999.0, _logger(), ctx)
    assert np.isclose(float(normalized.data[0, 0]), -0.05, atol=1e-2)
    assert float(normalized.data[0, 1]) == -9999.0
    assert tags["units"] == "degC"


def test_normalize_trace_bio07_fallback():
    raw = xr.DataArray(np.array([2812.0], dtype=np.float32))
    ctx = NormalizationContext(unit_normalize=True, recompute_bio07=False, recompute_bio03=False)
    normalized, tags = normalize_units("trace", "bio07", raw, -9999.0, _logger(), ctx)
    assert np.isclose(float(normalized.data[0]), 8.05, atol=1e-2)
    assert tags["units"] == "degC"


def test_recompute_bio07_and_bio03():
    bio05 = np.array([10.0, -9999.0], dtype=np.float32)
    bio06 = np.array([4.0, 2.0], dtype=np.float32)
    bio07 = recompute_bio07(bio05, bio06, -9999.0, _logger())
    assert np.isclose(float(bio07[0]), 6.0, atol=1e-6)
    assert float(bio07[1]) == -9999.0

    bio02 = np.array([20.0, 5.0], dtype=np.float32)
    bio03 = recompute_bio03(bio02, bio07, -9999.0, _logger())
    assert np.isclose(float(bio03[0]), 100.0 * (20.0 / 6.0), atol=1e-2)
    assert float(bio03[1]) == -9999.0


def test_normalize_present_monthly_pr_scales_by_tenth():
    raw = xr.DataArray(np.array([123.0, -9999.0], dtype=np.float32))
    ctx = NormalizationContext(unit_normalize=True, recompute_bio07=False, recompute_bio03=False)
    normalized, tags = normalize_units("present_monthly", "pr", raw, -9999.0, _logger(), ctx)
    assert np.isclose(float(normalized.data[0]), 12.3, atol=1e-6)
    assert float(normalized.data[1]) == -9999.0
    assert tags["units"] == "mm"


def test_normalize_trace_monthly_pr_uses_raw_mm_values():
    raw = xr.DataArray(np.array([123.0, -9999.0], dtype=np.float32))
    ctx = NormalizationContext(unit_normalize=True, recompute_bio07=False, recompute_bio03=False)
    normalized, tags = normalize_units("trace_monthly", "pr", raw, -9999.0, _logger(), ctx)
    assert np.isclose(float(normalized.data[0]), 123.0, atol=1e-6)
    assert float(normalized.data[1]) == -9999.0
    assert tags["units"] == "mm"
