from pathlib import Path

import pytest

from chelsa_download.config import GlobalConfig, TargetConfig
from chelsa_download.list_manager import (
    ListFileEntry,
    ListManager,
    build_trace_metadata,
    infer_time_id,
    trace_time_id_to_ka,
)


def minimal_config(tmp_path: Path) -> GlobalConfig:
    return GlobalConfig(
        aoi_path=tmp_path / "aoi.geojson",
        lists_dir=tmp_path / "lists",
        cache_dir=tmp_path / "cache",
        rclone_config=None,
        max_workers=1,
        trace_filelist_json=None,
        present=TargetConfig(remote="present"),
        trace=TargetConfig(remote="trace"),
    )


def test_infer_time_id_and_conversion():
    assert infer_time_id("CHELSA_TraCE21k_bio01_-155_V1.0.tif") == -155
    assert infer_time_id("no_time_here") is None
    assert pytest.approx(trace_time_id_to_ka(-200)) == 22.0
    assert pytest.approx(trace_time_id_to_ka(20)) == 0.0


def test_build_trace_metadata(tmp_path: Path):
    source = tmp_path / "trace.json"
    source.write_text("[]")
    entries = [
        {"Name": "CHELSA_TraCE21k_bio01_-100_V1.0.tif", "Size": 1024},
        {"Name": "CHELSA_TraCE21k_bio01_-50_V1.0.tif", "Size": 2048},
    ]
    metadata = build_trace_metadata("bio01", entries, source)
    assert metadata.count == 2
    assert metadata.stats["time_ids"]["min"] == -100
    assert metadata.stats["time_ids"]["max"] == -50


def test_list_manager_round_trip(tmp_path: Path):
    cfg = minimal_config(tmp_path)
    manager = ListManager(cfg)
    list_path = tmp_path / "lists" / "trace_bio01.txt"
    list_path.parent.mkdir(parents=True, exist_ok=True)
    list_path.write_text("CHELSA_TraCE21k_bio01_-100_V1.0.tif\n")
    source = tmp_path / "trace.json"
    source.write_text("[]")
    metadata = build_trace_metadata(
        "bio01",
        [{"Name": "CHELSA_TraCE21k_bio01_-100_V1.0.tif", "Size": 1024}],
        source,
    )
    manager.save_metadata(list_path, metadata)
    loaded = manager.load_metadata(list_path)
    assert isinstance(loaded.files[0], ListFileEntry)

    # Tamper with list to ensure validation fails
    list_path.write_text("different_file.tif\n")
    with pytest.raises(RuntimeError):
        manager.load_metadata(list_path)
