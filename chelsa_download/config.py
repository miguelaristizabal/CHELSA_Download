from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

try:  # Python >=3.11
    import tomllib  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for <3.11
    import tomli as tomllib  # type: ignore


CONFIG_ENV = "CHELSA_DOWNLOAD_CONFIG"
DEFAULT_CONFIG_PATH = Path.home() / ".chelsa-download.toml"
PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent


def _expand(path: Optional[str | Path]) -> Optional[Path]:
    if path is None:
        return None
    return Path(os.path.expanduser(str(path))).resolve()


@dataclass
class TargetConfig:
    """Configuration for a single dataset target."""

    remote: str
    prefix: str = ""
    lists_subdir: str = ""
    output_dir: Path = Path("./output")
    nodata_value: float = -9999.0


@dataclass
class GlobalConfig:
    """Runtime configuration shared by the CLI commands."""

    aoi_path: Path
    lists_dir: Path
    cache_dir: Path
    rclone_config: Optional[Path] = None
    max_workers: int = 4
    trace_filelist_json: Optional[Path] = None
    present: TargetConfig = field(default_factory=lambda: TargetConfig(remote="chelsa02_bioclim"))
    trace: TargetConfig = field(default_factory=lambda: TargetConfig(remote="chelsa01_trace21k_bioclim"))

    @classmethod
    def load(cls, path: Optional[Path] = None) -> "GlobalConfig":
        """Load configuration from disk, falling back to defaults."""
        cfg_path = (
            Path(path)
            if path
            else Path(os.environ.get(CONFIG_ENV, DEFAULT_CONFIG_PATH))
        )
        if not cfg_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {cfg_path}. "
                "Run `chelsa-download configure` or copy chelsa-download.example.toml."
            )

        with open(cfg_path, "rb") as fh:
            raw = tomllib.load(fh)

        paths = raw.get("paths", {})
        downloads = raw.get("downloads", {})
        trace_cfg = cls._parse_target(raw.get("trace", {}))
        present_cfg = cls._parse_target(raw.get("present", {}))

        return cls(
            aoi_path=_expand(paths.get("aoi")) or Path("./aoi.geojson"),
            lists_dir=_expand(paths.get("lists_dir")) or Path("./lists"),
            cache_dir=_expand(paths.get("cache_dir")) or Path("./cache"),
            rclone_config=_expand(raw.get("rclone", {}).get("config")),
            max_workers=int(downloads.get("max_workers", 4)),
            trace_filelist_json=_expand(paths.get("trace_filelist_json")),
            present=present_cfg,
            trace=trace_cfg,
        )

    @staticmethod
    def _parse_target(section: Dict[str, Any]) -> TargetConfig:
        return TargetConfig(
            remote=section.get("remote", ""),
            prefix=section.get("prefix", ""),
            lists_subdir=section.get("lists_subdir", ""),
            output_dir=_expand(section.get("output_dir") or "./output"),
            nodata_value=float(section.get("nodata_value", -9999.0)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "aoi_path": str(self.aoi_path),
            "lists_dir": str(self.lists_dir),
            "cache_dir": str(self.cache_dir),
            "rclone_config": str(self.rclone_config) if self.rclone_config else None,
            "max_workers": self.max_workers,
            "trace_filelist_json": str(self.trace_filelist_json) if self.trace_filelist_json else None,
            "present": self.present.__dict__,
            "trace": self.trace.__dict__,
        }

    @classmethod
    def default(cls, aoi_path: Path) -> "GlobalConfig":
        base = PROJECT_ROOT if (PROJECT_ROOT / "lists").exists() else Path.cwd()
        lists_dir = (base / "lists").resolve()
        cache_dir = (Path.cwd() / "chelsa_cache").resolve()
        outputs_root = (Path.cwd() / "outputs").resolve()
        rclone_path = (base / "envicloud.conf").resolve()
        if not rclone_path.exists():
            rclone_path = None
        trace_json = (lists_dir / "raw" / "chelsatrace_filelist.json").resolve()
        if not trace_json.exists():
            trace_json = None

        present = TargetConfig(
            remote="chelsa02_bioclim",
            prefix="",
            lists_subdir="present",
            output_dir=(outputs_root / "present").resolve(),
            nodata_value=-9999.0,
        )
        trace = TargetConfig(
            remote="chelsa01_trace21k_bioclim",
            prefix="",
            lists_subdir=".",
            output_dir=(outputs_root / "trace").resolve(),
            nodata_value=-9999.0,
        )

        return cls(
            aoi_path=aoi_path.resolve(),
            lists_dir=lists_dir,
            cache_dir=cache_dir,
            rclone_config=rclone_path,
            max_workers=4,
            trace_filelist_json=trace_json,
            present=present,
            trace=trace,
        )


def compute_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()
