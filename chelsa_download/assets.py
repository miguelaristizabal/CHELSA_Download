from __future__ import annotations

import shutil
from pathlib import Path
from typing import Optional

from importlib import resources


ASSET_TARGET = Path.home() / ".chelsa-download"


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        return
    shutil.copytree(src, dst)


def _copy_file(src: Path, dst: Path) -> None:
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def ensure_default_assets(target_root: Optional[Path] = None) -> Path:
    """Copy bundled lists/configs into a writable user directory."""
    target = target_root or ASSET_TARGET
    target.mkdir(parents=True, exist_ok=True)

    data_root = resources.files("chelsa_download") / "data"

    _copy_file(data_root / "envicloud.conf", target / "envicloud.conf")
    _copy_file(data_root / "chelsa-download.example.toml", target / "chelsa-download.example.toml")
    _copy_tree(data_root / "lists", target / "lists")

    return target
