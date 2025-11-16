from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Optional

from importlib import resources


ASSET_TARGET = Path.home() / ".chelsa-download"


def _copy_file(src: Path, dst: Path) -> None:
    if dst.exists():
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        return
    shutil.copytree(src, dst)
    _refresh_metadata(dst)


def _refresh_metadata(lists_root: Path) -> None:
    if not lists_root.exists():
        return
    for meta_path in lists_root.rglob("*.txt.meta.json"):
        list_path = Path(str(meta_path).replace(".meta.json", ""))
        if not list_path.exists():
            continue
        sha1 = _compute_sha1(list_path)
        with open(meta_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if data.get("list_sha1") == sha1:
            continue
        data["list_sha1"] = sha1
        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)


def _compute_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_default_assets(target_root: Optional[Path] = None) -> Path:
    """Copy bundled lists/configs into a writable user directory."""
    target = target_root or ASSET_TARGET
    target.mkdir(parents=True, exist_ok=True)

    data_root = resources.files("chelsa_download") / "data"

    _copy_file(data_root / "envicloud.conf", target / "envicloud.conf")
    _copy_file(data_root / "chelsa-download.example.toml", target / "chelsa-download.example.toml")
    _copy_tree(data_root / "lists", target / "lists")
    _refresh_metadata(target / "lists")

    return target
