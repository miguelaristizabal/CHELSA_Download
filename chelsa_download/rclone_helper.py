from __future__ import annotations

import json
from configparser import ConfigParser
from functools import lru_cache
import subprocess
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


class RcloneError(RuntimeError):
    """Raised when an rclone invocation fails."""


DEFAULT_HTTP_REMOTES = {
    "chelsa02_bioclim": "https://os.unil.cloud.switch.ch/chelsa02/chelsa/global/bioclim",
    "chelsa01_trace21k_bioclim": "https://os.zhdk.cloud.switch.ch/chelsa01/chelsa_trace21k/global/bioclim",
}


def _base_command(config_path: Optional[Path], *args: str) -> List[str]:
    cmd = ["rclone"]
    if config_path:
        cmd += ["--config", str(config_path)]
    cmd.extend(args)
    return cmd


def run_rclone(args: Iterable[str], config_path: Optional[Path] = None, retries: int = 3, backoff: float = 2.0) -> subprocess.CompletedProcess:
    """Execute an rclone command with retries."""
    args_list = list(args)
    last_exc: Optional[subprocess.CalledProcessError] = None
    for attempt in range(1, retries + 1):
        try:
            proc = subprocess.run(
                _base_command(config_path, *args_list),
                check=True,
                capture_output=True,
                text=True,
            )
            return proc
        except subprocess.CalledProcessError as exc:
            last_exc = exc
            if attempt == retries:
                break
            time.sleep(backoff * attempt)
    assert last_exc is not None
    raise RcloneError(last_exc.stderr or last_exc.stdout)


def copy_to(remote: str, dest: Path, config_path: Optional[Path], retries: int = 3) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    run_rclone(
        ["copyto", remote, str(dest), "--retries", "1", "--low-level-retries", "10", "--no-traverse"],
        config_path=config_path,
        retries=retries,
    )
    return dest


def list_remote(remote: str, recursive: bool = True, config_path: Optional[Path] = None) -> List[Dict[str, object]]:
    args = ["lsjson", remote, "--files-only"]
    if recursive:
        args.append("--recursive")
    proc = run_rclone(args, config_path=config_path)
    return json.loads(proc.stdout)


@lru_cache(maxsize=8)
def _load_rclone_config(path_str: str) -> ConfigParser:
    config = ConfigParser()
    config.read(path_str)
    return config


def _resolve_remote_alias(config: ConfigParser, name: str) -> Optional[Tuple[str, str, Dict[str, str]]]:
    prefix_parts: List[str] = []
    current = name
    visited = set()
    while True:
        if current in visited or not config.has_section(current):
            return None
        visited.add(current)
        section = config[current]
        remote_type = section.get("type", "").strip().lower()
        if remote_type == "alias":
            alias_target = section.get("remote", "")
            base, sep, path = alias_target.partition(":")
            if not sep or not base:
                return None
            if path:
                prefix_parts.append(path.strip("/"))
            current = base
            continue
        prefix = "/".join(reversed([p for p in prefix_parts if p]))
        return current, prefix, dict(section)


def remote_to_http_url(remote: str, config_path: Optional[Path]) -> Optional[str]:
    """Convert an rclone remote path to a public HTTP URL when possible."""
    name, sep, key = remote.partition(":")
    if not sep:
        return None
    key = key.lstrip("/")

    if config_path and config_path.exists():
        config = _load_rclone_config(str(config_path))
        resolved = _resolve_remote_alias(config, name)
        if resolved:
            _, prefix, base_section = resolved
            remote_type = base_section.get("type", "").strip().lower()
            endpoint = base_section.get("endpoint", "").strip().lstrip("https://").lstrip("http://")
            if remote_type == "s3" and endpoint:
                full_key = "/".join(part for part in [prefix, key] if part)
                bucket, _, object_key = full_key.partition("/")
                if bucket and object_key:
                    return f"https://{endpoint.rstrip('/')}/{bucket}/{object_key}"

    base = DEFAULT_HTTP_REMOTES.get(name)
    if base:
        if key:
            return f"{base.rstrip('/')}/{key}"
        return base

    return None
