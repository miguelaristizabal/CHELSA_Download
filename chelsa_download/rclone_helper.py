from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional


class RcloneError(RuntimeError):
    """Raised when an rclone invocation fails."""


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
