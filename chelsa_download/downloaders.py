from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from rich.progress import (
    BarColumn,
    Progress,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from .config import GlobalConfig
from .list_manager import ListFileEntry, ListManager, ListMetadata, parse_variable_from_listfilename
from .processing import clip_scale_and_fill, load_aoi, write_raster
from .rclone_helper import RcloneError, copy_to, list_remote


TRACE_REMOTE_SPECIAL_FOLDERS = {
    "dem": "orog",
    "gle": "orog",
    "glz": "orog",
    "pr": "pr",
    "tasmin": "tasmin",
    "tasmax": "tasmax",
    "tz": "tz",
}


def trace_remote_subdir(variable: str) -> str:
    v = variable.lower()
    if v.startswith("bio") or v in {
        "scd",
        "swe",
        "epot",
        "fcf",
        "gdd0",
        "gdd5",
        "gdd10",
        "gdd30",
        "gsl",
        "gst",
        "gts0",
        "gts5",
        "gts10",
        "gts30",
        "end0",
        "end5",
        "end10",
        "end30",
        "lgd",
    }:
        return "bio"
    return TRACE_REMOTE_SPECIAL_FOLDERS.get(v, v)


def present_remote_subdir(variable: str) -> str:
    v = variable.lower()
    if v.startswith("bio"):
        return "bio"
    if v in {"scd"}:
        return "scd"
    return v


@dataclass
class DownloadJob:
    kind: str
    variable: str
    entry: ListFileEntry
    metadata: ListMetadata
    remote_path: str
    temp_path: Path
    output_path: Path
    nodata: float
    force: bool = False


def build_remote_path(remote: str, prefix: str, subdir: str, filename: str) -> str:
    pieces = [prefix.strip("/"), subdir.strip("/"), filename]
    joined = "/".join(part for part in pieces if part)
    return f"{remote}:{joined}"


def collect_trace_jobs(
    config: GlobalConfig,
    manager: ListManager,
    vars_filter: Optional[List[str]] = None,
    limit: Optional[int] = None,
    force: bool = False,
) -> List[DownloadJob]:
    lists_root = Path(config.lists_dir)
    if config.trace.lists_subdir:
        lists_root = lists_root / config.trace.lists_subdir
    lists_root = lists_root.resolve()
    jobs: List[DownloadJob] = []
    selected_vars = {v.lower() for v in vars_filter} if vars_filter else None
    for list_path in manager.iter_list_files(lists_root, "trace"):
        variable = parse_variable_from_listfilename(list_path.name)
        if not variable:
            continue
        if selected_vars and variable.lower() not in selected_vars:
            continue
        metadata = manager.load_metadata(list_path)
        subdir = trace_remote_subdir(variable)
        out_dir = Path(config.trace.output_dir) / variable
        out_dir.mkdir(parents=True, exist_ok=True)
        for entry in metadata.files:
            if entry.path:
                joined = "/".join(
                    part
                    for part in [
                        config.trace.prefix.strip("/") if config.trace.prefix else "",
                        entry.path.strip("/"),
                    ]
                    if part
                )
                remote_path = f"{config.trace.remote}:{joined}"
            else:
                remote_path = build_remote_path(config.trace.remote, config.trace.prefix, subdir, entry.name)
            temp_path = Path(config.cache_dir) / entry.name
            out_path = out_dir / entry.name.replace(".tif", "_AOI.tif")
            jobs.append(
                DownloadJob(
                    kind="trace",
                    variable=variable,
                    entry=entry,
                    metadata=metadata,
                    remote_path=remote_path,
                    temp_path=temp_path,
                    output_path=out_path,
                    nodata=config.trace.nodata_value,
                    force=force,
                )
            )
            if limit and len(jobs) >= limit:
                return jobs
    return jobs


def collect_present_jobs(
    config: GlobalConfig,
    manager: ListManager,
    vars_filter: Optional[List[str]] = None,
    limit: Optional[int] = None,
    force: bool = False,
) -> List[DownloadJob]:
    lists_root = Path(config.lists_dir)
    if config.present.lists_subdir:
        lists_root = lists_root / config.present.lists_subdir
    lists_root = lists_root.resolve()
    jobs: List[DownloadJob] = []
    selected_vars = {v.lower() for v in vars_filter} if vars_filter else None
    for list_path in manager.iter_list_files(lists_root, "present"):
        variable = parse_variable_from_listfilename(list_path.name)
        if not variable:
            continue
        if selected_vars and variable.lower() not in selected_vars:
            continue
        metadata = manager.load_metadata(list_path)
        subdir = present_remote_subdir(variable)
        out_dir = Path(config.present.output_dir) / subdir
        out_dir.mkdir(parents=True, exist_ok=True)
        for entry in metadata.files:
            if entry.path:
                joined = "/".join(
                    part
                    for part in [
                        config.present.prefix.strip("/") if config.present.prefix else "",
                        entry.path.strip("/"),
                    ]
                    if part
                )
                remote_path = f"{config.present.remote}:{joined}"
            else:
                remote_path = build_remote_path(config.present.remote, config.present.prefix, subdir, entry.name)
            temp_path = Path(config.cache_dir) / entry.name
            out_path = out_dir / entry.name.replace(".tif", "_AOI.tif")
            jobs.append(
                DownloadJob(
                    kind="present",
                    variable=variable,
                    entry=entry,
                    metadata=metadata,
                    remote_path=remote_path,
                    temp_path=temp_path,
                    output_path=out_path,
                    nodata=config.present.nodata_value,
                    force=force,
                )
            )
            if limit and len(jobs) >= limit:
                return jobs
    return jobs


def _monitor_progress(path: Path, progress: Progress, task_id: TaskID, total: Optional[int], stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        if path.exists():
            size = path.stat().st_size
            if total:
                progress.update(task_id, total=total, completed=min(size, total))
            else:
                progress.update(task_id, completed=size)
        time.sleep(0.2)


def _download_one(job: DownloadJob, config: GlobalConfig, progress: Progress) -> str:
    if job.output_path.exists() and not job.force:
        return f"Skipped (exists): {job.output_path.name}"

    task_total = job.entry.size if job.entry.size and job.entry.size > 0 else None
    task_id = progress.add_task(f"{job.variable}", total=task_total)
    stop_event = threading.Event()
    monitor = threading.Thread(target=_monitor_progress, args=(job.temp_path, progress, task_id, job.entry.size, stop_event))
    monitor.daemon = True
    monitor.start()
    try:
        copy_to(job.remote_path, job.temp_path, config_path=config.rclone_config, retries=3)
        stop_event.set()
        monitor.join(timeout=1)
        if job.entry.size and job.temp_path.stat().st_size != job.entry.size:
            raise RcloneError(
                f"Downloaded size mismatch for {job.entry.name}: "
                f"{job.temp_path.stat().st_size} != {job.entry.size}"
            )
        progress.update(task_id, completed=job.entry.size or job.temp_path.stat().st_size)
    finally:
        stop_event.set()
    progress.remove_task(task_id)
    return "downloaded"


def _process_one(job: DownloadJob, aoi, logger) -> str:
    clipped = clip_scale_and_fill(job.temp_path, aoi, job.nodata)
    clipped_path = job.output_path
    write_raster(clipped, clipped_path)
    job.temp_path.unlink(missing_ok=True)
    return f"Processed {job.variable}:{job.entry.name}"


def execute_jobs(jobs: Iterable[DownloadJob], config: GlobalConfig, logger, max_workers: Optional[int] = None) -> Dict[str, int]:
    job_list = list(jobs)
    if not job_list:
        logger.warning("No jobs found. Ensure you ran `prepare-lists`.")
        return {"processed": 0, "skipped": 0, "failed": 0}

    cache_dir = Path(config.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    aoi = load_aoi(Path(config.aoi_path))
    summary = {"processed": 0, "skipped": 0, "failed": 0}

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
    )

    with progress:
        overall = progress.add_task("files", total=len(job_list))

        def task_runner(job: DownloadJob):
            result = _download_one(job, config, progress)
            if result.startswith("Skipped"):
                summary["skipped"] += 1
                return result
            msg = _process_one(job, aoi, logger)
            summary["processed"] += 1
            return msg

        with ThreadPoolExecutor(max_workers=max_workers or config.max_workers) as pool:
            futures = {pool.submit(task_runner, job): job for job in job_list}
            for future in as_completed(futures):
                job = futures[future]
                try:
                    result = future.result()
                    logger.info(result)
                except Exception as exc:  # pragma: no cover
                    summary["failed"] += 1
                    logger.error("Failed %s: %s", job.entry.name, exc)
                finally:
                    progress.advance(overall)

    return summary


def prepare_present_listing(config: GlobalConfig, logger) -> List[Dict[str, object]]:
    path = config.present.prefix.strip("/")
    remote_target = f"{config.present.remote}:{path}" if path else f"{config.present.remote}:"
    logger.info("Listing present remote %s", remote_target)
    return list_remote(remote_target, recursive=True, config_path=config.rclone_config)
