from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import numpy as np

from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

from .config import GlobalConfig
from .list_manager import ListFileEntry, ListManager, ListMetadata, parse_variable_from_listfilename
from .processing import clip_raster, load_aoi, write_raster
from .rclone_helper import RcloneError, copy_to, list_remote, remote_to_http_url
from .units import NORMALIZATION_VERSION, NormalizationContext, normalize_units, recompute_bio03, recompute_bio07


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


def _human_bytes(num: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} B"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TB"


def _human_speed(num: float) -> str:
    return f"{_human_bytes(num)}/s"


def _download_one(job: DownloadJob, config: GlobalConfig) -> tuple[str, int, bool]:
    if job.output_path.exists() and not job.force:
        return f"Skipped (exists): {job.output_path.name}", 0, True

    copy_to(job.remote_path, job.temp_path, config_path=config.rclone_config, retries=3)
    size_on_disk = job.temp_path.stat().st_size
    if job.entry.size and size_on_disk != job.entry.size:
        raise RcloneError(
            f"Downloaded size mismatch for {job.entry.name}: "
            f"{size_on_disk} != {job.entry.size}"
        )
    return "downloaded", size_on_disk, False


def _process_one(job: DownloadJob, aoi, logger, context: NormalizationContext) -> str:
    clipped = clip_raster(job.temp_path, aoi)
    normalized, tags = normalize_units(job.kind, job.variable, clipped, job.nodata, logger, context)
    write_raster(normalized, job.output_path, tags=tags)
    job.temp_path.unlink(missing_ok=True)
    return f"Processed {job.variable}:{job.entry.name}"


def _process_remote(
    job: DownloadJob,
    aoi,
    logger,
    config: GlobalConfig,
    context: NormalizationContext,
) -> tuple[str, int, str]:
    if job.output_path.exists() and not job.force:
        return f"Skipped (exists): {job.output_path.name}", 0, "skipped"

    url = remote_to_http_url(job.remote_path, config.rclone_config)
    if not url:
        logger.warning(
            "Windowed read unavailable for %s (no HTTP mapping). Falling back to full download.",
            job.remote_path,
        )
        status_msg, bytes_dl, skipped = _download_one(job, config)
        if skipped:
            return status_msg, bytes_dl, "skipped"
        msg = _process_one(job, aoi, logger, context)
        return msg, bytes_dl, "processed"

    try:
        clipped = clip_raster(url, aoi)
        normalized, tags = normalize_units(job.kind, job.variable, clipped, job.nodata, logger, context)
        write_raster(normalized, job.output_path, tags=tags)
        return f"Processed (windowed) {job.variable}:{job.entry.name}", 0, "processed"
    except Exception as exc:  # pragma: no cover - network/driver errors
        logger.warning(
            "Windowed read failed for %s (%s). Falling back to full download.",
            job.entry.name,
            exc,
        )
        status_msg, bytes_dl, skipped = _download_one(job, config)
        if skipped:
            return status_msg, bytes_dl, "skipped"
        msg = _process_one(job, aoi, logger, context)
        return msg, bytes_dl, "processed"


def execute_jobs(
    jobs: Iterable[DownloadJob],
    config: GlobalConfig,
    logger,
    max_workers: Optional[int] = None,
    windowed: bool = False,
    unit_normalize: bool = True,
) -> Dict[str, int]:
    job_list = list(jobs)
    if not job_list:
        logger.warning("No jobs found. Ensure you ran `prepare-lists`.")
        return {"processed": 0, "skipped": 0, "failed": 0}

    cache_dir = Path(config.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    aoi = load_aoi(Path(config.aoi_path))
    summary = {"processed": 0, "skipped": 0, "failed": 0}

    requested_vars = {job.variable.lower() for job in job_list}
    recompute_bio07_flag = {"bio05", "bio06", "bio07"}.issubset(requested_vars)
    recompute_bio03_flag = {"bio02", "bio07", "bio03"}.issubset(requested_vars)
    normalization_context = NormalizationContext(
        unit_normalize=unit_normalize,
        recompute_bio07=recompute_bio07_flag,
        recompute_bio03=recompute_bio03_flag,
    )

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TextColumn("{task.fields[status]}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    )

    with progress:
        overall = progress.add_task("total", total=len(job_list), status="starting")
        total_bytes = 0
        start_time = time.time()

        def update_overall(extra_bytes: int):
            nonlocal total_bytes
            total_bytes += max(extra_bytes, 0)
            elapsed = max(time.time() - start_time, 0.001)
            speed_val = total_bytes / elapsed
            if windowed:
                status = "windowed"
            else:
                status = f"{_human_bytes(total_bytes)} @ {_human_speed(speed_val)}"
            progress.update(
                overall,
                advance=1,
                status=status,
            )

        job_tasks: Dict[int, int] = {}
        for job in job_list:
            job_tasks[id(job)] = progress.add_task(
                f"{job.variable}:{job.entry.name}",
                total=1,
                status="queued",
            )

        def task_runner(job: DownloadJob, task_id: int):
            progress.update(task_id, status="running")
            if windowed:
                message, bytes_dl, state = _process_remote(job, aoi, logger, config, normalization_context)
                progress.update(task_id, advance=1, status=state)
                return message, bytes_dl, state
            status_msg, bytes_dl, skipped = _download_one(job, config)
            if skipped:
                progress.update(task_id, advance=1, status="skipped")
                return status_msg, bytes_dl, "skipped"
            msg = _process_one(job, aoi, logger, normalization_context)
            progress.update(task_id, advance=1, status="processed")
            return msg, bytes_dl, "processed"

        with ThreadPoolExecutor(max_workers=max_workers or config.max_workers) as pool:
            futures = {
                pool.submit(task_runner, job, job_tasks[id(job)]): job
                for job in job_list
            }
            for future in as_completed(futures):
                job = futures[future]
                try:
                    message, bytes_dl, state = future.result()
                    if state == "skipped":
                        summary["skipped"] += 1
                    elif state == "processed":
                        summary["processed"] += 1
                    logger.info(message)
                    update_overall(bytes_dl)
                except Exception as exc:  # pragma: no cover
                    summary["failed"] += 1
                    logger.error("Failed %s: %s", job.entry.name, exc)
                    task_id = job_tasks[id(job)]
                    progress.update(task_id, advance=1, status="failed")
                    update_overall(0)

    if unit_normalize:
        _recompute_derived_outputs(job_list, config, logger, recompute_bio07_flag, recompute_bio03_flag)

    return summary


def _replace_bio_var(filename: str, new_var: str) -> str:
    return re.sub(r"(bio\d{1,2})", new_var, filename, count=1, flags=re.IGNORECASE)


def _dependency_output_path(job: DownloadJob, dep_var: str, config: GlobalConfig) -> Path:
    filename = _replace_bio_var(job.entry.name, dep_var).replace(".tif", "_AOI.tif")
    if job.kind == "trace":
        out_dir = Path(config.trace.output_dir) / dep_var
    else:
        out_dir = Path(config.present.output_dir) / present_remote_subdir(dep_var)
    return out_dir / filename


def _read_output_array(path: Path):
    import rioxarray  # type: ignore

    with rioxarray.open_rasterio(path, masked=True) as rds:
        data = rds.squeeze("band", drop=True)
        return data


def _array_from_dataarray(dataarray, nodata_value: float):
    data = dataarray.data
    if np.ma.isMaskedArray(data):
        arr = np.ma.filled(data, nodata_value).astype("float32")
    else:
        arr = np.asarray(data).astype("float32")
    if np.issubdtype(arr.dtype, np.floating):
        arr = np.where(np.isnan(arr), nodata_value, arr)
    return arr


def _clear_scale_offset_attrs(dataarray) -> None:
    for key in ("scale_factor", "add_offset", "scale", "offset", "_FillValue"):
        dataarray.attrs.pop(key, None)
        if hasattr(dataarray, "encoding"):
            dataarray.encoding.pop(key, None)


def _recompute_derived_outputs(
    jobs: Iterable[DownloadJob],
    config: GlobalConfig,
    logger,
    recompute_bio07_flag: bool,
    recompute_bio03_flag: bool,
) -> None:
    jobs_by_var: Dict[str, List[DownloadJob]] = {}
    for job in jobs:
        jobs_by_var.setdefault(job.variable.lower(), []).append(job)

    if recompute_bio07_flag:
        for job in jobs_by_var.get("bio07", []):
            dep05 = _dependency_output_path(job, "bio05", config)
            dep06 = _dependency_output_path(job, "bio06", config)
            if not dep05.exists() or not dep06.exists():
                logger.warning("Skipping recompute for %s; missing bio05/bio06 outputs.", job.output_path.name)
                continue
            bio05 = _read_output_array(dep05)
            bio06 = _read_output_array(dep06)
            arr05 = _array_from_dataarray(bio05, job.nodata)
            arr06 = _array_from_dataarray(bio06, job.nodata)
            result = recompute_bio07(arr05, arr06, job.nodata, logger)
            out_da = bio05.copy(deep=True)
            out_da.data = result
            out_da.rio.write_nodata(job.nodata, inplace=True)
            _clear_scale_offset_attrs(out_da)
            tags = {
                "units": "degC",
                "quantity": "temperature_range",
                "unit_normalized": "true",
                "unit_normalization_version": NORMALIZATION_VERSION,
            }
            write_raster(out_da, job.output_path, tags=tags)
            logger.info("Recomputed bio07 from bio05/bio06: %s", job.output_path.name)

    if recompute_bio03_flag:
        for job in jobs_by_var.get("bio03", []):
            dep02 = _dependency_output_path(job, "bio02", config)
            dep07 = _dependency_output_path(job, "bio07", config)
            if not dep02.exists() or not dep07.exists():
                logger.warning("Skipping recompute for %s; missing bio02/bio07 outputs.", job.output_path.name)
                continue
            bio02 = _read_output_array(dep02)
            bio07 = _read_output_array(dep07)
            arr02 = _array_from_dataarray(bio02, job.nodata)
            arr07 = _array_from_dataarray(bio07, job.nodata)
            result = recompute_bio03(arr02, arr07, job.nodata, logger)
            out_da = bio02.copy(deep=True)
            out_da.data = result
            out_da.rio.write_nodata(job.nodata, inplace=True)
            _clear_scale_offset_attrs(out_da)
            tags = {
                "units": "%",
                "quantity": "isothermality",
                "unit_normalized": "true",
                "unit_normalization_version": NORMALIZATION_VERSION,
            }
            write_raster(out_da, job.output_path, tags=tags)
            logger.info("Recomputed bio03 from bio02/bio07: %s", job.output_path.name)


def prepare_present_listing(config: GlobalConfig, logger) -> List[Dict[str, object]]:
    path = config.present.prefix.strip("/")
    remote_target = f"{config.present.remote}:{path}" if path else f"{config.present.remote}:"
    logger.info("Listing present remote %s", remote_target)
    return list_remote(remote_target, recursive=True, config_path=config.rclone_config)
