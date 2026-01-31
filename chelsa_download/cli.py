from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import os
import typer
from rich import box
from rich.console import Console
from rich.table import Table

# --- GDAL Performance Tuning ---
# These must be set before rasterio/gdal is heavily used.
# 1. EMPTY_DIR prevents GDAL from trying to list the remote directory (slow/404s on HTTP).
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
# 2. Merge range requests to reduce the number of HTTP round-trips.
os.environ["GDAL_HTTP_MERGE_CONSECUTIVE_RANGES"] = "YES"
os.environ["GDAL_HTTP_MULTIPLEX"] = "YES"
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif,.TIF"
# 3. Increase buffer for fewer requests on high-latency connections (512KB -> 1MB)
os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "1048576"
# 4. Suppress harmless GDAL warnings about source file issues
os.environ["CPL_LOG"] = "ERROR"  # Only show errors, not warnings 

from .config import GlobalConfig
from .downloaders import (
    collect_present_jobs,
    collect_present_monthly_jobs,
    collect_trace_jobs,
    collect_trace_monthly_jobs,
    execute_jobs,
    prepare_present_listing,
    prepare_present_monthly_listing,
    prepare_trace_monthly_listing,
    DownloadJob,
)
from .list_manager import ListManager
from .logging_utils import setup_logging

app = typer.Typer(add_completion=False)


@dataclass
class AppContext:
    config: GlobalConfig
    logger: logging.Logger
    manager: ListManager


def _get_context(ctx: typer.Context) -> AppContext:
    if ctx.obj is None:
        raise typer.BadParameter("CLI context not initialized.")
    return ctx.obj


def _print_plan(context: AppContext, jobs: List[DownloadJob], windowed: bool, unit_normalize: bool):
    """Print a pretty summary of the download plan."""
    from collections import Counter
    console = Console()
    
    # Configuration Summary
    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(style="bold cyan", justify="right")
    grid.add_column()
    
    grid.add_row("AOI:", str(context.config.aoi_path))
    grid.add_row("Output Root:", str(context.config.present.output_dir.parent))
    grid.add_row("Mode:", "Windowed (AOI Clip)" if windowed else "Full Download")
    grid.add_row("Normalization:", "Enabled (Physical Units)" if unit_normalize else "Disabled (Raw Values)")
    
    console.print()
    console.print(grid)
    console.print()

    # Variable Summary
    counts = Counter(j.variable for j in jobs)
    table = Table(title=f"Download Plan ({len(jobs)} files)", box=box.SIMPLE)
    table.add_column("Variable", style="magenta")
    table.add_column("Count", justify="right", style="green")
    
    for var, count in sorted(counts.items()):
        table.add_row(var, str(count))
        
    console.print(table)
    console.print()


@app.callback()
def main(
    ctx: typer.Context,
    config: Optional[Path] = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to a TOML configuration file. If not provided, bundled defaults will be used. See chelsa-download.example.toml for reference.",
        envvar="CHELSA_DOWNLOAD_CONFIG",
    ),
    aoi: Optional[Path] = typer.Option(
        None,
        "--aoi",
        help="Path to your Area of Interest (AOI) file (GeoJSON, Shapefile, etc.). This defines the geographic region to download. Required when no config file is provided.",
    ),
    max_workers: Optional[int] = typer.Option(
        None,
        "--max-workers",
        help="Number of parallel download/processing workers. Higher values speed up downloads but increase memory usage. Default is 6 (or value from config).",
    ),
    quiet: bool = typer.Option(False, "--quiet", help="Suppress informational messages. Only warnings and errors will be shown."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable detailed debug logging for troubleshooting."),
):
    logger = setup_logging(verbose=verbose, quiet=quiet)
    cfg: GlobalConfig
    try:
        cfg = GlobalConfig.load(config)
        if aoi:
            cfg.aoi_path = aoi.expanduser().resolve()
    except FileNotFoundError:
        chosen_aoi = aoi
        if chosen_aoi is None:
            prompt_value = typer.prompt(
                "Config file not found. Enter the path to your AOI to use built-in defaults"
            ).strip()
            if not prompt_value:
                raise typer.BadParameter("AOI path is required the first time you run the CLI without a config.")
            chosen_aoi = Path(prompt_value)
        cfg = GlobalConfig.default(chosen_aoi.expanduser().resolve())
        logger.info("Using bundled defaults (lists: %s, outputs: %s)", cfg.lists_dir, cfg.present.output_dir.parent)
    
    # Override max_workers if provided
    if max_workers is not None:
        cfg.max_workers = max_workers
        logger.debug("Overriding max_workers to %d", max_workers)
    
    ctx.obj = AppContext(cfg, logger, ListManager(cfg))
    logger.debug("Loaded configuration: %s", cfg.to_dict())


@app.command("prepare-lists")
def prepare_lists(
    ctx: typer.Context,
    kind: str = typer.Option(..., "--kind", "-k", help="Type of file lists to generate: 'trace' for TraCE21k paleoclimate bioclim data, 'present' for modern (1981-2010) bioclim climatology, 'present_monthly' for modern monthly data (pr/tasmin/tasmax), or 'trace_monthly' for TraCE21k monthly centennial data."),
    source_json: Optional[Path] = typer.Option(
        None,
        "--source-json",
        help="Path to a pre-cached rclone lsjson output file for TraCE21k data. Only used with --kind trace. If not provided, uses the path from your config file.",
    ),
):
    """Generate file lists and metadata needed for downloads. Run this once before downloading if not using bundled lists."""
    context = _get_context(ctx)
    manager = context.manager
    cfg = context.config
    kind_lower = kind.lower()

    if kind_lower == "trace":
        source = source_json or cfg.trace_filelist_json
        if not source:
            raise typer.BadParameter("No source JSON provided for TraCE lists.")
        output_dir = Path(cfg.lists_dir)
        if cfg.trace.lists_subdir:
            output_dir = output_dir / cfg.trace.lists_subdir
        context.logger.info("Building TraCE21k lists from %s", source)
        created = manager.build_trace_lists(Path(source), output_dir.resolve())
        context.logger.info("Wrote %d lists to %s", len(created), output_dir)
    elif kind_lower == "present":
        output_dir = Path(cfg.lists_dir)
        if cfg.present.lists_subdir:
            output_dir = output_dir / cfg.present.lists_subdir
        records = prepare_present_listing(cfg, context.logger)
        created = manager.build_present_lists(records, output_dir.resolve())
        context.logger.info("Wrote %d present-day lists to %s", len(created), output_dir)
    elif kind_lower == "present_monthly":
        output_dir = Path(cfg.lists_dir)
        if cfg.present_monthly.lists_subdir:
            output_dir = output_dir / cfg.present_monthly.lists_subdir
        records = prepare_present_monthly_listing(cfg, context.logger)
        created = manager.build_present_monthly_lists(records, output_dir.resolve())
        context.logger.info("Wrote %d present monthly lists to %s", len(created), output_dir)
    elif kind_lower == "trace_monthly":
        output_dir = Path(cfg.lists_dir)
        if cfg.trace_monthly.lists_subdir:
            output_dir = output_dir / cfg.trace_monthly.lists_subdir
        records = prepare_trace_monthly_listing(cfg, context.logger)
        created = manager.build_trace_monthly_lists(records, output_dir.resolve())
        context.logger.info("Wrote %d trace monthly lists to %s", len(created), output_dir)
    else:
        raise typer.BadParameter(f"Unsupported kind '{kind}'. Choose 'trace', 'present', 'present_monthly', or 'trace_monthly'.")


@app.command("download-trace")
def download_trace(
    ctx: typer.Context,
    variable: List[str] = typer.Option(None, "--var", "-v", help="Filter downloads to specific variables (e.g., --var bio01 --var bio12). Can be specified multiple times. If omitted, all available variables are downloaded."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit processing to the first N files (useful for testing). If not specified, all matching files will be processed."),
    force: bool = typer.Option(False, "--force", help="Force re-download and overwrite existing output files, even if they already exist."),
    max_workers: Optional[int] = typer.Option(None, "--max-workers", help="Number of parallel workers for this specific command. Overrides global --max-workers and config file settings."),
    windowed: bool = typer.Option(
        True,
        "--windowed/--no-windowed",
        help="Use windowed (HTTP range-based) reads to only download pixels within your AOI. Faster and saves bandwidth. Use --no-windowed to download full files.",
    ),
    unit_normalize: bool = typer.Option(
        True,
        "--unit-normalize/--no-unit-normalize",
        help="Convert output values to physical units (°C, mm, etc.) by applying scale/offset. Use --no-unit-normalize only for debugging raw GeoTIFF values.",
    ),
):
    """Download and clip CHELSA-TraCE21k paleoclimate rasters for your AOI."""
    context = _get_context(ctx)
    jobs = collect_trace_jobs(context.config, context.manager, vars_filter=variable or None, limit=limit, force=force)
    _print_plan(context, jobs, windowed, unit_normalize)
    summary = execute_jobs(
        jobs,
        context.config,
        context.logger,
        max_workers=max_workers,
        windowed=windowed,
        unit_normalize=unit_normalize,
    )
    context.logger.info("Trace download summary: %s", summary)


@app.command("download-present")
def download_present(
    ctx: typer.Context,
    variable: List[str] = typer.Option(None, "--var", "-v", help="Filter downloads to specific bioclim variables (e.g., --var bio01 --var bio12). Can be specified multiple times. If omitted, all available variables are downloaded."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit processing to the first N files (useful for testing). If not specified, all matching files will be processed."),
    force: bool = typer.Option(False, "--force", help="Force re-download and overwrite existing output files, even if they already exist."),
    max_workers: Optional[int] = typer.Option(None, "--max-workers", help="Number of parallel workers for this specific command. Overrides global --max-workers and config file settings."),
    windowed: bool = typer.Option(
        True,
        "--windowed/--no-windowed",
        help="Use windowed (HTTP range-based) reads to only download pixels within your AOI. Faster and saves bandwidth. Use --no-windowed to download full files.",
    ),
    unit_normalize: bool = typer.Option(
        True,
        "--unit-normalize/--no-unit-normalize",
        help="Convert output values to physical units (°C, mm, etc.) by applying scale/offset. Use --no-unit-normalize only for debugging raw GeoTIFF values.",
    ),
):
    """Download and clip CHELSA v2.1 present-day climatology (1981-2010) for your AOI."""
    context = _get_context(ctx)
    jobs = collect_present_jobs(context.config, context.manager, vars_filter=variable or None, limit=limit, force=force)
    _print_plan(context, jobs, windowed, unit_normalize)
    summary = execute_jobs(
        jobs,
        context.config,
        context.logger,
        max_workers=max_workers,
        windowed=windowed,
        unit_normalize=unit_normalize,
    )
    context.logger.info("Present download summary: %s", summary)


@app.command("download-present-monthly")
def download_present_monthly(
    ctx: typer.Context,
    variable: List[str] = typer.Option(None, "--var", "-v", help="Filter downloads to specific monthly variables (e.g., --var pr --var tasmin). Valid options: pr, tasmin, tasmax. Can be specified multiple times. If omitted, all available variables are downloaded."),
    month: List[int] = typer.Option(None, "--month", "-m", help="Filter downloads to specific months (1-12). Can be specified multiple times (e.g., --month 1 --month 7). If omitted along with --month-range, all 12 months are downloaded."),
    month_range: Optional[str] = typer.Option(None, "--month-range", help="Filter downloads to a range of months (format: start-end, e.g., --month-range 1-6 for January through June). Inclusive range."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit processing to the first N files (useful for testing). If not specified, all matching files will be processed."),
    force: bool = typer.Option(False, "--force", help="Force re-download and overwrite existing output files, even if they already exist."),
    max_workers: Optional[int] = typer.Option(None, "--max-workers", help="Number of parallel workers for this specific command. Overrides global --max-workers and config file settings."),
    windowed: bool = typer.Option(
        True,
        "--windowed/--no-windowed",
        help="Use windowed (HTTP range-based) reads to only download pixels within your AOI. Faster and saves bandwidth. Use --no-windowed to download full files.",
    ),
    unit_normalize: bool = typer.Option(
        True,
        "--unit-normalize/--no-unit-normalize",
        help="Convert output values to physical units (°C for temperature, mm for precipitation) by applying transformations. Use --no-unit-normalize only for debugging raw GeoTIFF values.",
    ),
):
    """Download and clip CHELSA v2.1 present-day monthly climatologies (1981-2010) for your AOI.
    
    Available variables: pr (precipitation), tasmin (minimum temperature), tasmax (maximum temperature).
    Each variable includes 12 monthly values (January through December).
    """
    context = _get_context(ctx)
    month_range_tuple = None
    if month_range:
        try:
            start, end = map(int, month_range.split("-"))
            month_range_tuple = (start, end)
        except ValueError:
            context.logger.error("Invalid --month-range format. Use: start-end (e.g., 1-6)")
            raise typer.Exit(code=1)
    
    jobs = collect_present_monthly_jobs(
        context.config,
        context.manager,
        vars_filter=variable or None,
        months=month or None,
        month_range=month_range_tuple,
        limit=limit,
        force=force,
    )
    _print_plan(context, jobs, windowed, unit_normalize)
    summary = execute_jobs(
        jobs,
        context.config,
        context.logger,
        max_workers=max_workers,
        windowed=windowed,
        unit_normalize=unit_normalize,
    )
    context.logger.info("Present monthly download summary: %s", summary)


@app.command("download-trace-monthly")
def download_trace_monthly(
    ctx: typer.Context,
    variable: List[str] = typer.Option(None, "--var", "-v", help="Filter downloads to specific monthly variables (e.g., --var pr --var tasmin). Valid options: pr, tasmin, tasmax. Can be specified multiple times. If omitted, all available variables are downloaded."),
    time_slice: List[int] = typer.Option(None, "--time-slice", "-t", help="Filter downloads to specific time slices in years BP (e.g., --time-slice -200 --time-slice 0). Range: -200 to 20. Can be specified multiple times. If omitted along with --time-range, all available time slices are downloaded."),
    time_range: Optional[str] = typer.Option(None, "--time-range", help="Filter downloads to a range of time slices (format: start-end, e.g., --time-range -200--100). Inclusive range in years BP."),
    time_interval: Optional[int] = typer.Option(None, "--time-interval", help="Step size when using --time-range (e.g., --time-range -200-0 --time-interval 10 downloads every 10th time slice). Must be positive."),
    month: List[int] = typer.Option(None, "--month", "-m", help="Filter downloads to specific months (1-12). Can be specified multiple times (e.g., --month 1 --month 7). If omitted along with --month-range, all 12 months are downloaded."),
    month_range: Optional[str] = typer.Option(None, "--month-range", help="Filter downloads to a range of months (format: start-end, e.g., --month-range 1-6 for January through June). Inclusive range."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Limit processing to the first N files (useful for testing). If not specified, all matching files will be processed."),
    force: bool = typer.Option(False, "--force", help="Force re-download and overwrite existing output files, even if they already exist."),
    max_workers: Optional[int] = typer.Option(None, "--max-workers", help="Number of parallel workers for this specific command. Overrides global --max-workers and config file settings."),
    windowed: bool = typer.Option(
        True,
        "--windowed/--no-windowed",
        help="Use windowed (HTTP range-based) reads to only download pixels within your AOI. Faster and saves bandwidth. Use --no-windowed to download full files.",
    ),
    unit_normalize: bool = typer.Option(
        True,
        "--unit-normalize/--no-unit-normalize",
        help="Convert output values to physical units (°C for temperature, mm for precipitation) by applying transformations. Use --no-unit-normalize only for debugging raw GeoTIFF values.",
    ),
):
    """Download and clip CHELSA TraCE21k monthly centennial data for your AOI.
    
    Available variables: pr (precipitation), tasmin (minimum temperature), tasmax (maximum temperature).
    Time coverage: -200 to 20 years BP (Before Present, where 0 = 1950 CE) in centennial steps.
    Each variable includes 12 monthly values (January through December) for each time slice.
    """
    context = _get_context(ctx)
    time_range_tuple = None
    if time_range:
        try:
            start, end = map(int, time_range.split("-"))
            time_range_tuple = (start, end)
        except ValueError:
            context.logger.error("Invalid --time-range format. Use: start-end (e.g., -200--100)")
            raise typer.Exit(code=1)
    
    month_range_tuple = None
    if month_range:
        try:
            start, end = map(int, month_range.split("-"))
            month_range_tuple = (start, end)
        except ValueError:
            context.logger.error("Invalid --month-range format. Use: start-end (e.g., 1-6)")
            raise typer.Exit(code=1)
    
    jobs = collect_trace_monthly_jobs(
        context.config,
        context.manager,
        vars_filter=variable or None,
        time_slice=time_slice or None,
        time_range=time_range_tuple,
        time_interval=time_interval,
        months=month or None,
        month_range=month_range_tuple,
        limit=limit,
        force=force,
    )
    _print_plan(context, jobs, windowed, unit_normalize)
    summary = execute_jobs(
        jobs,
        context.config,
        context.logger,
        max_workers=max_workers,
        windowed=windowed,
        unit_normalize=unit_normalize,
    )
    context.logger.info("Trace monthly download summary: %s", summary)
