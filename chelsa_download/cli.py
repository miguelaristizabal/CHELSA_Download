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

from .config import GlobalConfig
from .downloaders import (
    collect_present_jobs,
    collect_trace_jobs,
    execute_jobs,
    prepare_present_listing,
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
        help="Path to chelsa-download TOML configuration file.",
        envvar="CHELSA_DOWNLOAD_CONFIG",
    ),
    aoi: Optional[Path] = typer.Option(
        None,
        "--aoi",
        help="Path to the AOI file. Required if no config file is available.",
    ),
    quiet: bool = typer.Option(False, "--quiet", help="Only log warnings and errors."),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging."),
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
    ctx.obj = AppContext(cfg, logger, ListManager(cfg))
    logger.debug("Loaded configuration: %s", cfg.to_dict())


@app.command("prepare-lists")
def prepare_lists(
    ctx: typer.Context,
    kind: str = typer.Option(..., "--kind", "-k", help="List kind to prepare (trace or present)."),
    source_json: Optional[Path] = typer.Option(
        None,
        "--source-json",
        help="Path to cached lsjson output for TraCE21k (defaults to paths.trace_filelist_json).",
    ),
):
    """Generate text lists and metadata for later downloads."""
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
    else:
        raise typer.BadParameter(f"Unsupported kind '{kind}'. Choose 'trace' or 'present'.")


@app.command("download-trace")
def download_trace(
    ctx: typer.Context,
    variable: List[str] = typer.Option(None, "--var", "-v", help="Filter to one or more variables."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Process only the first N files."),
    force: bool = typer.Option(False, "--force", help="Re-download and overwrite outputs."),
    max_workers: Optional[int] = typer.Option(None, "--max-workers", help="Override configured worker count."),
    windowed: bool = typer.Option(
        True,
        "--windowed/--no-windowed",
        help="Use HTTP range-based reads by default (falls back to full if needed).",
    ),
    unit_normalize: bool = typer.Option(
        True,
        "--unit-normalize/--no-unit-normalize",
        help="Normalize outputs to physical units (disable only for debugging).",
    ),
):
    """Download and clip CHELSA-TraCE21k rasters."""
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
    variable: List[str] = typer.Option(None, "--var", "-v", help="Filter to variables (e.g., bio01)."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Process only the first N files."),
    force: bool = typer.Option(False, "--force", help="Re-download and overwrite outputs."),
    max_workers: Optional[int] = typer.Option(None, "--max-workers", help="Override configured worker count."),
    windowed: bool = typer.Option(
        True,
        "--windowed/--no-windowed",
        help="Use HTTP range-based reads by default (falls back to full if needed).",
    ),
    unit_normalize: bool = typer.Option(
        True,
        "--unit-normalize/--no-unit-normalize",
        help="Normalize outputs to physical units (disable only for debugging).",
    ),
):
    """Download and clip CHELSA v2.1 present-day climatology."""
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
