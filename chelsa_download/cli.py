from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import typer

from .config import GlobalConfig
from .downloaders import (
    collect_present_jobs,
    collect_trace_jobs,
    execute_jobs,
    prepare_present_listing,
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
    quiet: bool = typer.Option(False, "--quiet", help="Only log warnings and errors."),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging."),
):
    logger = setup_logging(verbose=verbose, quiet=quiet)
    cfg = GlobalConfig.load(config)
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
):
    """Download and clip CHELSA-TraCE21k rasters."""
    context = _get_context(ctx)
    jobs = collect_trace_jobs(context.config, context.manager, vars_filter=variable or None, limit=limit, force=force)
    summary = execute_jobs(jobs, context.config, context.logger, max_workers=max_workers)
    context.logger.info("Trace download summary: %s", summary)


@app.command("download-present")
def download_present(
    ctx: typer.Context,
    variable: List[str] = typer.Option(None, "--var", "-v", help="Filter to variables (e.g., bio01)."),
    limit: Optional[int] = typer.Option(None, "--limit", help="Process only the first N files."),
    force: bool = typer.Option(False, "--force", help="Re-download and overwrite outputs."),
    max_workers: Optional[int] = typer.Option(None, "--max-workers", help="Override configured worker count."),
):
    """Download and clip CHELSA v2.1 present-day climatology."""
    context = _get_context(ctx)
    jobs = collect_present_jobs(context.config, context.manager, vars_filter=variable or None, limit=limit, force=force)
    summary = execute_jobs(jobs, context.config, context.logger, max_workers=max_workers)
    context.logger.info("Present download summary: %s", summary)
