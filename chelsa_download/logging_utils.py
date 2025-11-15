from __future__ import annotations

import logging
from typing import Optional

from rich.console import Console
from rich.logging import RichHandler


def setup_logging(verbose: bool = False, quiet: bool = False) -> logging.Logger:
    """Configure a Rich-powered logger."""
    level = logging.INFO
    if quiet:
        level = logging.ERROR
    elif verbose:
        level = logging.DEBUG

    console = Console(force_terminal=True)
    handler: logging.Handler = RichHandler(
        console=console,
        markup=True,
        show_time=True,
        show_path=False,
        rich_tracebacks=True,
    )
    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[handler],
    )
    logger = logging.getLogger("chelsa-download")
    logger.setLevel(level)
    return logger
