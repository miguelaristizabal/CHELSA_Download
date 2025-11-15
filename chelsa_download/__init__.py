"""Top level package for the unified CHELSA download CLI."""

from importlib.metadata import version, PackageNotFoundError

__all__ = ["__version__"]


def _detect_version() -> str:
    try:
        return version("chelsa-download")
    except PackageNotFoundError:
        return "0.1.0-dev"


__version__ = _detect_version()
