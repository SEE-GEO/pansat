"""
pansat.logging
==============

Configures logging for the pansat package.
"""
import logging
import os

from rich.console import Console
from rich.logging import RichHandler

CONSOLE = Console()

_LEVEL = "DEBUG" if "PANSAT_DEBUG" in os.environ else "INFO"

FORMAT = "%(message)s"
logging.basicConfig(
    level=_LEVEL,
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(console=CONSOLE, rich_tracebacks=True)],
    force=True
)
