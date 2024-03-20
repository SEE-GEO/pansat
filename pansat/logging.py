import logging
from rich.console import Console
from rich.logging import RichHandler

CONSOLE = Console()

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO",
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler(console=CONSOLE, rich_tracebacks=True)],
    force=True
)
