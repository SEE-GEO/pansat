"""
======
pansat
======

Accessing satellite and reanalysis data made easy.
"""
import os
import logging

# Set logging level.
_LOGGING_LEVEL = os.environ.get("PANSAT_LOG_LEVEL", "WARNING")
_LOG_FORMAT = "{name} ({levelname:10}) :: {message}"
logging.basicConfig(level=_LOGGING_LEVEL, format=_LOG_FORMAT, style="{")
