"""
======
pansat
======

Accessing satellite and reanalysis data made easy.
"""
import os
import logging

from pansat.download.providers.data_provider import DataProvider
from pansat.products.product import Product

# Set logging level.
_LOGGING_LEVEL = os.environ.get("PANSAT_LOG_LEVEL", "WARNING")
_LOG_FORMAT = "{name} ({levelname:10}) :: {message}"
logging.basicConfig(level=_LOGGING_LEVEL, format=_LOG_FORMAT, style="{")
