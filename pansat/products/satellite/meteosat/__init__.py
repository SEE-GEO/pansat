"""
==================================
pansat.products.satellite.meteosat
==================================

This module provides product classes and object for satellite products
derived from the Meteosat Second Generation (MSG) satellites.
"""
from pathlib import Path
import re
from zipfile import ZipFile
from datetime import datetime

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.exceptions import NoAvailableProvider


def _extract_file(filename):
    """
    Extracts the data file from the .zip archive downloaded from the
    provider and deletes the original archive.
    """
    path = Path(filename)
    data = path.stem + ".nat"
    with ZipFile(path) as archive:
        archive.extract(data, path=path.parent)
    path.unlink()
    return path.parent / data


class MSGSeviriL1BProduct(Product):
    """
    Base class for Meteosat Second Generation (MSG) SEVIRI L1B products.
    """

    def __init__(self, location=None):
        """
        Create MSG Seviri L1B product.

        Args:
            location: None for the 0-degree position of MSG and "IO" for the
                 the position over the Indian Ocean.

        """
        self.name = "MSG_Seviri"

        if location is not None:
            if location == "IO":
                self.name = "MSG_Seviri_IO"
            else:
                raise ValueError(
                    "'location' kwarg of MSGSeviriProduct should be None for "
                    " the 0-degree position or 'IO' for the Indian Ocean "
                    "location."
                )
        self.filename_regex = re.compile(
            "MSG\d-SEVI-MSG15-0100-NA-(\d{14})\.\d*Z-NA.nat"
        )

    @property
    def default_destination(self):
        return Path("MSG")

    def __str__(self):
        return self.name

    def filename_to_date(self, filename):
        match = self.filename_regex.match(Path(filename).name)
        if match is None:
            raise ValueError(
                f"Given filename '{filename}' does not match the expected "
                f"filename format of MSG Seviri L1B files."
            )
        time = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
        return time

    def _get_provider(self):
        """Find a provider that provides the product."""
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProvider(
                f"Could not find a provider for the" f" product {self.name}."
            )
        return available_providers[0]

    def download(self, start_time, end_time, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start
                 date of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date
                 of the of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to
                 store the output data.
        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        files = provider.download(start_time, end_time, destination)
        return [_extract_file(f) for f in files]


l1b_msg_seviri = MSGSeviriL1BProduct()
l1b_msg_seviri_io = MSGSeviriL1BProduct(location="IO")
