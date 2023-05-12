"""
pansat.products.satellite.gridsat
=================================

This module defines the NOAA GridSat products.
"""
from datetime import datetime
from pathlib import Path

from pansat.download import providers
from pansat.exceptions import NoAvailableProvider


class GridsatProduct:
    """
    Class for NOAA GridSat GOES and CONUS products.
    """

    def __init__(self, variant):
        """
        Args:
            variant: The variant of the GridSat product: 'conus' or 'goes'.
        """
        self.variant = variant

    def filename_to_date(self, filename):
        """
        Args:
            filename: The name of GridSat file.
        Return:
            ``datetime.datetime`` object of the corresponding
            time.
        """
        parts = filename.split(".")
        year, month, day, hour_min = parts[2:6]
        return datetime(
            int(year), int(month), int(day), int(hour_min[:2]), int(hour_min[2:])
        )

    @property
    def name(self):
        """The product name."""
        return f"gridsat_{self.variant}"

    @property
    def default_destination(self):
        """Default destination for downloads."""
        return "grid_sat"

    def open(self, filename):
        """Open given file as ``xarray.Dataset``."""
        return xr.open_dataset(self.filename)

    def _get_provider(self):
        """Find a provider that provides the product."""
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if self.name in p.get_available_products()
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
            destination = Path(self.default_destination)
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        files = provider.download(start_time, end_time, destination)
        return files


class GridsatB1(GridsatProduct):
    """
    Specialized class for the GridSat CDR.
    """

    def __init__(self):
        """Create product."""
        super().__init__("b1")

    def filename_to_date(self, filename):
        """
        Args:
            filename: The name of GridSat file.
        Return:
            ``datetime.datetime`` object of the corresponding
            time.
        """
        parts = filename.split(".")
        year, month, day, hour = parts[1:5]
        return datetime(int(year), int(month), int(day), int(hour))


gridsat_goes = GridsatProduct("goes")
gridsat_conus = GridsatProduct(("conus"))
gridsat_b1 = GridsatB1()
