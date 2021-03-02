"""
This module provides product class for NOAA NSSL Multi-Radar/Multi-Sensor
system (`MRMS`_.

.. _MRMS: https://www.nssl.noaa.gov/projects/mrms/
"""
from pansat.products.product import Product
from pathlib import Path

class MRMSPrecipRate(Product):
    """
    MRMS radar-only instantaneous precipitation rates.
    """
    def __init__(self):
        self.name = "MRMS_PrecipRate"
        self.filename_regexp = re.compile(
            "PrecipRate_00\.00_\d{8}-\d{6}.grib2\.?g?z?"
        )

    @property
    def default_destination(self):
        return "MRMS"

    def filename_to_date(self, filename):
        """
        Extract data corresponding to MRMS file.
        """
        name = Path(filename).name.split(".")[1]
        return datetime.strptime(name, "00_%Y%m%d-%H%M%S")

    def __str__(self):
        return "MRMS_PrecipRate"

    def _get_provider(self):
        """ Find a provider that provides the product. """
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProviderError(
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

        return provider.download(start_time, end_time, destination)
