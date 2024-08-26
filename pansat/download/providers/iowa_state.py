"""
====================================
pansat.download.providers.iowa_state
====================================

This module provides a data provider class to download MRMS products from an
archive located at https://mtarchive.geol.iastate.edu/, which is hosted by the
Department of Geological and atmospheric sciences at Iowa State University.

This provider doesn't require any user authentication to download data.
"""
from copy import copy
from datetime import datetime, timedelta
from pathlib import Path
import shutil
from typing import Optional

import requests

from pansat.time import to_datetime, Time
from pansat import cache
from pansat.geometry import Geometry
from pansat.file_record import FileRecord
from pansat.download.providers.discrete_provider import DiscreteProviderDay

PRODUCTS = {
    "ground_based.mrms.precip_rate": ["mrms", "ncep", "PrecipRate"],
    "ground_based.mrms.radar_quality_index": ["mrms", "ncep", "RadarQualityIndex"],
    "ground_based.mrms.precip_flag": ["mrms", "ncep", "PrecipFlag"],
    "ground_based.mrms.precip_1h": ["mrms", "ncep", "RadarOnly_QPE_01H"],
    "ground_based.mrms.precip_24h": ["mrms", "ncep", "RadarOnly_QPE_24H"],
    "ground_based.mrms.precip_1h_gc": ["mrms", "ncep", "GaugeCorr_QPE_01H"],
    "ground_based.mrms.precip_1h_ms": ["mrms", "ncep", "MultiSensor_QPE_01H_Pass2"],
    "ground_based.mrms.precip_24h_ms": ["mrms", "ncep", "MultiSensor_QPE_24H_Pass2"],
}


class IowaStateProvider(DiscreteProviderDay):
    """
    Base class for data products available from htps://mtarchive.geol.iastate.edu/
    """

    base_url = "https://mtarchive.geol.iastate.edu/"

    def __init__(self):
        """
        Create a new product instance.

        Args:

            product(``Product``): Product class object available from the archive.

        """
        super().__init__()

    @classmethod
    def get_available_products(cls):
        return PRODUCTS.keys()

    def get_url(self, product, date):
        suffix = "/".join(PRODUCTS[product.name])
        url = f"{self.base_url}/{date.year}/{date.month:02}/{date.day:02}/"
        url += suffix
        return url

    def provides(self, product):
        return product.name in PRODUCTS.keys()

    def find_files_by_day(
        self, product: "pansat.Product", time: Time, roi: Optional[Geometry] = None
    ) -> [FileRecord]:
        """
        Return all files from given year and julian day.

        Args:
            year(``int``): The year from which to retrieve the filenames.
            day(``int``): Day of the year of the data from which to retrieve the
                the filenames.

        Return:
            List of the filenames of this product on the given day.
        """
        url = self.get_url(product, to_datetime(time))
        session = cache.get_session()
        with session.get(url) as resp:
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            files_unique = set(product.filename_regexp.findall(resp.text))
            filenames = sorted(list(files_unique))
            return [
                FileRecord.from_remote(product, self, url + "/" + fname, fname)
                for fname in filenames
            ]

    def download(
        self, rec: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download file from data provider.

        Args:
            product:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        if destination is None:
            destination = rec.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        url = rec.remote_path
        destination = Path(destination)
        if destination.is_dir():
            destination = destination / rec.filename

        with requests.get(url, stream=True) as resp:
            resp.raise_for_status()
            with open(destination, "wb") as output:
                shutil.copyfileobj(resp.raw, output)

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec


iowa_state_provider = IowaStateProvider()
