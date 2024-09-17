"""
pansat.download.providers.copernicus
====================================

This module provides the ``CopernicusProvider`` class to download data from the
`Copernicus data store <https://cds.climate.copernicus.eu/cdsapp#!/home>`_.
After creating an account, you need to install a key for the CDS API and the python library cdsapi
following these steps: https://cds.climate.copernicus.eu/api-how-to
"""
from contextlib import contextmanager
from copy import copy
from datetime import datetime, timedelta
import logging
import itertools
import os
from pathlib import Path
import tempfile
from typing import Optional, List

import numpy as np
import cdsapi

from pansat import FileRecord, TimeRange
from pansat.geometry import Geometry
from pansat.download.accounts import get_identity
from pansat.download.providers.data_provider import DataProvider


LOGGER = logging.getLogger(__name__)


DOWNLOAD_KEYS = {
    "pressure_levels": "reanalysis-era5-pressure-levels",
    "land": "reanalysis-era5-land",
    "surface": "reanalysis-era5-single-levels",
}


@contextmanager
def _create_cds_api_rc():
    """
    Context manager to create a temporary file with the Copernicus CDS login
    credentials obtained from the Pansat account manager.
    """
    _, path = tempfile.mkstemp()
    url, key = get_identity("Copernicus")
    # Write key to file.
    with open(path, "w") as file:
        file.write(f"url: {url}\n")
        file.write(f"key: {key}\n")

    os.environ["CDSAPI_RC"] = path
    try:
        yield path
    finally:
        os.environ.pop("CDSAPI_RC")
        Path(path).unlink()


class CopernicusProvider(DataProvider):
    """
    Base class for reanalysis products available from Copernicus.
    """

    def __init__(self):
        """
        Create a provider for the Copernicus CDS.
        """
        super().__init__()


    def provides(self, product) -> bool:
        return product.name.startswith("reanalysis.era5")


    def download(self, rec: FileRecord, destination: Optional[Path] = None) -> FileRecord:
        """
        Download ERA5 file.

        Args:
            rec: A ERA5 file record.
            destination: An optional destiation to which to write the file.

        Return:
            A new file record whose 'local_path' attribute points to the downloaded file.
        """
        if destination is None:
            destination = rec.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        if destination.is_dir():
            destination = destination / rec.filename

        # open new client instance
        with _create_cds_api_rc():
            client = cdsapi.Client()

            domain = rec.product.bounding_box_string
            print(domain)

            time_steps = rec.product.get_time_steps(rec)
            time_range = rec.temporal_coverage
            year = time_range.start.year
            month = time_range.start.month
            day = time_range.start.day

            filename = rec.product.get_filename(rec.temporal_coverage)

            product_type = "reanalysis"
            download_key = DOWNLOAD_KEYS[rec.product.subset]
            if rec.product.time_step == "monthly":
                download_key += "monthly-means"
                product_type = "monthly_averaged_reanalysis"


            request_dict = {
                "product_type": product_type,
                "format": "netcdf",
                "variable": rec.product.variables,
                "year": year,
                "month": month,
                "day": day,
                "time": time_steps,
            }
            if "pressure_levels" in rec.product.name:
                request_dict["pressure_level"] = [
                    "1",
                    "2",
                    "3",
                    "5",
                    "7",
                    "10",
                    "20",
                    "30",
                    "50",
                    "70",
                    "100",
                    "125",
                    "150",
                    "175",
                    "200",
                    "225",
                    "250",
                    "300",
                    "350",
                    "400",
                    "450",
                    "500",
                    "550",
                    "600",
                    "650",
                    "700",
                    "750",
                    "775",
                    "800",
                    "825",
                    "850",
                    "875",
                    "900",
                    "925",
                    "950",
                    "975",
                    "1000",
                ]
            if domain != "":
                request_dict["area"] = domain

            client.retrieve(
                download_key,
                request_dict,
                destination,
            )
            LOGGER.info("file downloaded and saved as %s", destination)

        new_rec = copy(rec)
        new_rec.local_path = destination
        return new_rec


    def find_files(self, product, time_range: TimeRange, roi: Optional[Geometry] = None):
        """
        Find ERA5 files within given time range.
        """
        recs = []
        time = time_range.start
        if product.time_step == "hourly":
            time_step = timedelta(days=1)
            time_resolution = timedelta(hours=1)
            start_hour = time.hour
            time = datetime(time.year, time.month, time.day)

            while time <= time_range.end:
                step_range = TimeRange(time + timedelta(hours=start_hour), min(time + time_step, time_range.end + time_resolution))
                recs.append(
                    FileRecord.from_remote(product, self, "", product.get_filename(step_range))
                )
                time = time + time_step
                start_hour = 0
        else:
            time = datetime(time.year, time.month)
            while time <= time_range.end:
                _, n_days = monthrange(time.year, time.month)
                step_range = TimeRange(time, time + timedelta(days=n_days))
                recs.append(
                    FileRecord.from_remote(product, self, "", product.get_filename(step_range))
                )
                time = time + timedelta(days=n_days)
        return recs


CDS = CopernicusProvider()
