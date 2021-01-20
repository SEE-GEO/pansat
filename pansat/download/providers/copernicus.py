"""
pansat.download.providers.copernicus
====================================

This module provides the ``CopernicusProvider`` class to download data from the
`Copernicus data store <https://cds.climate.copernicus.eu/cdsapp#!/home>`_.
"""

import logging
from contextlib import contextmanager
import itertools
import os
from pathlib import Path
import tempfile
from datetime import timedelta
import numpy as np
import cdsapi
from pansat.download.accounts import get_identity
from pansat.download.providers.data_provider import DataProvider


COPERNICUS_PRODUCTS = [
    "reanalysis-era5-land",
    "reanalysis-era5-land-monthly-means",
    "reanalysis-era5-pressure-levels",
    "reanalysis-era5-pressure-levels-monthly-means",
    "reanalysis-era5-single-levels",
    "reanalysis-era5-single-levels-monthly-means",
]

LOGGER = logging.getLogger(__name__)


@contextmanager
def _create_cds_api_rc():
    """
    Context manager to create a temporary file with the Copernicus CDS login
    credentials obtained from the Pansat account manager.
    """
    _, path = tempfile.mkstemp()
    url, key = get_identity("Copernicus")
    # Write key to file.
    file = open(path, "w")
    file.write(f"url: {url}\n")
    file.write(f"key: {key}\n")
    file.close()

    LOGGER.info("Creating file: %s", open(path).read())
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

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

            product(``str``): product name, available products are land, single-level,
                pressure-level for hourly and monthly resolution
        """
        super().__init__()
        self.product = product

        if not product.name in COPERNICUS_PRODUCTS:
            available_products = COPERNICUS_PRODUCTS
            raise ValueError(
                f"{product.name} not a available from the Copernicus data"
                " provider. Currently available products are: "
                f" {available_products}."
            )

    @classmethod
    def get_available_products(cls):
        """
        The products available from this dataprovider.
        """
        return COPERNICUS_PRODUCTS

    @classmethod
    def get_timesteps_monthly(cls, start, end):
        """
        Create a time range with all dates between the start and end date.

        Args:

            start(``datetime.datetime``): datetime.datetime object for start time
            end(``datetime.datetime``): datetime.datetime object for end time

        Returns:
            dates(``list``): list with months for each year
            years(``list``): list with all years

        """
        # handling data ranges over multiple years:
        if start.year != end.year:
            # get years with complete nr. of months
            full_years_range = range(start.year + 1, end.year)
            full_years = list(
                itertools.chain.from_iterable(
                    itertools.repeat(x, 12) for x in full_years_range
                )
            )
            all_months = np.arange(1, 13).astype(str)

            # get months of incomplete years
            months_first_year = list(np.arange((start.month + 1), 13).astype(str))
            months_last_year = list(np.arange(1, (end.month + 1)).astype(str))

            # create lists for years with months
            years = (
                [str(start.year)] * len(months_first_year)
                + [str(f) for f in full_years]
                + [str(end.year)] * len(months_last_year)
            )
            dates = (
                months_first_year
                + [str(m) for m in all_months] * len(full_years_range)
                + months_last_year
            )
        else:
            # getting all month for the specified year
            dates = np.arange(start.month, end.month + 1).astype(str)
            nr_of_months = np.shape(dates)[0]
            years = [str(start.year)] * nr_of_months

        return dates, years

    @classmethod
    def get_timesteps_hourly(cls, start, end):
        """
        Create a time range with all dates between the start and end date.

        Args:

            start(``datetime.datetime``): datetime.datetime object for start time
            end(``datetime.datetime``): datetime.datetime object for end time

        Returns:
            dates(``list``): list with all hours, days, months and years between two dates
        """

        delta = end - start
        dhour = delta / 3600
        dates = []
        for hour in range(dhour.seconds + 1):
            hourdelta = start + timedelta(hours=hour)
            dates.append(hourdelta)

        return dates

    def download_monthly(self, start, end, destination):
        """Downloads monthly files for given time range and stores at specified location.
        Hourly data products are saved per hour and monthly data products are
        saved per month. Note that you have to install the CDS API key before
        download is possible: https://cds.climate.copernicus.eu/api-how-to

        Args:

            start(``datetime.datetime``): start date and time (year, month, day,
                hour), if hour is not specified for hourly dataproduct, all
                hours are downloaded for each date.
            end(``datetime.datetime``): end date and time (year, month, day, hour),
                if hour is not specified for hourly dataproduct, all hours are
                downloaded for each date.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """

        # open new client instance

        with _create_cds_api_rc():
            client = cdsapi.Client()

            # subset region, if requested
            if self.product.domain == "global":
                area = ""
                domain = ""
            else:
                # change to requested order of coordinate values
                dom = np.array(
                    [
                        self.product.domain[1],
                        self.product.domain[2],
                        self.product.domain[0],
                        self.product.domain[3],
                    ]
                ).astype(str)
                area = "/".join(dom)
                domain = "-".join(np.array(self.product.domain).astype(str))

            dates, years = self.get_timesteps_monthly(start, end)
            # container to save list of downloaded files
            files = []

            # send API request for each specific month in time range
            for idx, date in enumerate(dates):
                # define download parameters for monthly download
                month = date
                year = years[idx]
                hour = "00:00"

                # zero padding for month
                if int(month) < 10:
                    month = "0" + str(month)

                filename = (
                    self.product.name
                    + "_"
                    + year
                    + month
                    + "_"
                    + "-".join(self.product.variables)
                    + domain
                    + ".nc"
                )

                # set output path and file name
                out = Path(str(destination)) / str(filename)

                # only download if file not already already exists
                if os.path.exists(out):
                    LOGGER.info(destination, "%s already exists.")
                    files.append(out)
                else:
                    if "pressure" in self.product.name:
                        client.retrieve(
                            self.product.name,
                            {
                                "product_type": "monthly_averaged_reanalysis",
                                "format": "netcdf",
                                "area": area,
                                "variable": self.product.variables,
                                "pressure_level": [
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
                                ],
                                "year": year,
                                "month": month,
                                "time": hour,
                            },
                            out,
                        )

                    else:
                        client.retrieve(
                            self.product.name,
                            {
                                "product_type": "monthly_averaged_reanalysis",
                                "format": "netcdf",
                                "area": area,
                                "variable": self.product.variables,
                                "year": year,
                                "month": month,
                                "time": hour,
                            },
                            out,
                        )

                    LOGGER.info("file downloaded and saved as %s", out)
                    files.append(out)

            return files

    def download_hourly(self, start, end, destination):
        """Downloads hourly files for given time range and stores at specified location.
        Hourly data products are saved per hour and monthly data products are
        saved per month. Note that you have to install the CDS API key before
        download is possible: https://cds.climate.copernicus.eu/api-how-to

        Args:

            start(``datetime.datetime``): start date and time (year, month, day,
                hour), if hour is not specified for hourly dataproduct, all
                hours are downloaded for each date.
            end(``datetime.datetime``): end date and time (year, month, day, hour),
                if hour is not specified for hourly dataproduct, all hours are
                downloaded for each date.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """

        # open new client instance

        with _create_cds_api_rc():
            client = cdsapi.Client()

            # subset region, if requested
            if self.product.domain == "global":
                area = ""
                domain = ""
            else:
                # change to requested order of coordinate values
                dom = np.array(
                    [
                        self.product.domain[1],
                        self.product.domain[2],
                        self.product.domain[0],
                        self.product.domain[3],
                    ]
                ).astype(str)
                area = "/".join(dom)
                domain = "-".join(np.array(self.product.domain).astype(str))

            dates = self.get_timesteps_hourly(start, end)

            # container to save list of downloaded files
            files = []

            # send API request for each specific hour in time range
            for idx in np.arange(len(dates)):
                # define download parameters for hourly download
                year = str(dates[idx].year)
                month = str(dates[idx].month)
                day = str(dates[idx].day)
                hour = str(dates[idx].hour)

                # get download key
                download_key = "reanalysis"
                if "land" in self.product.name:
                    download_key = ""

                # zero padding for day
                if int(day) < 10:
                    day = "0" + str(day)

                # zero padding for hour string in filename
                hourstr = str(hour)
                if int(hour) < 10:
                    hourstr = "0" + str(hour)

                # zero padding for month
                if int(month) < 10:
                    month = "0" + str(month)

                filename = (
                    self.product.name
                    + "_"
                    + year
                    + month
                    + day
                    + hourstr
                    + "_"
                    + "-".join(self.product.variables)
                    + domain
                    + ".nc"
                )

                # set output path and file name
                out = Path(str(destination)) / str(filename)

                # only download if file not already already exists
                if os.path.exists(out):
                    LOGGER.info(out, "%s already exists.")
                    files.append(out)
                else:
                    if "pressure" in self.product.name:
                        client.retrieve(
                            self.product.name,
                            {
                                "product_type": download_key,
                                "format": "netcdf",
                                "area": area,
                                "variable": self.product.variables,
                                "pressure_level": [
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
                                ],
                                "year": year,
                                "month": month,
                                "day": day,
                                "time": hour,
                            },
                            out,
                        )

                    else:
                        client.retrieve(
                            self.product.name,
                            {
                                "product_type": download_key,
                                "format": "netcdf",
                                "area": area,
                                "variable": self.product.variables,
                                "year": year,
                                "month": month,
                                "day": day,
                                "time": hour,
                            },
                            out,
                        )

                    LOGGER.info("file downloaded and saved as %s", out)
                    files.append(out)

            return files

    def download(self, start, end, destination):
        """Downloads files dependent on desired temporal resolution of data product.

        Args:

            start(``datetime.datetime``): start date and time (year, month, day,
                hour), if hour is not specified for hourly dataproduct, all
                hours are downloaded for each date.
            end(``datetime.datetime``): end date and time (year, month, day, hour),
                if hour is not specified for hourly dataproduct, all hours are
                downloaded for each date.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.

        """

        if "monthly" in self.product.name:
            downloaded = self.download_monthly(start, end, destination)
        else:
            downloaded = self.download_hourly(start, end, destination)

        return downloaded
