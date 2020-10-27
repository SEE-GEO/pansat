"""
pansat.download.providers.copernicus
====================================

This module provides the ``CopernicusProvider`` class to download data from the
Copernicus data store.
"""
import itertools
import os
import cdsapi
import numpy as np
from pansat.download.providers.data_provider import DataProvider

COPERNICUS_PRODUCTS = [
    "reanalysis-era5-land",
    "reanalysis-era5-land-monthly-means",
    "reanalysis-era5-pressure-levels",
    "reanalysis-era5-pressure-levels-monthly-means",
    "reanalysis-era5-single-levels",
    "reanalysis-era5-single-levels-monthly-means",
]


class CopernicusProvider(DataProvider):
    """
    Base class for reanalysis products available from Copernicus.
    """

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

        product(str): product name, available products are land, single-level,
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

    def download(self, start, end, destination=None):
        """Downloads files for given time range and stores at specified location.
        Hourly data products are saved per hour and monthly data products are
        saved per month. Note that you have to install the CDS API key before
        download is possible: https://cds.climate.copernicus.eu/api-how-to

        Args:

            start(datetime.datetime): start date and time (year, month, day,
                hour), if hour is not specified for hourly dataproduct, all
                hours are downloaded for each date.
            end(datetime.datetime): end date and time (year, month, day, hour),
                if hour is not specified for hourly dataproduct, all hours are
                downloaded for each date.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """

        # open new client instance

        c = cdsapi.Client()

        # subset region, if requested
        if self.product.domain == None:
            area = ""
        else:
            area = "/".join(self.product.domain)

        ################### create time range for monthly data products ########
        if "monthly" in self.product.name:
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
        else:
            ############### create time range for hourly data products #########

            # get list with all years, months, days, hours between the two dates
            delta = end - start
            hour = delta / 3600
            dates = []
            for i in range(hour.seconds + 1):
                h = start + timedelta(hours=i)
                dates.append(h)

        # container to save list of downloaded files
        files = []

        # send API request for each specific month or hour
        for idx, date in enumerate(dates):
            if "monthly" in self.product.name:
                # define download parameters for monthly download
                month = date
                year = years[idx]
                day = ""
                hour = "00:00"
                download_key = "monthly_averaged_reanalysis"

            else:
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

            # zero padding for month
            if int(month) < 10:
                month = "0" + str(month)

            filename = (
                self.product.name
                + "_"
                + year
                + month
                + day
                + hour
                + "_"
                + "-".join(self.product.variables)
                + "-".join(area)
                + ".nc"
            )

            # set output path and file name
            out = str(destination) + "/" + str(filename)

            # only download if file not already already exists
            if os.path.exists(out):
                print(destination, " already exists.")

            else:
                c.retrieve(
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
                print("file downloaded and saved as", out)

                files.append(out)

            return files
