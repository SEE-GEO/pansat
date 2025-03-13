"""
pansat.download.providers.himawari_aws
======================================

This module contains a data provider for data from currently operational
HIMAWARI series satellites, which is available from Amazon
AWS cloud storage.

Reference
---------
"""
from copy import copy
from datetime import datetime
from pathlib import Path
from typing import Union, Optional

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import numpy as np
import requests

from pansat import FileRecord
from pansat.time import to_datetime
from pansat.download.providers.data_provider import DataProvider

HIMAWARI_AWS_PRODUCTS = [
    "AHI-L1b-FLDK",
]


class HimawariAWSProvider(DataProvider):
    """
    Dataprovider class for product available from NOAA Himarwari 8 bucket on Amazon
    AWS.
    """
    bucket_name = "noaa-himawari{series_index}"

    def __init__(self):
        """
        Create new NOAA Himawari provider.

        Args:
            product: The product to download.
        """
        super().__init__()
        self.client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        self.cache = {}


    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return HIMAWARI_AWS_PRODUCTS

    def provides(self, product):
        return product.name.startswith("satellite.himawari")

    def _get_keys(self, product: "pansat.Product", date: Union[datetime, np.datetime64]):

        self.cache = {}
        date = to_datetime(date)
        year = date.year
        month = date.month
        day = date.day
        hour = date.hour
        minute = date.minute
        minute = 10 * (minute // 10)

        prefix = f"AHI-L1b-FLDK/{year:04}/{month:02}/{day:02}/{hour:02}{minute:02}"

        bucket = self.bucket_name.format(series_index=product.series_index)
        kwargs = {"Bucket": bucket, "Prefix": prefix}

        urls = []
        if prefix not in self.cache:
            while True:
                response = self.client.list_objects_v2(**kwargs)
                if "Contents" in response:
                    for cont in response["Contents"]:
                        urls.append(cont["Key"])

                try:
                    kwargs["ContinuationToken"] = response["NextContinuationToken"]
                except KeyError:
                    break
            self.cache[prefix] = urls

        recs = []
        for url in self.cache[prefix]:
            rec = FileRecord.from_remote(
                product, self, url, url.split("/")[-1]
            )
            recs.append(rec)
        return recs


    def find_files(self, product, time_range, roi=None):
        """
        Return list of available files for a given day of a year.

        Args:
            product: The product for which to find data files.
            time_range: The time range for which to find data files.
            roi: Not used

        Return:
            A list of strings containing the filename that are available
            for the given day.
        """
        time = time_range.start
        files = []
        while time <= time_range.end + np.timedelta64(5, "m"):
            files += [rec for rec in self._get_keys(product, time) if product.matches(rec)]
            time += np.timedelta64(10, "m")
        return files


    def download(
        self, rec: FileRecord, destination: Optional[Path] = None
    ):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        if destination is None:
            destination = rec.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        bucket = self.bucket_name.format(series_index=rec.product.series_index)
        obj = rec.remote_path

        if destination.is_dir():
            destination = destination / rec.filename

        with open(destination, "wb") as output_file:
            self.client.download_fileobj(bucket, obj, output_file)

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec



himawari_aws_provider = HimawariAWSProvider()
