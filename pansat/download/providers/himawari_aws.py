"""
pansat.download.providers.himawari_aws
======================================

This module contains a data provider for data from currently operational
HIMAWARI series satellites, which is available from Amazon
AWS cloud storage.

Reference
---------
"""
from datetime import datetime
from pathlib import Path

import requests
import boto3
from botocore import UNSIGNED
from botocore.config import Config

from pansat.download.providers.discrete_provider import DiscreteProvider

HIMAWARI_AWS_PRODUCTS = [
    "AHI-L1b-FLDK",
]

_BUCKET_CACHE = {}


class HimawariAWSProvider(DiscreteProvider):
    """
    Dataprovider class for product available from NOAA Himarwari 8 bucket on Amazon
    AWS.
    """

    bucket_name = "noaa-himawari8"

    def __init__(self, product):
        """
        Create new NOAA Himawari provider.

        Args:
            product: The product to download.
        """
        super().__init__(product)
        self.product_name = str(product)
        self.bucket_name = HimawariAWSProvider.bucket_name
        self.client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return HIMAWARI_AWS_PRODUCTS

    def _get_keys(self, prefix):
        global _BUCKET_CACHE
        cache_id = (prefix,)

        bucket = self.bucket_name
        kwargs = {"Bucket": bucket, "Prefix": prefix}

        files = []
        if cache_id not in _BUCKET_CACHE:
            while True:
                response = self.client.list_objects_v2(**kwargs)
                if "Contents" not in response:
                    break
                files += [Path(obj["Key"]).name for obj in response["Contents"]]
                try:
                    kwargs["ContinuationToken"] = response["NextContinuationToken"]
                except KeyError:
                    break
            _BUCKET_CACHE[cache_id] = files
        return _BUCKET_CACHE[cache_id]

    def _get_request_url(self, year, month, day, hour, minute, filename):
        url = f"https://noaa-himawari8.s3.amazonaws.com/"
        url += f"{self.product_name}/{year:02}/{month:02}/{day:02}/{hour:02}{minute:02}/{filename}"
        return url

    def get_files_by_day(self, year, day):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.
            day(``int``): The Julian day for which to look up the files.

        Return:
            A list of strings containing the filename that are available
            for the given day.
        """
        date = datetime.strptime(str(year) + f"{day:03}", "%Y%j")
        prefix = f"{self.product_name}/{year}/{date.month:02}/{date.day:02}"
        files = [f for f in self._get_keys(prefix) if self.product.matches(f)]
        return files

    def download_file(self, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        t = self.product.filename_to_date(filename)
        year = t.year
        month = t.month
        day = t.day
        hour = t.hour
        minute = t.minute
        request_string = self._get_request_url(year, month, day, hour, minute, filename)
        r = requests.get(request_string)
        with open(destination, "wb") as f:
            for chunk in r:
                f.write(chunk)
