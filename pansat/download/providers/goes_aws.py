"""
pansat.download.providers.goes_aws
==================================

This module contains a data provider for data from currently operational
 GOES series satellites (GOES 16 and 17), which is available from Amazon
 AWS cloud storage.

Reference
---------
"""
from pathlib import Path

import requests
import boto3
from botocore import UNSIGNED
from botocore.config import Config

from pansat.download.providers.discrete_provider import DiscreteProvider

GOES_AWS_PRODUCTS = [
    "GOES-16-ABI-L1b-RadC",
    "GOES-16-ABI-L1b-RadF",
    "GOES-16-ABI-L1b-RadM",
    "GOES-17-ABI-L1b-RadC",
    "GOES-17-ABI-L1b-RadF",
    "GOES-17-ABI-L1b-RadM",
    "GOES-18-ABI-L1b-RadC",
    "GOES-18-ABI-L1b-RadF",
    "GOES-18-ABI-L1b-RadM",
]

_BUCKET_CACHE = {}


class GOESAWSProvider(DiscreteProvider):
    """
    Dataprovider class for product available from NOAA GOES16 bucket on Amazon
    AWS.
    """

    bucket_name = "noaa-goes"

    def __init__(self, product):
        """
        Create new NOAA GOES16 provider.

        Args:
            product: The product to download.
        """
        super().__init__(product)
        self.product_name = str(product)[8:]  # Strip off GOES-XX
        self.bucket_name = GOESAWSProvider.bucket_name
        self.bucket_name += str(product.series_index)
        self.client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return GOES_AWS_PRODUCTS

    def _get_keys(self, prefix):
        global _BUCKET_CACHE
        cache_id = (prefix, self.product.series_index)

        bucket = self.bucket_name
        kwargs = {"Bucket": bucket, "Prefix": prefix}

        files = []
        if cache_id not in _BUCKET_CACHE:
            while True:
                response = self.client.list_objects_v2(**kwargs)
                if "Contents" in response:
                    files += [Path(obj["Key"]).name for obj in response["Contents"]]
                try:
                    kwargs["ContinuationToken"] = response["NextContinuationToken"]
                except KeyError:
                    break
            _BUCKET_CACHE[cache_id] = files
        return _BUCKET_CACHE[cache_id]

    def _get_request_url(self, year, day, hour, filename):
        url = f"https://noaa-goes{self.product.series_index}.s3.amazonaws.com/"
        url += f"{self.product_name}/{year}/{day}/{hour:02}/{filename}"
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
        prefix = f"{self.product_name}/{year}/{day:03}"
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
        day = t.strftime("%j")
        hour = t.hour
        request_string = self._get_request_url(year, day, hour, filename)
        r = requests.get(request_string)
        with open(destination, "wb") as f:
            for chunk in r:
                f.write(chunk)
