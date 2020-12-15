"""
pansat.download.providers.goes_aws
==================================

This module contain a data provider for the GOES 16 data available from Amazon
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
    "ABI-L1b-RadC",
    "ABI-L1b-RadF",
    "ABI-L1b-RadM",
]

class GOESAWSProvider(DiscreteProvider):
    """
    Dataprovider class for product available from NOAA GOES16 bucket on Amazon
    AWS.
    """
    bucket_name = 'noaa-goes16'

    def __init__(self, product):
        """
        Create new NOAA GOES16 provider.

        Args:
            product: The product to download.
        """
        super().__init__(product)
        self.product_name = str(product)
        self.client = boto3.client('s3',
                                   config=Config(signature_version=UNSIGNED))

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

        bucket = GOESAWSProvider.bucket_name
        kwargs = {"Bucket": bucket,
                  "Prefix": prefix}

        while True:
            response = self.client.list_objects_v2(**kwargs)
            for obj in response['Contents']:
                key = obj['Key']
                if key.startswith(prefix):
                    filename = Path(key).name
                    if self.product.matches(filename):
                        yield filename

            try:
                kwargs['ContinuationToken'] = response['NextContinuationToken']
            except KeyError:
                break

    def _get_request_url(self, year, day, hour, filename):
        url = f"https://noaa-goes16.s3.amazonaws.com/{self.product_name}/"
        url += f"{year}/{day}/{hour:02}/{filename}"
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
        files = list(self._get_keys(prefix))
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
