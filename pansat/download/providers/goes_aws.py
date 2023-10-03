"""
pansat.download.providers.goes_aws
==================================

This module contains a data provider for data from currently operational
 GOES series satellites (GOES 16 and 17), which is available from Amazon
 AWS cloud storage.

Reference
---------
"""
from copy import copy
from datetime import datetime
from pathlib import Path
import re
from typing import List, Union, Optional

import requests
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import numpy as np

from pansat.download.providers.discrete_provider import DiscreteProviderDay
from pansat.time import to_datetime
from pansat.file_record import FileRecord

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


class GOESAWSProvider(DiscreteProviderDay):
    """
    Dataprovider class for product available from NOAA GOES16 bucket on Amazon
    AWS.
    """

    bucket_name = "noaa-goes"

    def __init__(self, series_index):
        """
        Create new NOAA GOES16 provider.

        Args:
            product: The product to download.
        """
        super().__init__()
        self._client = None
        self.series_index = series_index
        self.cache = {}
        self.product_regexp = re.compile(rf"satellite.goes\..*goes{series_index:02}.*")

    @property
    def client(self):
        """
        Delayed access to boto client.
        """
        if self._client is None:
            self._client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
        return self._client

    def provides(self, product):
        return self.product_regexp.match(product.name) is not None

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return GOES_AWS_PRODUCTS

    def _get_keys(
        self, product: "pansat.Product", date: Union[datetime, np.datetime64]
    ) -> List[str]:
        """
        Args:
           prefix: Prefix string to limit search.


        Return:
            List of available filename.
        """
        date = to_datetime(date)
        year = date.year
        day = int(date.strftime("%j"))

        bucket = f"{self.bucket_name}{self.series_index:02}"

        instr_str = product.instrument.upper()
        lvl_str = f"L{product.level}"
        prod_str = f"{product.product_name.capitalize()}"
        reg_str = product.region.upper()
        prefix = f"{instr_str}-{lvl_str}-{prod_str}{reg_str}/{year}/{day:03}"
        kwargs = {"Bucket": bucket, "Prefix": prefix}

        files = []
        if prefix not in self.cache:
            while True:
                response = self.client.list_objects_v2(**kwargs)
                if "Contents" in response:
                    for cont in response["Contents"]:
                        url = cont["Key"]
                        rec = FileRecord.from_remote(
                            product, self, url, url.split("/")[-1]
                        )
                        files.append(rec)
                try:
                    kwargs["ContinuationToken"] = response["NextContinuationToken"]
                except KeyError:
                    break
            self.cache[prefix] = files
        return self.cache[prefix]

    def _get_request_url(self, product, time, filename):
        """
        Get the URL for requesting a given file.
        """
        instr_str = product.instrument.upper()
        lvl_str = f"L{product.level}"
        prod_str = f"{product.product_name.capitalize}"
        reg_str = product.region.upper()
        folder = f"{instr_str}-{lvl_str}-{prod_str}{reg_str}"

        time = to_datetime(time)
        year = time.year
        day = int(time.strftime("%j"))
        hour = time.hour

        url = f"https://noaa-goes{self.product.series_index}.s3.amazonaws.com/"
        url += f"{folder}/{year}/{day}/{hour:02}/{filename}"
        return url

    def find_files_by_day(self, product, date):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.
            day(``int``): The Julian day for which to look up the files.

        Return:
            A list of strings containing the filename that are available
            for the given day.
        """
        files = [rec for rec in self._get_keys(product, date) if product.matches(rec)]
        return files

    def download(
        self, rec: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download a product file to a given destination.

        Args:
            file_record: A FileRecord identifying the

        Return:
            An updated file record whose 'local_path' attribute points
            to the downloaded file.
        """
        if destination is None:
            destination = rec.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        bucket = f"{self.bucket_name}{self.series_index:02}"
        obj = rec.remote_path

        if destination.is_dir():
            destination = destination / rec.filename

        with open(destination, "wb") as output_file:
            self.client.download_fileobj(bucket, obj, output_file)

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec


goes_16_aws_provider = GOESAWSProvider(16)
goes_17_aws_provider = GOESAWSProvider(17)
goes_18_aws_provider = GOESAWSProvider(18)
