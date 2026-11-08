"""
pansat.download.providers.gk2a_aws
==================================

This module provides a data provider for GEO-KOMPTSAT-2A observations available through the
NOAA AWS archive.
---------
"""
from contextlib import contextmanager
from copy import copy
from datetime import datetime
from pathlib import Path
import re
from typing import List, Union, Optional

import requests
import boto3
from boto3.s3.transfer import TransferConfig
from botocore import UNSIGNED
from botocore.config import Config
import numpy as np
import s3fs

from pansat.download.providers.discrete_provider import DiscreteProviderHour
from pansat.time import to_datetime
from pansat.file_record import FileRecord


class GK2AAWSProvider(DiscreteProviderHour):
    """
    Dataprovider class for product available from NOAA GK2A bucket on Amazon
    AWS.
    """

    bucket_name = "noaa-gk2a-pds"

    def __init__(self):
        """
        Create new GK2A PDS provider.

        Args:
            product: The product to download.
        """
        super().__init__()
        self._client = None
        self.cache = {}
        self.product_regexp = re.compile(rf"satellite.gk2a\..*")

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
        month = date.month
        hour = date.hour
        day = date.day
        bucket = f"{self.bucket_name}"
        reg_str = product.region.upper()
        prefix = f"AMI/L1B/{reg_str}/{year}{month:02}/{day:02}/{hour:02}"
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
            cloud_path = f"s3://{bucket}/{url}/"
            rec = FileRecord.from_remote(
                product, self, url, url.split("/")[-1],
                cloud_path=cloud_path
            )
            recs.append(rec)
        self.cache.pop(prefix)
        return recs

    def find_files_by_hour(self, product, date):
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

        bucket = f"{self.bucket_name}"
        obj = rec.remote_path

        if destination.is_dir():
            destination = destination / rec.filename

        with open(destination, "wb") as output_file:
            response = self.client.get_object(Bucket=bucket, Key=obj)
            output_file.write(response['Body'].read())

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec

    @contextmanager
    def open_remote(self, rec: FileRecord):
        fs = s3fs.S3FileSystem(anon=True)
        with fs.open(rec.cloud_path, "rb") as data:
            yield data


gk2a_aws_provider = GK2AAWSProvider()
