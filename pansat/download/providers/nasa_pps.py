"""
pansat.download.providers.nasa_pps
==================================

This module provides a data provider to download GPM data from the arthurhou sever
of NASA's PPS.

"""
from copy import copy
import datetime
import json
import logging
import os
from pathlib import Path
import re
import tempfile
from typing import List, Optional
import xml.etree.ElementTree as ET

import requests
from requests.exceptions import HTTPError

from pansat import cache, FileRecord
from pansat.download import accounts
from pansat.download.providers.discrete_provider import (
    DiscreteProviderDay,
    DiscreteProviderMonth,
    DiscreteProviderYear,
)
from pansat.time import to_datetime

_DATA_FOLDER = Path(__file__).parent / "data"
with open(_DATA_FOLDER / "gpm_products.json", "r") as file:
    GPM_PRODUCTS = json.load(file)


LOGGER = logging.getLogger(__file__)


class NASAPPSProvider(DiscreteProviderDay):
    """
    Data provider for data available from https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata
    """

    def provides(self, product):
        if not product.name.startswith("satellite.gpm"):
            return False
        if product.level.startswith("1"):
            return True
        elif product.algorithm.startswith("GPROF"):
            return True
        elif product.algorithm.startswith("3IMERG"):
            return True
        return False


    def download_url(self, url, path):
        """
        Downloads file from GES DISC server using the 'GES DISC' identity
        and taking care of the URL redirection.
        """
        auth = accounts.get_identity("NASA PPS")
        # If only requests.get(url, auth=auth) is used, the requests library
        # will search in ~/.netrc credentials for the machine
        # urs.earthdata.nasa.gov, but `auth` is not used as the authorization
        # is after a redirection
        # The method below handles the authorization after a redirection
        with requests.Session() as session:
            # Set credentials
            session.auth = auth

            # Get data
            response = session.get(url, auth=auth, stream=True)
            response.raise_for_status()

            # Write to disk
            with open(path, "wb") as f:
                for chunk in response:
                    f.write(chunk)

    def get_base_url(self, product):
        """
        URL pointing to the root of the directory tree containing the
        data files of the given product.

        Args:
            product: The product for which to retrieve the URL.

        Return:
            The URL as a string.
        """
        base_url, path = GPM_PRODUCTS[product.name]
        return "/".join([base_url, path])

    def _request_string(self, product):
        """The URL containing the data files for the given product."""
        base_url = self.get_base_url(product)
        return base_url + "/{year}/{day}/{filename}"

    def find_files_by_day(self, product, time, roi=None):
        """
        Find files available data files at a given day.

        Args:
            product: A 'pansat.Product' object identifying the product
               for which to retrieve available data files.
            time: A time object specifying the day for which to retrieve
               available products.
            roi: An optional geometry object to limit the files to
               only those that cover a certain geographical region.

        Return:
            A list of file records identifying the files from the requested
            day.
        """
        time = to_datetime(time)
        rel_url = time.strftime("/%Y/%m/%d")

        if product.level.startswith("1"):
            rel_url = rel_url + f"/{product.level.upper()[:2]}"
        elif product.algorithm.lower().startswith("gprof"):
            rel_url = rel_url + f"/gprof"
        elif product.algorithm.lower().startswith("3imerg"):
            rel_url = rel_url + f"/imerg"
        else:
            raise ValueError(
                "The PPS data provider currently only support level 1, GPROF, and IMERG products."
            )

        url = "https://arthurhouhttps.pps.eosdis.nasa.gov/gpmdata" + rel_url

        auth = accounts.get_identity("NASA PPS")

        session = cache.get_session()
        response = session.get(url, auth=auth)

        # 404 error likely means that no products are available for
        # this day.
        try:
            response.raise_for_status()
        except HTTPError as exc:
            if exc.response.status_code == 404:
                pass

        files = set()
        for match in product.filename_regexp.finditer(response.text):
            files.add(match.group(0))
        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}", fname)
            for fname in files
        ]
        return recs

    def download(
        self, rec: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download a product file to a given destination.

        Args:
            rec: A FileRecord identifying the file to download.
            destination: An optional path pointing to a file or folder
                to which to download the file.

        Return:
            An updated file record whose 'local_path' attribute points
            to the downloaded file.
        """
        if destination is None:
            destination = rec.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        if destination.is_dir():
            destination = destination / rec.filename

        url = rec.remote_path
        self.download_url(url, destination)

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec


nasa_pps_provider = NASAPPSProvider()
