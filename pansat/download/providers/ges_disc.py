"""
pansat.download.providers.ges_disc
==================================

This module provides a data provider for NASA's Goddard Earth Sciences Data and
Information Services Center (`GES DISC <https://disc.gsfc.nasa.gov/>`_).

Reference
---------
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


class GesDiscProviderBase:
    """
    Dataprovider class for for products available from the
    the gesdisc.eosdis.nasa.gov servers.
    """

    file_pattern = re.compile(r'"[^"]*\.(?:HDF5|h5|nc4?)"')

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return GPM_PRODUCTS.keys()

    def download_url(self, url, path):
        """
        Downloads file from GES DISC server using the 'GES DISC' identity
        and taking care of the URL redirection.
        """
        auth = accounts.get_identity("GES DISC")
        # If only requests.get(url, auth=auth) is used, the requests library
        # will search in ~/.netrc credentials for the machine
        # urs.earthdata.nasa.gov, but `auth` is not used as the authorization
        # is after a redirection
        # The method below handles the authorization after a redirection
        with requests.Session() as session:
            # Set credentials
            session.auth = auth

            # Get data
            redirect = session.get(url, auth=auth)
            response = session.get(redirect.url, auth=auth, stream=True)
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

    def download_metadata(self, filename):
        """
        Download metadata for given file.

        Args:
            filename(``str``): The name of the file to download.

        Return:
            The metadata in XML formaton
        """
        t = self.product.filename_to_date(filename)
        year = t.year
        day = t.strftime("%j")
        day = "0" * (3 - len(day)) + day
        url = (
            self._request_string.format(year=year, day=day, filename=filename) + ".xml"
        )
        session = cache.get_session()
        response = session.get(url)
        return ET.fromstring(response.text)


class GesDiscProviderDay(GesDiscProviderBase, DiscreteProviderDay):
    def provides(self, product) -> bool:
        """
        Determine whether provide provides the given product.
        """
        name = product.name
        if not name.startswith("satellite.gpm"):
            return False
        if hasattr(product, "variant"):
            if product.variant.startswith("mo"):
                return False
            if product.variant.startswith("day"):
                return False
        return True

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
        rel_url = time.strftime("/%Y/%j")
        url = self.get_base_url(product) + rel_url
        auth = accounts.get_identity("GES DISC")

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


class GesDiscProviderMonth(GesDiscProviderBase, DiscreteProviderMonth):
    def provides(self, product):
        name = product.name
        if not name.startswith("satellite.gpm"):
            return False
        if hasattr(product, "variant"):
            if product.variant.startswith("day"):
                return True
        return False

    def find_files_by_month(self, product, time) -> list[FileRecord]:
        """
        Return list of available files for a given day of a year.

        Args:


        Return:
            A list of strings containing the filenames that are available
            for the given year.
        """
        url = self.get_base_url(product) + f"/{time.year}/{time.month:02}"
        auth = accounts.get_identity("GES DISC")
        session = cache.get_session()
        response = session.get(url, auth=auth)
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


class GesDiscProviderYear(GesDiscProviderBase, DiscreteProviderYear):
    def provides(self, product):
        name = product.name
        if not name.startswith("satellite.gpm"):
            return False
        if hasattr(product, "variant"):
            if product.variant.startswith("mo"):
                return True
        return False

    def find_files_by_year(self, product, time, roi=None) -> list[FileRecord]:
        """
        Return list of available files for a given day of a year.

        Args:
            product: A 'pansat.Product' object identifying the product
                whose data files to find.
            time: A time object specifying the year for which to find
                data files.
            roi: An optional geometry object limiting the search to files
                within a given geographical region.
        Return:
            A list of file records identifying the found files.
        """
        url = self.get_base_url(product) + f"/{time.year}"
        auth = accounts.get_identity("GES DISC")
        session = cache.get_session()
        response = session.get(url, auth=auth)
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


ges_disc_provider_day = GesDiscProviderDay()
ges_disc_provider_month = GesDiscProviderMonth()
ges_disc_provider_year = GesDiscProviderYear()


class MerraProvider(GesDiscProviderBase, DiscreteProviderMonth):
    """
    Provider for MERRA2 data from NASA GesDisc servers.
    """

    MERRA_PRODUCTS = {
        "reanalysis.merra.m2i3npasm": [
            "https://goldsmr5.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2I3NPASM.5.12.4/"
        ],
        "reanalysis.merra.m2i3nvaer": [
            "https://goldsmr5.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2I3NVAER.5.12.4/"
        ],
        "reanalysis.merra.m2i3nvasm": [
            "https://goldsmr5.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2I3NVASM.5.12.4/"
        ],
        "reanalysis.merra.m2i1nxasm": [
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2I1NXASM.5.12.4/"
        ],
        "reanalysis.merra.m2t1nxlnd": [
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2T1NXLND.5.12.4/"
        ],
        "reanalysis.merra.m2t1nxflx": [
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2T1NXFLX.5.12.4/"
        ],
        "reanalysis.merra.m2t1nxrad": [
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2T1NXRAD.5.12.4/"
        ],
        "reanalysis.merra.m2t1nxrad": [
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/data",
            "MERRA2/M2T1NXRAD.5.12.4/"
        ],
    }

    def provides(self, product) -> bool:
        """
        Determine whether provide provides the given product.
        """
        name = product.name
        if name.startswith("reanalysis.merra"):
            return name in self.MERRA_PRODUCTS
        return False

    def get_base_url(self, product):
        """
        URL pointing to the root of the directory tree containing the
        data files of the given product.

        Args:
            product: The product for which to retrieve the URL.

        Return:
            The URL as a string.
        """
        base_url, path = self.MERRA_PRODUCTS[product.name]
        return "/".join([base_url, path])

    def find_files_by_month(self, product, time, roi=None):
        """
        Find files available data files for a given month.

        Args:
            product: A 'pansat.Product' object identifying the product
               for which to retrieve available data files.
            time: A time object specifying the month for which to retrieve
               available products.
            roi: An optional geometry object to limit the files to
               only those that cover a certain geographical region.

        Return:
            A list of file records identifying the files from the requested
            day.
        """
        time = to_datetime(time)
        rel_url = time.strftime("/%Y/%m")
        url = self.get_base_url(product) + rel_url
        session = cache.get_session()
        response = session.get(url)

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

        subset_str = ""
        variables = product.variables
        if variables is not None:
            url = url.replace("/data/", "/opendap/")
            subset_str = ".nc4?" + ",".join(variables + ["time", "lat", "lon"])

        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}{subset_str}", fname)
            for fname in files
        ]
        return recs

    def download_url(self, url, path):
        """
        Download file from MERRA GES DISC server.
        """
        auth = accounts.get_identity("GES DISC")
        ## If only requests.get(url, auth=auth) is used, the requests library
        ## will search in ~/.netrc credentials for the machine
        ## urs.earthdata.nasa.gov, but `auth` is not used as the authorization
        ## is after a redirection
        ## The method below handles the authorization after a redirection
        with requests.Session() as session:
            redirect = session.get(url, auth=auth)
            response = session.get(redirect.url, auth=auth, stream=True)
            response.raise_for_status()

            # Write to disk
            with open(path, "wb") as f:
                for chunk in response:
                    f.write(chunk)

ges_disc_provider_merra = MerraProvider()


class MerraConstantProvider(MerraProvider):
    """
    Provider for MERRA2 data from NASA GesDisc servers.
    """

    MERRA_PRODUCTS = {
        "reanalysis.merra.m2conxasm": [
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/data",
            "MERRA2_MONTHLY/M2C0NXASM.5.12.4/"
        ],
        "reanalysis.merra.m2conxctm": [
            "https://goldsmr4.gesdisc.eosdis.nasa.gov/data",
            "MERRA2_MONTHLY/M2C0NXCTM.5.12.4/"
        ],
    }

    def find_files(
            self,
            product: "pansat.Product",
            time_range: "pansat.TimeRange",
            roi: "pansat.Geometry" = None
    ) -> List[FileRecord]:
        """
        Find files available data files at a given day.

        Args:
            product: A 'pansat.Product' object identifying the product
               for which to retrieve available data files.
            time_range: A time object specifying the day for which to retrieve
               available products.
            roi: An optional geometry object to limit the files to
               only those that cover a certain geographical region.

        Return:
            A list of file records identifying the files from the requested
            day.
        """
        url = self.get_base_url(product) + "1980"
        session = cache.get_session()
        response = session.get(url)

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

        subset_str = ""
        variables = product.variables
        if variables is not None:
            url = url.replace("/data/", "/opendap/")
            subset_str = ".nc4?" + ",".join(variables + ["time", "lat", "lon"])

        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}{subset_str}", fname)
            for fname in files
        ]
        return recs


ges_disc_provider_merra_constant = MerraConstantProvider()
