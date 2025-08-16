"""
pansat.download.providers.nasa_nccs
===================================

This module provides a data provider for the NASA NCCS data portal.
"""

from copy import copy
import logging
from pathlib import Path
from typing import Optional

import requests
from requests.exceptions import HTTPError

from pansat import FileRecord
from pansat.download import accounts
from pansat.download.providers.discrete_provider import DiscreteProviderDay
from pansat.products.model.geos import GEOSForecastProduct, GEOSAnalysisProduct
from pansat.time import to_datetime


LOGGER = logging.getLogger(__file__)


class NASANCCSProvider(DiscreteProviderDay):
    """
    Dataprovider class for for data available from portal.nccs.nasa.gov
    """

    def provides(self, product) -> bool:
        """
        Indicates whether product is provided by the provider.
        """
        return isinstance(product, GEOSAnalysisProduct)

    def download_url(self, url, path):
        with requests.Session() as session:
            try:
                response = session.get(url, stream=True)
                response.raise_for_status()  # Ensure the request was successful
                with open(path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            except HTTPError as err:
                LOGGER.error(f"Failed to download {url}: {err}")
                raise

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
        base_url = "https://portal.nccs.nasa.gov/datashare/gmao/geos-fp/das"
        time = to_datetime(time)
        rel_url = time.strftime("/Y%Y/M%m/D%d/")
        url = base_url + rel_url

        session = requests.Session()
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
        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}", fname)
            for fname in files
        ]
        return recs


nasa_nccs_provider = NASANCCSProvider()


class NASANCCSForecastProvider(NASANCCSProvider):
    """
    Dataprovider class for for forecast data available from portal.nccs.nasa.gov
    """

    def provides(self, product) -> bool:
        """
        Indicates whether product is provided by the provider.
        """
        return isinstance(product, GEOSForecastProduct)

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
        base_url = "https://portal.nccs.nasa.gov/datashare/gmao/geos-fp/forecast"
        time = to_datetime(time)

        recs = []

        for hour in [0, 6, 12, 18]:
            rel_url = time.strftime("/Y%Y/M%m/D%d/") + f"H{hour:02}"

            url = base_url + rel_url
            session = requests.Session()
            response = session.get(url)

            # 404 error likely means that no products are available for
            # this day.
            try:
                response.raise_for_status()
            except HTTPError as exc:
                if exc.response.status_code == 404:
                    continue

            files = set()
            for match in product.filename_regexp.finditer(response.text):
                files.add(match.group(0))

            recs += [
                FileRecord.from_remote(product, self, url + f"/{fname}", fname)
                for fname in files
            ]
        return recs


forecast_provider = NASANCCSForecastProvider()
