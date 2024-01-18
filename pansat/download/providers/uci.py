"""
pansat.download.providers.uci
=============================

Provider to download PERSIANN files from servers of the University
of California, Irvine.
"""
from copy import copy
from datetime import datetime, timedelta
from urllib.request import urlopen
from pathlib import Path
from typing import List, Union, Optional

from bs4 import BeautifulSoup
import requests
import numpy as np

from pansat import cache
from pansat.download.providers.discrete_provider import (
    DiscreteProviderDay,
    DiscreteProviderYear
)
from pansat.products import Product
from pansat.file_record import FileRecord
from pansat.time import to_datetime, TimeRange

BASE_URL = "http://persiann.eng.uci.edu/CHRSdata/"
_CACHE = {}

URLS = {
    "satellite.persiann.cdr_daily":  BASE_URL + "PERSIANN-CDR/daily",
    "satellite.persiann.cdr_monthly":  BASE_URL + "PERSIANN-CDR/mthly",
    "satellite.persiann.cdr_yearly":  BASE_URL + "PERSIANN-CDR/yearly",
    "satellite.persiann.ccs_3h":  BASE_URL + "PERSIANN-CSS/3hrly",
    "satellite.persiann.ccs_6h":  BASE_URL + "PERSIANN-CSS/6hrly",
    "satellite.persiann.ccs_daily":  BASE_URL + "PERSIANN-CSS/daily",
    "satellite.persiann.css_monthly":  BASE_URL + "PERSIANN-CCS/mthly",
    "satellite.persiann.ccs_yearly":  BASE_URL + "PERSIANN-CCS/yearly",
}


class UCIProvider(DiscreteProviderYear):
    """
    Data provider providing access to the PERSIANN-CCS and PERSIANN-CDR
    products from the Center for Hydrometeorology and Remote Sensing at
    UC Irvine.
    """
    def provides(self, product: "pansat.Product") -> bool:
        """
        This provider provides all currently available PERSIANN products.
        """
        return product.name in URLS


    def get_file_records(self, product: Product, url: str):
        """
        Extract all file records from a html file listing.

        PERSIANN files are exposed as HTML downloads on give URLs. This
        function extracts all link targets with filenames matching the
        given product.

        Args:
            url: URL of the website from which to extract file records
                from links.

        Return:
            List of strings containing the targets of all links find under
            the given URL.
        """
        session = cache.get_session()
        response = session.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, features="html.parser")
        files = []
        for link in soup.findAll("a"):
            target = link.get("href")
            filename = target.split("/")[-1]
            if product.matches(filename):
                files.append(
                    FileRecord.from_remote(
                        product=product,
                        provider=self,
                        remote_path=url + "/" + target,
                        filename=filename
                    )
                )
        return files


    def find_files_by_year(
            self,
            product: Product,
            time: Union[np.datetime64, datetime]
    ) -> List[FileRecord]:
        """
        Retrieves all available files for a given year.

        Args:
            time: A time stamp identifying a given year.

        Return:
            List of file records identifying files available for the
            given year.
        """
        date = to_datetime(time)
        url = URLS[product.name]
        recs = self.get_file_records(product, url)
        time_range = TimeRange(
            datetime(year=date.year, month=1, day=1),
            datetime(year=date.year + 1, month=1, day=1) - timedelta(seconds=1)
        )
        within_year = []
        for rec in recs:
            if rec.temporal_coverage.covers(time_range):
                within_year.append(rec)
        return within_year


    def download(
            self,
            rec: FileRecord,
            destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download a given file.

        Args:
            rec: A filename identifying the file to download.
            filename: Name of the file to download.
            destination: Filename to which to store the downloaded
                file.

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
        response = requests.get(url)
        response.raise_for_status()
        with open(destination, "wb") as output:
            for chunk in response:
                output.write(chunk)

        new_rec = copy(rec)
        new_rec.local_path = destination
        return new_rec


uci_provider = UCIProvider()
