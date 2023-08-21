"""
pansat.download.providers.uci
=============================

Provider to download PERSIANN files from servers of the University
of California, Irvine.
"""
from datetime import datetime, timedelta
from urllib.request import urlopen
from pathlib import Path

from bs4 import BeautifulSoup
import requests

from pansat.download.providers.discrete_provider import DiscreteProvider

BASE_URL = "http://persiann.eng.uci.edu/CHRSdata/"
_CACHE = {}


def get_links(url):
    """
    Extract all link destination from a given URL.

    Args:
         url: URL of the website from which to retrieve the link targets.

    Return:
        List of strings containing the targets of all links find under
        the given URL.
    """
    if url not in _CACHE:
        html = urlopen(url).read()
        soup = BeautifulSoup(html, features="html.parser")
        files = []
        for link in soup.findAll("a"):
            files.append(link.get("href"))
        _CACHE[url] = files
    files = _CACHE[url]
    return files


class UciProvider(DiscreteProvider):
    """
    Data provider providing access to the PDIRNow, PERSIANN-CCS and
    PERSIANN-CDF products from the Center for Hydrometeorology and
    Remote Sensing.
    """

    @classmethod
    def get_available_products(cls):
        return ["PDIRNow", "PERSIANN-CCS", "PERSIANN-CDR"]

    def __init__(self, product):
        """
        Create provider for a given product.
        """
        self.product = product

    def get_files_by_day(self, year, day):
        """
        Retrieve available files for a given day.

        Args:
            year: Integer specifying the year for which to retrieve the
                available files.
            day: Julian day for which to retrieve available files.

        Return:
            List of strings containing the filename of the files of the
            providers product that are available on the requested day.
        """
        url = BASE_URL + self.product.get_path(year)
        links = get_links(url)

        date = datetime(year=year, month=1, day=1) + timedelta(days=day - 1)
        files = []
        for l in links:
            try:
                f_date = self.product.filename_to_date(l)
                if (
                    (f_date.year == date.year)
                    and (f_date.month == date.month)
                    and (f_date.day == date.day)
                ):
                    files.append(l)
            except ValueError:
                pass
        return files

    def download_file(self, filename, destination):
        """
        Download a given file.

        Args:
            filename: Name of the file to download.
            destination: Filename to which to store the downloaded
                file.
        """
        date = self.product.filename_to_date(filename)
        year = date.year
        url = BASE_URL + self.product.get_path(year) + "/" + filename
        response = requests.get(url)
        with open(destination, "wb") as output:
            for chunk in response:
                output.write(chunk)
