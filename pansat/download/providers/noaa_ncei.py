"""
pansat.download.providers.noaa_ncei
===================================

This module defines a provider for the NOAA NCEI data server
 https://www.ncei.noaa.gov/data/.
"""
from copy import copy
from datetime import datetime, timedelta
import re
from pathlib import Path
from typing import Optional

import requests
from pansat.download.providers.discrete_provider import (
    DiscreteProviderYear,
    DiscreteProviderMonth,
)
from pansat import cache
from pansat.file_record import FileRecord
from pansat.time import to_datetime


BASE_URL = "https://www.ncei.noaa.gov/data"


NCEI_PRODUCTS = {"ssmis": "ssmis-brightness-temperature-rss/access"}

PRODUCTS_MONTH = {
    "gridsat_goes": ("{year}/{month:02}", "gridsat-goes/access/goes"),
    "gridsat_conus": ("{year}/{month:02}", "gridsat-goes/access/conus"),
    "isccp_hxg": ("{year}{month:02}", "international-satellite-cloud-climate-project-isccp-h-series-data/access/isccp/hxg"),
}

PRODUCTS_YEAR = {
    "gridsat_b1": "geostationary-ir-channel-brightness-temperature-gridsat-b1/access",
    "ssmi_csu": "ssmis-brightness-temperature-csu/access/FCDR/",
    "ssmi_csu_gridded": "ssmis-brightness-temperature-csu/access/FCDR-GRID/",
    "ssmis_csu": "ssmis-brightness-temperature-csu/access/FCDR/",
    "ssmis_csu_gridded": "ssmis-brightness-temperature-csu/access/FCDR-GRID/",
    "amsr2_csu": "ssmis-brightness-temperature-csu/access/FCDR/",
    "amsr2_csu_gridded": "ssmis-brightness-temperature-csu/access/FCDR-GRID/",
    "patmosx": "avhrr-hirs-reflectance-and-cloud-properties-patmosx/access/",
    "patmosx_asc": "avhrr-hirs-reflectance-and-cloud-properties-patmosx/access/",
    "patmosx_des": "avhrr-hirs-reflectance-and-cloud-properties-patmosx/access/",
}

PRODUCTS_ALL = {
    "isccp_hgm": "international-satellite-cloud-climate-project-isccp-h-series-data/access/isccp-basic/hgm/",
}

LINK_REGEX = re.compile(r'<a href="([^"]*\.nc)">')


class NOAANCEIProviderBase:
    """
    Data provider for datasets available at https://www.ncei.noaa.gov/data/.
    """

    def __init__(self):
        """
        Instantiate provider for given product.

        Args:
            product: Product instance provided by the provider.
        """
        super().__init__()

    def provides(self, product: "pansat.Product") -> bool:
        """
        Whether or not this provider can provide data from the given
        product.
        """
        parts = product.name.split(".")
        if parts[0] != "satellite" or parts[1] != "ncei":
            return False
        return True

    def download(
        self, file_record: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download the file to a given destination.

        Args:
            filename: Name of the file to download.
            destination: The destination to which to write the
                results.
        """
        url = file_record.remote_path
        response = requests.get(url)
        response.raise_for_status()

        if destination.is_dir():
            destination = destination / file_record.filename

        with open(destination, "wb") as output:
            for chunk in response:
                output.write(chunk)

        new_record = copy(file_record)
        new_record.local_path = destination
        return new_record


class NOAANCEIProviderAll(NOAANCEIProviderBase, DiscreteProviderYear):
    """
    Specialization of the NOAA NCEI provider for files that are not
    sorted into subdirectories.

    """

    def __init__(self):
        NOAANCEIProviderBase.__init__(self)
        DiscreteProviderYear.__init__(self)

    def provides(self, product: "pansat.Product") -> bool:
        """
        Whether or not this provider can provide data from the given
        product.
        """
        is_ncei_product = super().provides(product)
        is_all = product.name.split(".")[-1] in PRODUCTS_ALL
        return is_ncei_product and is_all

    def find_files_by_year(self, product, time):
        """

        Args:
            time: A datetime or numpy.datetime64 object specifying the
                year for which to retrieve the files.

        Return:
            A list of file records pointing to the available files.
        """
        time = to_datetime(time)
        year = time.year

        ncei_name = product.name.split(".")[-1]
        url = f"{BASE_URL}/{PRODUCTS_ALL[ncei_name]}/"
        session = cache.get_session()
        response = session.get(url)
        pattern = re.compile(r'<a href="([^"]*\.nc)">')
        links = LINK_REGEX.findall(response.text)

        recs = []
        for link in links:
            filename = link.split("/")[-1]
            remote_path = url + link
            rec = FileRecord.from_remote(product, self, remote_path, filename)
            if product.matches(rec):
                time_range = product.get_temporal_coverage(rec)
                if time_range.start.year == year or time_range.end == year:
                    recs.append(rec)
        return recs


class NOAANCEIProviderYear(NOAANCEIProviderBase, DiscreteProviderYear):
    def __init__(self):
        NOAANCEIProviderBase.__init__(self)
        DiscreteProviderYear.__init__(self)

    def provides(self, product: "pansat.Product") -> bool:
        """
        Whether or not this provider can provide data from the given
        product.
        """
        is_ncei_product = super().provides(product)
        is_yearly = product.name.split(".")[-1] in PRODUCTS_YEAR
        return is_ncei_product and is_yearly

    def find_files_by_year(self, product, time):
        """
        Get files available for a given year.

        Args:
            time: A datetime or numpy.datetime64 object specifying the
                year for which to retrieve the files.

        Return:
            A list of file records pointing to the available files.
        """
        time = to_datetime(time)
        year = time.year

        ncei_name = product.name.split(".")[-1]
        url = f"{BASE_URL}/{PRODUCTS_YEAR[ncei_name]}/{year:04}/"
        session = cache.get_session()
        response = session.get(url)
        pattern = re.compile(r'<a href="([^"]*\.nc)">')
        links = LINK_REGEX.findall(response.text)

        recs = []
        for link in links:
            filename = link.split("/")[-1]
            remote_path = url + link
            rec = FileRecord.from_remote(product, self, remote_path, filename)
            if product.matches(rec):
                recs.append(rec)
        return recs


class NOAANCEIProviderMonth(NOAANCEIProviderBase, DiscreteProviderMonth):
    def __init__(self):
        NOAANCEIProviderBase.__init__(self)
        DiscreteProviderMonth.__init__(self)

    def provides(self, product: "pansat.Product") -> bool:
        """
        Whether or not this provider can provide data from the given
        product.
        """
        is_ncei_product = super().provides(product)
        is_monthly = product.name.split(".")[-1] in PRODUCTS_MONTH
        return is_ncei_product and is_monthly

    def find_files_by_month(self, product, time):
        """
        Get files available for a given year.

        Args:
            time: A datetime or numpy.datetime64 object specifying the
                year for which to retrieve the files.

        Return:
            A list of file records pointing to the available files.
        """
        time = to_datetime(time)
        year = time.year
        month = time.month

        ncei_name = product.name.split(".")[-1]
        yearmonth, path = PRODUCTS_MONTH[ncei_name]
        url = f"{BASE_URL}/{path}/{yearmonth.format(year=year, month=month)}"
        print(url)
        session = cache.get_session()
        response = session.get(url)
        links = LINK_REGEX.findall(response.text)

        recs = []
        for link in links:
            filename = link.split("/")[-1]
            remote_path = url + link
            rec = FileRecord.from_remote(product, self, remote_path, filename)
            if product.matches(rec):
                recs.append(rec)
        return recs


noaa_ncei_provider_all = NOAANCEIProviderAll()
noaa_ncei_provider_year = NOAANCEIProviderYear()
noaa_ncei_provider_month = NOAANCEIProviderMonth()
