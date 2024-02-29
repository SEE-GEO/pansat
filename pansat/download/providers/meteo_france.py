"""
pansat.download.providers.meteo_france
======================================

Provides a provider for downloading data from Meteo France APIs.
"""
from copy import copy
from datetime import datetime, timedelta
import json
from pathlib import Path
import requests
import shutil
import time
from typing import List, Dict, Optional

from pansat import FileRecord, TimeRange
from pansat.time import to_datetime
from pansat.download.providers.discrete_provider import DiscreteProviderDay, Time
from pansat.download.accounts import get_identity
from pansat.geometry import Geometry


def ensure_extension(path, ext):
    if not any([path[-len(e) :] == e for e in ext]):
        path = path + ext[0]
    return path


PARTNER_PRODUCTS = {
    "ground_based.opera.surface_precip": (
        "partner/radar/europe/odyssey/1.1/archive/composite/RAINFALL_RATE"
    ),
    "ground_based.opera.reflectivity": (
        "partner/radar/europe/odyssey/1.1/archive/composite/REFLECTIVITY"
    ),
}


ARCHIVE = "partner/radar/europe/odyssey/1.1/archive?date={date_str}"


class MeteoFrancePartnerProvider(DiscreteProviderDay):
    """
    Base class for data products available from the ICARE ftp server.
    """

    base_url = "https://partner-api.meteofrance.fr"

    def provides(self, product: "pansat.Product") -> bool:
        """
        Check if provider provides the product.
        """
        return product.name in PARTNER_PRODUCTS

    def get_authentication_headers(self, accept: str) -> Dict[str, str]:
        """
        Get dictionary of authentication headers for a MeteoFrance API
        request.

        The apikey is retrieved from the pansat identify file.

        Args:
            accept: String specifying the accepted responses.
        """
        _, api_key = get_identity("MeteoFrance")
        return {"accept": accept, "apikey": api_key}

    def get_request_url(self, product: "pansat.Product", time: Time) -> str:
        """
        Get URL for requesting an Opera composite file.

        Args:
            product: The OPERA product to request.
            time: A time stamp specifying the day for which to download the data.

        Return:

            A string containing the URL for the file request.
        """
        product_url = PARTNER_PRODUCTS[product.name]
        date = to_datetime(time)
        date_str = date.strftime("%Y-%m-%d")
        url = "/".join([self.base_url, product_url, date_str + "?format=HDF5"])
        return url

    def find_files_by_day(
        self, product: "pansat.Product", time: Time, roi: Optional[Geometry] = None
    ) -> List[FileRecord]:
        date = to_datetime(time)
        date_str = date.strftime("%Y-%m-%d")
        url = "/".join([self.base_url, ARCHIVE.format(date_str=date_str)])
        resp = requests.get(
            url, headers=self.get_authentication_headers(accept="application/json")
        )
        resp.raise_for_status()
        products = json.loads(resp.text)
        if "composite" in products:
            return [
                FileRecord.from_remote(
                    product=product,
                    provider=self,
                    filename=product.get_filename(time),
                    remote_path=self.get_request_url(product, time),
                )
            ]
        return []

    def download(
        self, rec: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        if destination is None:
            destination = rec.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        destination = Path(destination)
        if destination.is_dir():
            destination = destination / rec.filename

        time = rec.temporal_coverage.start
        url = self.get_request_url(rec.product, time)
        headers = self.get_authentication_headers(accept="application/tar")

        with requests.get(url, headers=headers, stream=True) as resp:
            resp.raise_for_status()
            with open(destination, "wb") as output:
                shutil.copyfileobj(resp.raw, output)

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec


meteo_france_partner_provider = MeteoFrancePartnerProvider()
