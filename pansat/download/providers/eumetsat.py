"""
pansat.download.providers.eumetsat
==================================

This module provides the ``EUMETSATProvider`` class, which implements a data
provider class for the `EUMETSAT data store <https://www.data.eumetsat.int/>`_.

"""
import base64
from datetime import datetime
from ftplib import FTP
from pathlib import Path
import shutil

import requests

from pansat.download.providers.data_provider import DataProvider
from pansat.download.accounts import get_identity
from pansat.exceptions import CommunicationError

_PRODUCTS = {
    "MSG_Seviri": "EO:EUM:DAT:MSG:HRSEVIRI",
    "MSG_Seviri_IO": "EO:EUM:DAT:MSG:HRSEVIRI-IODC",
}


def _retrieve_access_token(key, secret):
    b64 = base64.b64encode((key + ":" + secret).encode())
    header = "Authorization: Basic " + b64.decode()
    data = {"grant_type": "client_credentials"}
    url = "http://api.eumetsat.int/token"

    r = requests.post(
        url,
        auth=requests.auth.HTTPBasicAuth(key, secret),
        data={"grant_type": "client_credentials"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    if not r.ok:
        raise CommunicationError(
            "Retrieving the EUMETSAT access token failed with the following"
            f"error:\n {r.text}"
        )

    return r.json()


class AccessToken:
    """
    Simple helper class to manage the EUMETSAT access token.
    """

    def __init__(self, key, secret):
        token_data = _retrieve_access_token(key, secret)
        self.token = token_data["access_token"]
        self.lifetime = token_data["expires_in"]
        self.created = datetime.now()

    @property
    def valid(self):
        now = datetime.now()
        age = (now - self.created).total_seconds()
        return age < self.lifetime

    def __repr__(self):
        return f"AccessToken(created={self.created}, lifetime={self.lifetime})"

    def __str__(self):
        return str(self.token)

    def renew(self, key, secret):
        self.__init__(key, secret)


class EUMETSATProvider(DataProvider):
    """
    Base class for data products available from the EUMETSAT data store.
    """

    base_url = "http://api.eumetsat.int"

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

            product(``Product``): Product class object with specific product for ICARE

        """
        if str(product) not in _PRODUCTS:
            available_products = list(_PRODUCTS.keys())
            raise ValueError(
                f"The product {product} is  not a available from the Eumetsat data"
                f" provider. Currently available products are: "
                f"{available_products}."
            )
        super().__init__()
        self.product = product

    def get_available_products():
        return _PRODUCTS.keys()

    def get_files_in_range(self, start, end):
        product_id = _PRODUCTS[str(self.product)]

        parameters = {
            "format": "json",
            "pi": product_id,
            "bbox": "-90,-90, 90, 90",
            "dtstart": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "dtend": end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        url = self.base_url + "/data/search-products/os"

        with requests.get(url, params=parameters) as r:
            datasets = r.json()
        links = [f["properties"]["links"]["data"][0]["href"] for f in datasets["features"]]
        return links

    def download(self, start, end, destination):
        destination = Path(destination)

        user, password = get_identity("EUMETSAT")
        token = AccessToken(user, password)

        links = self.get_files_in_range(start, end)
        downloads = []

        for l in links:
            params = {"access_token": str(token)}
            with requests.get(l, stream=True, params=params) as r:
                if not r.ok:
                    raise CommunicationError(
                        f"Downloading the file {l} from the EUMETSAT data "
                        f"store failed token failed with the following"
                        f"error:\n {r.text}."
                    )
                filename = l.split("/")[-1]
                dest = destination / (filename + ".zip")
                with open(dest, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
                downloads.append(dest)

        return downloads
