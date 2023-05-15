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
    "MHS_L1B": "EO:EUM:DAT:METOP:MHSL1",
    "AVHRR_L1B": "EO:EUM:DAT:METOP:AVHRRL1",
}


def _retrieve_access_token(key, secret):
    """
    Retrieve an access token for the EUMETSAT data store. The access token
    is required to download files. It is time limited and must be generated
    for every session.

    Args:
        key: The EUMETSAT API key. It is stored as user name in the pansat
            credentials. It is obtained from the EUMETSAT data store web API.
        secret: The corresponding password.
    """
    b64 = base64.b64encode((key + ":" + secret).encode())
    header = "Authorization: Basic " + b64.decode()
    data = {"grant_type": "client_credentials"}
    url = "https://api.eumetsat.int/token"
    req = requests.post(
        url,
        auth=requests.auth.HTTPBasicAuth(key, secret),
        data={"grant_type": "client_credentials"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    if not req.ok:
        raise CommunicationError(
            "Retrieving the EUMETSAT access token failed with the following"
            f"error:\n {req.text}"
        )
    return req.json()


###############################################################################
# Access Token
###############################################################################


class AccessToken:
    """
    This class represents an AccessToken for the EUMETSAT data store.
    """

    def __init__(self, key, secret):
        """
        Obtain access token.

        Args:
            key: The EUMETSAT API key. It is stored as user name in the pansat
                credentials. It is obtained from the EUMETSAT data store web API.
            secret: The corresponding password.
        """
        token_data = _retrieve_access_token(key, secret)
        self.token = token_data["access_token"]
        self.lifetime = token_data["expires_in"]
        self.created = datetime.now()

    @property
    def valid(self):
        """
        Whether the key is still valid.
        """
        now = datetime.now()
        age = (now - self.created).total_seconds()
        return age < self.lifetime

    def __repr__(self):
        return f"AccessToken(created={self.created}, lifetime={self.lifetime})"

    def __str__(self):
        return str(self.token)

    def renew(self, key, secret):
        """
        Renew access token.
        """
        self.__init__(key, secret)

    def ensure_valid(self):
        if not self.valid:
            self.renew()


class Collection:
    """
    Helper dataset for parsing collections.
    """

    def __init__(self, identifier, url):
        """
        Retrieve info for collection.
        """
        self.identifier = identifier
        self.url = url

        req = requests.get(url)
        if not req.ok:
            raise CommunicationError(
                f"Retrieving the collection {identifier} from the EUMETSAT data "
                f"store failed with the following error:\n {req.text}."
            )
        data = req.json()["collection"]
        self.title = data["properties"]["title"]
        self.abstract = data["properties"]["abstract"]

        def __repr__(self):
            return f"Collection({self.collection_id}, {self.title})"


class EUMETSATProvider(DataProvider):
    """
    Base class for data products available from the EUMETSAT data store.
    """

    base_url = "https://api.eumetsat.int"

    @staticmethod
    def get_collections():
        """
        Return collections available from the Eumetsat data
        store.

        Return:
            A dict mapping names of the available collections to the
            URLs at which they are available.
        """
        # b64 = base64.b64encode((key + ":" + secret).encode())
        url = "https://api.eumetsat.int/data/browse/collections"
        req = requests.get(
            url,
            params={"format": "json"},
        )
        if not req.ok:
            raise CommunicationError(
                "Retrieving the collections from the EUMETSAT data store failed"
                f"with the following error:\n {req.text}"
            )
        data = req.json()
        collections = []
        for link in data["links"]:
            identifier = link["title"]
            url = link["href"]
            collections.append(Collection(identifier, url))

        return collections

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
        self.page_size = 100
        self._token = None

    @property
    def token(self):
        """The identification token for the API."""
        if self._token is None:
            user, password = get_identity("EUMETSAT")
            self._token = AccessToken(user, password)
        if not self._token.valid:
            user, password = get_identity("EUMETSAT")
            self._token.renew(user, password)
        return self._token

    def get_available_products():
        """List of the products available from this provider."""
        return _PRODUCTS.keys()

    def get_files_in_range(self, start, end, bounding_box=None):
        """
        Return files in available in range.

        Args:
            start: Start of the time range.
            end: End of the time range.
            bounding_box: An optional bounding box specifying a region
                of interest (ROI). If given, only files containing
                observations over this regions will be returned.

        Return:
             A list of URLs of available files.
        """
        product_id = _PRODUCTS[str(self.product)]

        parameters = {
            "format": "json",
            "pi": product_id,
            "si": 0,
            "c": self.page_size,
            "dtstart": start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "dtend": end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        if bounding_box is not None:
            bb_str = ",".join(map(str, bounding_box))
            parameters["bbox"] = bb_str
        url = self.base_url + f"/data/search-products/os"

        links = []

        # Helper function to extract links from response data.
        def get_link(feature):
            return feature["properties"]["links"]["data"][0]["href"]

        total_results = self.page_size
        start_index = 0
        while start_index < total_results:
            parameters["si"] = start_index
            with requests.get(url, params=parameters) as r:
                r.raise_for_status()

                datasets = r.json()
                features = datasets["features"]
                links += map(get_link, datasets["features"])
            start_index += self.page_size

        links = []

        # Helper function to extract links from response data.
        def get_link(feature):
            return feature["properties"]["links"]["data"][0]["href"]

        total_results = self.page_size
        start_index = 0
        while start_index < total_results:
            parameters["si"] = start_index
            with requests.get(url, params=parameters) as r:
                datasets = r.json()
                features = datasets["features"]
                links += map(get_link, datasets["features"])
            start_index += self.page_size

        return links

    def download_file(self, link, destination):
        """
        Download a specific file.

        Args:
            link: Link pointing towards the file to download.:
            destination: Where to store the file.

        Return:
            The path of the local file to which the file content
            was written.
        """
        destination = Path(destination)
        if not destination.exists():
            destination.mkdir(exist_ok=True, parents=True)

        self.token.ensure_valid()
        params = {"access_token": str(self.token)}

        with requests.get(link, stream=True, params=params) as r:
            if not r.ok:
                raise CommunicationError(
                    f"Downloading the file {link} from the EUMETSAT data "
                    f"store failed token failed with the following"
                    f"error:\n {r.text}."
                )
            filename = link.split("/")[-1]
            dest = destination / (filename + ".zip")
            with open(dest, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        return dest

    def download(self, start, end, destination):
        """
        Download all products within a given time range.

        Args:
            start: Start time
            end: End time.
            destination: Path to where to store the downloaded files.

        Return:
            List of the local paths of all downloaded files.
        """
        destination = Path(destination)

        links = self.get_files_in_range(start, end)
        downloads = []

        for link in links:
            filename = self.download_file(link, destination)
            downloads.append(filename)

        return downloads
