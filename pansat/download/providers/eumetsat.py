"""
pansat.download.providers.eumetsat
==================================

This module provides the ``EUMETSATProvider`` class, which implements a data
provider class for the `EUMETSAT data store <https://www.data.eumetsat.int/>`_.
"""
import base64
from copy import copy
from datetime import datetime
from ftplib import FTP
from typing import Optional
from pathlib import Path
import shutil

import requests

from pansat import FileRecord, Geometry, TimeRange, cache
from pansat.download.providers import DataProvider
from pansat.download.accounts import get_identity
from pansat.exceptions import CommunicationError


_PRODUCTS = {
    "satellite.meteosat.l1b_msg_seviri": "EO:EUM:DAT:MSG:HRSEVIRI",
    "satellite.meteosat.l1b_msg_seviri_io": "EO:EUM:DAT:MSG:HRSEVIRI-IODC",
    "satellite.meteosat.l1b_rs_msg_seviri": "EO:EUM:DAT:MSG:MSG15-RSS",
}


def _retrieve_access_token(key: str, secret: str) -> str:
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

    def __init__(self, key: str, secret: str):
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

    def __init__(self, identifier: str, url: str):
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

    def __init__(self):
        """
        Create and register data provider.
        """
        super().__init__()
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

    def provides(self, product):
        return product.name.startswith("satellite.meteosat")

    def find_files(
            self,
            product,
            time_range: TimeRange,
            roi: Optional[Geometry] = None
    ):
        """
        Find available files within a given time range and optional geographic
        region.

        Args:
            product: A 'pansat.Product' object representing the product to
                download.
            time_range: A 'pansat.time.TimeRange' object representing the time
                range within which to look for available files.
            roi: An optional region of interest (roi) restricting the search
                to a given geographical area.

        Return:
            A list of 'pansat.FileRecords' specifying the available
            files.
        """
        product_id = _PRODUCTS[product.name]

        parameters = {
            "format": "json",
            "pi": product_id,
            "si": 0,
            "c": self.page_size,
            "dtstart": time_range.start.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "dtend": time_range.end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        if roi is not None:
            bb_str = ",".join(map(str, roi.bounding_box_corners))
            parameters["bbox"] = bb_str

        url = self.base_url + f"/data/search-products/os"

        recs = []

        # Helper function to extract links from response data.
        def get_link(feature):
            return feature["properties"]["links"]["data"][0]["href"]

        total_results = self.page_size
        start_index = 0
        while start_index < total_results:
            parameters["si"] = start_index
            session = cache.get_session()
            with session.get(url, params=parameters) as r:
                r.raise_for_status()

                datasets = r.json()
                features = datasets["features"]

                links = map(get_link, datasets["features"])
                for link in links:
                    recs.append(
                        FileRecord.from_remote(
                            product=product,
                            provider=self,
                            remote_path=link,
                            filename=link.split("/")[-1],
                        )
                    )
            start_index += self.page_size

        return recs


    def download(
            self,
            rec: FileRecord,
            destination: Optional[Path] = None
    ):
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

        self.token.ensure_valid()
        params = {"access_token": str(self.token)}

        with requests.get(rec.remote_path, stream=True, params=params) as resp:
            if not resp.ok:
                raise CommunicationError(
                    f"Downloading the file {rec.filename} from the EUMETSAT "
                    " data store failed token failed with the following"
                    f"error:\n {resp.text}."
                )
            with open(destination, "wb") as output:
                shutil.copyfileobj(resp.raw, output)

        new_rec = copy(rec)
        new_rec.local_path = destination
        return new_rec


eumetsat_provider = EUMETSATProvider()
