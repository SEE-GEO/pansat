"""
pansat.download.providers.esa_maap
==================================

A data provider for the ESA Multi-Mission Algorithm and Analysis platform.
"""
from copy import copy
from functools import cached_property
import logging
from pathlib import Path
import re
from typing import Optional, List

import requests
from pystac_client import Client
from pystac_client.stac_api_io import StacApiIO
from urllib3.util.retry import Retry

from pansat.download.accounts import get_identity
from pansat.file_record import FileRecord
from pansat.geometry import Geometry
from pansat.time import TimeRange
from pansat.products import Product
from .data_provider import DataProvider


LOGGER = logging.getLogger(__name__)


PRODUCT_TYPES = {
    "satellite.earth_care.l2b_ac__tc_": 'AC__TC__2B',
    "satellite.earth_care.l2b_acm_cap": 'ACM_CAP_2B',
    "satellite.earth_care.l2b_acm_clp": 'ACM_CLP_2B',
}

# The ESA MAAP catalogue intermittently responds with 502 errors.
CATALOG_RETRY = Retry(
    total=6,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)


class ESAMAAPProvider(DataProvider):
    """
    Provider class providing an interface to the ESA MAAP catalog for EarthCARE products.
    """
    catalog_url = 'https://catalog.maap.eo.esa.int/catalogue/'

    def __init__(self):
        super().__init__()
        self.product_regexp = re.compile(rf"satellite.earth_care\..*")

    def provides(self, product):
        return self.product_regexp.match(product.name) is not None

    def token(self) -> str:
        """
        Retrieves a refreshed token using the offline token stored in the pansat
        credentials.
        """
        user_name, offline_token = get_identity("ESA MAAP")
        url = "https://iam.maap.eo.esa.int/realms/esa-maap/protocol/openid-connect/token"
        data = {
            "client_id": "offline-token",
            "client_secret": "p1eL7uonXs6MDxtGbgKdPVRAmnGxHpVE",
            "grant_type": "refresh_token",
            "refresh_token": offline_token,
            "scope": "offline_access openid"
        }
        response = requests.post(url, data=data)
        response.raise_for_status()

        response_json = response.json()
        access_token = response_json.get('access_token')
        if not access_token:
            raise RuntimeError("Failed to retrieve access token from IAM response")
        return access_token


    def download(
        self, file_record: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download a product file to a given destination.

        Args:
            file_record: A FileRecord identifying the

        Return:
            An updated file record whose 'local_path' attribute points
            to the downloaded file.
        """
        if destination is None:
            destination = file_record.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        destination = destination / file_record.filename

        url = file_record.remote_path
        headers = {"Authorization": f"Bearer {self.token()}"}

        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()
        file_size = int(response.headers.get('content-length', 0))
        chunk_size = 8 * 1024 * 1024

        with open(destination, "wb") as output:
            for chunk in response.iter_content(chunk_size=chunk_size):
                read_size=output.write(chunk)

        new_rec = copy(file_record)
        new_rec.local_path = destination
        return new_rec

    def find_files(
        self, product: Product, time_range: TimeRange, roi: Optional[Geometry]
    ) -> List[FileRecord]:
        """
        Finds file in catalog.
        """
        start_time = time_range.start.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_time = time_range.end.strftime("%Y-%m-%dT%H:%M:%SZ")
        prod_type = PRODUCT_TYPES.get(product.name)

        catalog = Client.open(
            self.catalog_url, stac_io=StacApiIO(max_retries=CATALOG_RETRY)
        )

        if roi is not None:
            lon_min, lat_min, lon_max, lat_max = roi.bounding_box_corners
            bbox = [lon_min, lat_min, lon_max, lat_max]
        else:
            bbox = None

        EC_COLLECTION = ['EarthCAREL2Validated_MAAP']
        search = catalog.search(
            collections=EC_COLLECTION,
            filter=f"productType = '{prod_type}'",
            bbox=bbox,
            datetime = [start_time, end_time],
            method = 'GET',
        )

        items = list(search.items())

        if len(items) == 0:
            EC_COLLECTION = ['JAXAL2Validated_MAAP']
            search = catalog.search(
                collections=EC_COLLECTION,
                filter=f"productType = '{prod_type}'",
                bbox=bbox,
                datetime = [start_time, end_time],
                method = 'GET',
            )
            items = list(search.items())

        recs = []
        for item in items:
            url = item.assets["enclosure_h5"].href
            rec = FileRecord.from_remote(
                product, self, url, url.split("/")[-1],
            )
            recs.append(rec)

        return recs


esa_maap_provider = ESAMAAPProvider()
