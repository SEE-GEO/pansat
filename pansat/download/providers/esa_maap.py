"""
pansat.download.providers.esa_maap
==================================

A data provider for the ESA Multi-Mission Algorithm and Analysis platform.
"""
from copy import copy
from functools import cached_property
from pathlib import Path
import re
from typing import Optional, List

import requests
from pystac_client import Client

from pansat.download.accounts import get_identity
from pansat.file_record import FileRecord
from pansat.geometry import Geometry
from pansat.time import TimeRange
from pansat.products import Product
from .data_provider import DataProvider

PRODUCT_TYPES = {
    "satellite.earth_care.l2b_ac_tc": 'AC__TC__2B',
    "satellite.earth_care.l2b_acm_cap": 'ACM_CAP_2B',
    "satellite.earth_care.l2b_acm_clp": 'ACM_CLP_2B',
}

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

    @cached_property
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
        headers = {"Authorization": f"Bearer {self.token}"}

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
        catalog = Client.open(self.catalog_url)
        EC_COLLECTION = ['EarthCAREL2Validated_MAAP']

        if roi is not None:
            lon_min, lat_min, lon_max, lat_max = roi.bounding_box_corners
        else:
            lon_min, lat_min, lon_max, lat_max = (-180, -90, 180, 90)

        for attempt in range(3):
            try:
                search = catalog.search(
                    collections=EC_COLLECTION,
                    filter=f"productType = '{prod_type}'",
                    bbox=[lon_min, lat_min, lon_max, lat_max],
                    datetime = [start_time, end_time],
                    method = 'GET',
                )
                break
            except APIError:
                LOGGER.info(
                    "Encoutered API error when getting products from ESA MAAP."
                )
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)

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
