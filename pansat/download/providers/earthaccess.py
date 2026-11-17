"""
pansat.download.providers.earthaccess
=====================================

A data provider for downloading NASA data using 'earthaccess'.
"""
from copy import copy
from pathlib import Path
from typing import List, Optional
import os

import numpy as np
import matplotlib.pyplot as pl

import earthaccess

from pansat.download import accounts
from pansat.file_record import FileRecord
from pansat.geometry import Geometry
from .data_provider import DataProvider
from pansat.products import Product
from pansat.time import TimeRange


PRODUCTS = {
    "satellite.gpm.l2b_corra2022_gpm_dprgmi_v07a": ('GPM_2BCMB', '07'),
    "satellite.gpm.l2b_corra2022_gpm_dprgmi_v07b": ('GPM_2BCMB', '07'),
    "satellite.gpm.l2b_corra2022_gpm_dprgmi_v07c": ('GPM_2BCMB', '07'),
    "satellite.gpm.l2a_slh_dpr_v07a": ('GPM_2HSLH', '07'),
    "satellite.gpm.l2a_slh_dpr_v07b": ('GPM_2HSLH', '07'),
    "satellite.gpm.l2a_slh_dpr_v07c": ('GPM_2HSLH', '07'),
}


def get_url(granule) -> str:
    """
    Get the 'GET DATA' urls from a granule returned from earthaccess.

    Args:
        granule: A granule as returned from earthaccess.search

    Return:
         str: The URL
    """
    urls = granule['umm']['RelatedUrls']
    for url in urls:
        if url["Type"] == 'GET DATA':
            return url["URL"]
    raise ValueError(
        "Did not find a 'GET DATA' URL in granule."
    )


class EarthAccessProvider(DataProvider):

    def __init__(self):
        super().__init__()

    def provides(self, product):
        return product.name in PRODUCTS

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

        user, password = accounts.get_identity("GES DISC")
        os.environ["EARTHDATA_USERNAME"] = user
        os.environ["EARTHDATA_PASSWORD"] = password
        earthaccess.login()

        files = earthaccess.download(
            file_record.remote_path,
            local_path=destination.parent,
        )

        new_rec = copy(file_record)
        new_rec.local_path = destination
        return new_rec


    def find_files(
        self, product: Product, time_range: TimeRange, roi: Optional[Geometry]
    ) -> List[FileRecord]:
        """
        Finds file in using earthacces.
        """
        start = time_range.start.strftime("%Y-%m-%dT%H:%M:%S")
        end = time_range.end.strftime("%Y-%m-%dT%H:%M:%S")
        name, version = PRODUCTS[product.name]

        granules = earthaccess.search_data(
            short_name=name,
            version=version,
            temporal=(start, end),
        )

        recs = []
        for granule in granules:
            url = get_url(granule)
            rec = FileRecord.from_remote(
                product, self, url, url.split("/")[-1],
            )
            recs.append(rec)

        return recs


earthaccess_provider = EarthAccessProvider()
