"""
pansat.download.providers.ncar_stage4
=====================================

Provider to download Stage IV files from the NCAR server.
"""
from copy import copy
from datetime import datetime, timedelta
from urllib.request import urlopen
from pathlib import Path
from typing import List, Union, Optional

from bs4 import BeautifulSoup
import requests
import numpy as np

from pansat import cache, FileRecord, Geometry, TimeRange
from pansat.download.providers.data_provider import DataProvider


BASE_URL = "https://ncar-cache.nationalresearchplatform.org:8443/"


def get_monthly_tar_urls() -> List[str]:
    """
    Retrieve URLS of available StageIV files.
    """
    resp = requests.get(BASE_URL + "/ncar/rda/d507005/stage4", verify=False)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".tar"):
            full = f"{BASE_URL}/{href}"
            urls.append(full)
    return sorted(urls)


class NCARStageIVProvider(DataProvider):
    """
    Data provider for Stage IV data available from data.eol.ucar.edu.
    """
    def provides(self, product: "pansat.Product") -> bool:
        """
        Returns True for StageIV products.
        """
        return product.name.startswith("ground_based.stage4")

    def download(
        self, rec: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
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

        url = rec.remote_path

        response = requests.get(rec.remote_path, stream=True)
        response.raise_for_status()

        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec

    def find_files(
        self, product: "pansat.Product", time_range: TimeRange, roi: Optional[Geometry]
    ) -> List[FileRecord]:
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
        urls = get_monthly_tar_urls()
        recs = []
        for url in urls:
            fname = url.split("/")[-1]
            if product.matches(fname):
                rec = FileRecord.from_remote(
                    product,
                    self,
                    remote_path=url,
                    filename=fname
                )
                if rec.temporal_coverage.covers(time_range):
                    if roi is None or roi.covers(rec.spatial_coverage):
                        recs.append(rec)
        return recs


stage4_provider = NCARStageIVProvider()
