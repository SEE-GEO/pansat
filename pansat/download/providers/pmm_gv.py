"""
pansat.download.providers.pmm_gv
================================

Data provider for GPM ground-validation (GV) data provided by NASA PMM.
"""
from copy import copy
from pathlib import Path
from typing import Optional

import requests

from pansat import cache
from pansat.file_record import FileRecord
from pansat.time import to_datetime
from pansat.download.providers.discrete_provider import DiscreteProviderMonth


BASE_URL = "https://pmm-gv.gsfc.nasa.gov/pub"

PRODUCTS = {
    "ground_based.gpm_gv.1hcf_gpm": "NMQ/level2/GPM/",
    "ground_based.gpm_gv.precip_rate_gpm": "NMQ/level2/GPM/",
    "ground_based.gpm_gv.mask_gpm": "NMQ/level2/GPM/",
    "ground_based.gpm_gv.rqi_gpm": "NMQ/level2/GPM/",
    "ground_based.gpm_gv.1hcf_metopa": "NMQ/level2/METOPA/",
    "ground_based.gpm_gv.precip_rate_metopa": "NMQ/level2/METOPA/",
    "ground_based.gpm_gv.mask_metopa": "NMQ/level2/METOPA/",
    "ground_based.gpm_gv.rqi_metopa": "NMQ/level2/METOPA/",
}


class PMMGVProvider(DiscreteProviderMonth):


    def provides(self, prod: "Product") -> bool:
        """
        Whether or not the provider provides the given product.
        """
        return prod.name in PRODUCTS

    def download_url(self, url: str, destination: Path) -> None:
        """
        Download file from PMM GV server.

        Args:
             url: String containing the URL of the file to download.
            destination: Path object pointint to the file in which to store the downloaded ata.
        """
        with requests.Session() as session:
            response = session.get(url)
            # Write to disk
            with open(destination, "wb") as f:
                for chunk in response:
                    f.write(chunk)


    def find_files_by_month(self, product, time, roi=None):
        """
        Find files available data files for a given month.

        Args:
            product: A 'pansat.Product' object identifying the product
               for which to retrieve available data files.
            time: A time object specifying the day for which to retrieve
               available products.
            roi: An optional geometry object to limit the files to
               only those that cover a certain geographical region.

        Return:
            A list of file records identifying the files from the requested
            month.
        """
        time = to_datetime(time)
        rel_url = PRODUCTS[product.name] + f"{time.year}/{time.month:02}"
        url = BASE_URL + "/" + rel_url

        session = cache.get_session()
        response = session.get(url)

        files = set()
        for match in product.filename_regexp.finditer(response.text):
            files.add(match.group(0))
        recs = [
            FileRecord.from_remote(product, self, url + f"/{fname}", fname)
            for fname in files
        ]
        return recs


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
        self.download_url(url, destination)

        new_rec = copy(rec)
        new_rec.local_path = destination

        return new_rec


pmm_gv_provider = PMMGVProvider()
