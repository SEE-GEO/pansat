"""
pansat.download.providers.example
=================================

Implements an example provider for illustrative and testing purposes.
"""
from pathlib import Path
import shutil
from typing import Optional

from pansat.download.providers.data_provider import DataProvider
from pansat.products import Product
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.geometry import Geometry


class ExampleProvider(DataProvider):
    """
    A data provider for the example product defined in
    'pansat.products.example'.

    This example provider assumes that all available product data
    files a in a single directory the path to which is provided
    to the provider at construction.
    """

    def __init__(self, data_dir, fmt):
        """
        Args:
            data_dir: Path to the folder containing the example product
                files provided by this provider.
            fmt: "hdf4" or "hdf5" for the example.hdf4_product and
                example hdf5_product, respectively.
        """
        self.data_dir = Path(data_dir)
        self.fmt = fmt
        super().__init__()

    @classmethod
    def get_available_products(cls):
        return []

    def provides(self, product):
        """
        Does the provider provide the product?
        """
        name = product.name
        print(name)
        return name.startswith("example") and name.split(".")[1].startswith(self.fmt)

    def find_files(
            self, product: Product,
            time_range: TimeRange,
            roi: Optional[Geometry] = None
    ):
        """
        Find all product files within a given time range.

        Args:
            product: A 'pansat.Product' object identifying the product
                whose file to find.
            time_range: A 'TimeRange' object identifying a time range
                to which to limit the search.
            roi: A Geometry object identifying a region of interest (ROI)
                to which to limit the search.

        Return:
            A list of FileRecord object identifying the found files.
        """
        all_recs = map(
            lambda path: FileRecord.from_remote(product, self, path, path.name),
            sorted(list(self.data_dir.glob(f"**/*"))),
        )
        found_recs = []
        for rec in all_recs:
            try:
                rng = product.get_temporal_coverage(rec)
            except ValueError:
                continue

            if time_range.covers(time_range):
                if roi is not None:
                    geo = product.get_spatial_coverage(rec)
                    if geo.covers(roi):
                        found_recs.append(rec)
                else:
                    found_recs.append(rec)

        return found_recs

    def download(
            self,
            file_record: FileRecord,
            destination: Optional[Path]
    ) -> FileRecord:
        """
        Download a file.

        Args:
            file_record: A FileRecord object identifying the file to download.
            destination: Local path pointing to a file or directory to
                which to copy the remote file.

        Rerturn:
            A 'Path' object pointing to the downloaded file.
        """
        if destination is None:
            destination = Path(".") / file_record.product.default_destination

        destination = Path(destination)
        if destination.is_dir():
            destination = destination / file_record.filename

        shutil.copy(file_record.remote_path, destination)
        file_record.local_path = destination
        return file_record
