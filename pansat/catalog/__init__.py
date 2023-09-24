"""
pansat.catalog
==============

The ``catalog`` module provides functionality to organize, parse and
 list local and remote files.
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import Manager, TimeoutError
from pathlib import Path
import queue

import numpy as np
import xarray as xr
import geopandas
import rich.progress

from pansat.time import TimeRange, to_datetime64
from pansat.file_record import FileRecord
from pansat.catalog.index import Index
from pansat.granule import Granule, merge_granules
from pansat.products import Product, GranuleProduct, get_product
from pansat.geometry import ShapelyGeometry


class Catalog:
    """
    A catalog manages collections of data files from different
    products.
    """

    def __init__(self, path):
        self.path = Path(path)
        self.indices = self._load_indices(self.path)

    def _load_indices(self, folder):
        """
        Load all indices found in folder.

        Args:
            folder: The directory to search.

        Return:
            A dictionary matching product names to index object.
        """
        folder = Path(folder)
        files = sorted(list(folder.glob("*.idx")))
        indices = {}
        for path in files:
            try:
                index = Index.load(path)
                indices[index.product.name] = index
            except ValueError:
                LOGGER.warning(f"Loading of the index file '%s' failed.", path)
        return indices

    def __repr__(self):
        products = ", ".join(self.indices.keys())
        return f"Catalog(path='{self.path}')"

    def find_local_path(self, rec: FileRecord):
        """
        Find the local path of a given file in the current catalog.

        Args:
            rec: A FileRecord object identifying a given data file.

        Return:
            A 'pathlib.Path' object pointing to the local file or 'None'
            if the file is not present in this catalog.

        """
        pname = rec.product.name
        if not pname in self.indices:
            return None
        index = self.indices[pname]
        return index.find_local_path(rec)


def find_files(product: "pansat.products.Prodcut", path: Path):
    """
    Find files of a given product.

    Args:
        product: A pansat product representing the product to find.
        path: A 'pathlib.Path' object pointing to a folder containing
            local files.

    Return:
        A list of file records pointing to the found files.
    """
    files = []
    for file_path in sorted(list(Path(path).glob("**/*"))):
        if product.matches(file_path.name):
            files.append(FileRecord(product, file_path))
    return files
