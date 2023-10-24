"""
pansat.catalog
==============

The ``catalog`` module provides functionality to organize, parse and
 list local and remote files.
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from multiprocessing import Manager, TimeoutError
from pathlib import Path
from typing import Optional, List, Dict
import queue

import numpy as np
import xarray as xr
import geopandas
import rich.progress

from pansat.time import TimeRange, to_datetime64
from pansat.file_record import FileRecord
from pansat.catalog.index import Index
from pansat.granule import Granule, merge_granules
from pansat.products import (
    Product,
    GranuleProduct,
    get_product,
    all_products
)
from pansat.geometry import ShapelyGeometry


LOGGER = logging.Logger(__file__)


class Catalog:
    """
    A catalog manages collections of data files from different
    products.
    """
    @staticmethod
    def from_existing_files(
            path,
            products: Optional[List[Product]] = None
    ):
        """
        Create a catalog by scanning existing files.

        Args:
            path: Path pointing to the root of the directory tree within
                which to search for available product files.
            products: List of products to consider. If not provided all
                currently known products will be consdiered.
                NOTE: This can be slow.

        Return:
            A catalog object providing an overview of available pansat
            product files.
        """
        if products is None:
            LOGGER.warning(
                "No list of product provided to Catalog.from_existing_files, "
                "which will cause all currently known products to be "
                " considered. This may be slow."
            )
            products = list(all_products())
        path = Path(path)

        files = np.array(sorted(list(path.glob("**/*"))))

        indices = {}

        for prod in products:
            matching = np.array(list(map(prod.matches, files)))
            files_p = files[matching]
            if files_p.size == 0:
                continue
            files = files[~matching]
            indices[prod.name] = Index.index(prod, files_p)

        cat = Catalog(path, indices=indices)
        return cat

    def __init__(
            self,
            path: Path,
            indices: Optional[Dict[str, Index]] = None
    ):
        self.path = Path(path)

        self.indices = indices
        if indices is None:
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
