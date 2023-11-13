"""
pansat.catalog
==============

The ``catalog`` module provides functionality to track and retrieve
product data files.
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from filelock import FileLock
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
from pansat.products import Product, GranuleProduct, get_product, all_products
from pansat.geometry import ShapelyGeometry


LOGGER = logging.Logger(__file__)


CATALOGS = {}


class Catalog:
    """
    A catalog manages collections of indices. Each index tracks known
    files of a given pansat product.
    """
    @staticmethod
    def from_existing_files(path, products: Optional[List[Product]] = None):
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

        db_path = path / ".catalog.pansat.db"
        cat = Catalog(db_path, indices=indices)
        return cat

    def __init__(
        self,
        db_path: Optional[Path] = None,
        indices: Optional[Dict[str, Index]] = None
    ):
        """
        Args:
            db_path: If provided, this path should point to a database containing
                previously stored indices. If the path is provided, but no such
                database exists, the database will be created when the catalog's
                save method is called.
            indices: Optional, pre-populated indices. If not provided and path is
                provided, indices will
        """
        self.db_path = db_path
        if db_path is not None:
            self.db_path = Path(db_path)

        self.indices = indices
        if indices is None and self.db_path is not None:
            self.indices = Index.load_indices(self.db_path)

    def save(self) -> None:
        """
        Persist catalog if associated with a directory.
        """
        if self.db_path is None:
            return None

        for index in self.indices.values():
            index.save(self.db_path, append=True)

    def __repr__(self):
        products = ", ".join(self.indices.keys())
        return f"Catalog(db_path='{self.db_path}')"

    def add(self, rec: FileRecord) -> None:
        """
        Add a file record to the catalog.

        Args:
            rec: A file record identifying a product file to add to the
                catalog.
        """
        index = Index.index(rec.product, [rec])
        pname = rec.product.name
        if self.indices is None or pname not in self.indices:
            self.indices = {pname: index}
        else:
            self.indices[pname] = self.indices[pname] + index

        if self.db_path is not None:
            index.save(self.db_path, append=True)

    def get_index(
            self,
            prod: Product,
            time_range: Optional[TimeRange] = None
    ) -> Index:
        """
        Get index for a given product.

        If this catalog has an associated index database, then the
        index is loaded from that database. Otherwise the index
        is returned from the index dictionary of the catalog object.

        Args:
            prod: The products for which to retrieve the index.
            time_range: An optional time range object to restrict
                the returned index to.

        Return:
            The index for the requested product.
        """
        if self.db_path is not None and self.db_path.exists():
            index = Index.load(prod, self.db_path, time_range=time_range)
            if self.indices is None:
                self.indices = {product.name: index}
            else:
                self.indices[prod.name] = index
            return index

        if self.indices is None or prod.name not in self.indices:
            None

        return self.indices[product.name]


    def find_local_path(self, rec: FileRecord) -> Optional[Path]:
        """
        Find the local path of a given file in the current catalog.

        Args:
            rec: A FileRecord object identifying a given data file.

        Return:
            A 'pathlib.Path' object pointing to the local file or 'None'
            if the file is not present in this catalog.

        """
        pname = rec.product.name
        if self.db_path is not None:
            time_range = rec.temporal_coverage
            index = Index.load(rec.product, self.db_path, time_range=time_range)
            return index.find_local_path(rec)

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
