"""
pansat.catalog
==============

The ``catalog`` module provides functionality to organize, parse and
 list local and remote files.
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
    A catalog manages collections of data files from different
    products.
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

        cat = Catalog(path, indices=indices)
        return cat

    def __init__(
        self, path: Optional[Path] = None, indices: Optional[Dict[str, Index]] = None
    ):
        """
        Args:
            path: If provided, this path will be used to persist the catalog, when
                the corresponding object is destroyed.
            indices: Optional, pre-populated indices. If not provided and path is
                provided, indices will
        """
        self.path = path
        if path is not None:
            self.path = Path(path)
            if not self.path.exists():
                raise RuntimeError(
                    f"Provided path '{self.path}' does not point to an "
                    " existing directory."
                )

        self.indices = indices
        if indices is None and self.path is not None:
            self.indices = self._load_indices(self.path / ".pansat")

    def _load_indices(self, folder):
        """
        Load all indices found in folder.

        Args:
            folder: The directory to search.

        Return:
            A dictionary matching product names to index object.
        """
        folder = Path(folder)
        index_files = Index.list_index_files(folder)
        indices = {}
        for prod_name, index_file in index_files.items():
            try:
                index = Index.load(index_file)
                indices[prod_name] = index
            except ValueError:
                LOGGER.warning(f"Loading of the index file '%s' failed.", index_file)
        return indices

    def save(self) -> None:
        """
        Persist catalog if associated with a directory.
        """
        if self.path is None:
            return None

        existing = Index.list_index_files(self.path)

        if self.indices is not None:
            for prod_name, index in self.indices.items():
                if prod_name in existing:
                    lock = FileLock(self.path / (prod_name + ".lock"))
                    with lock.acquire(timeout=10):
                        index_ex = Index.load(existing[prod_name])
                        index = index + index_ex
                        index.save(self.path)
                else:
                    index.save(self.path)
=======
        pansat_dir = self.path / ".pansat"
        if not pansat_dir.exists():
            pansat_dir.mkdir()

        existing = Index.list_index_files(pansat_dir)
        for prod_name, index in self.indices.items():

            if prod_name in existing:
                lock = FileLock(pansat_dir / (prod_name + ".lock"))
                with lock.acquire(timeout=10):
                    index_ex = Index.load(existing[prod_name])
                    index = index + index_ex
                    index.save(pansat_dir)
            else:
                index.save(pansat_dir)
>>>>>>> ac0ae54 (Working towards dynamic catalogs.)

    def __repr__(self):
        products = ", ".join(self.indices.keys())
        return f"Catalog(path='{self.path}')"

    def add(self, rec: FileRecord) -> None:
        """
        Add a file record to the catalog.

        Args:
            rec: A file record identifying a product file to add to the
                catalog.
        """
        pname = rec.product.name
        if self.indices is None:
            self.indices = {}
        self.indices.setdefault(pname, Index(rec.product)).insert(rec)

    def get_index(self, prod: Product) -> Index:
        """
        Get index for a given product.

        Args:
            prod: The products for which to retrieve the index.

        Return:
            The index for the requested product.
        """
        return self.indices.get(prod.name, Index(prod))

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
