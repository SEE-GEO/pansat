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
from typing import Dict, List, Optional, Union
import queue

import numpy as np
import xarray as xr
import geopandas
import rich
import rich.progress
import rich.text

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
    def from_existing_files(
            path: Union[Path, List[Path]],
            products: Optional[List[Product]] = None,
            n_processes: Optional[int] = None,
            recursive: bool = True,
            pattern: Optional[str] = None,
            relative: bool = False
    ):
        """
        Create a catalog by scanning existing files.

        Args:
            path: Path pointing to the root of the directory tree within
                which to search for available product files.
            products: List of products to consider. If not provided all
                currently known products will be consdiered.
                NOTE: This can be slow.
            recursive: Whether to search recursively for candidate files or
                not.
            relative: Whether or not to use relative paths in the index.

        Return:
            A catalog object providing an overview of available pansat
            product files.
        """
        if products is None:
            LOGGER.warning(
                "No list of products provided to 'Catalog.from_existing_files', "
                "which will cause all currently known products to be "
                " considered. This may be slow."
            )
            products = list(all_products())

        if pattern is None:
            pattern = "*"

        if isinstance(path, str):
            path = Path(path)
        if not isinstance(path, list):
            paths = [path]
        else:
            paths = path

        files = []
        for path in paths:
            if recursive:
                files = sorted(list(path.glob(f"**/{pattern}")))
            else:
                files = sorted(list(path.glob(pattern)))

        if not relative:
            files = [path.absolute() for path in files]
        files = np.array(files)

        indices = {}

        for prod in products:
            matching = np.array(list(map(prod.matches, files)))
            LOGGER.warning(
                "Found %s files matching product %s.",
                matching.sum(),
                prod.name

            )
            if len(matching) == 0 or not matching.any():
                continue

            files_p = files[matching]
            files = files[~matching]
            indices[prod.name] = Index.index(prod, files_p, n_processes=n_processes)

        cat = Catalog(db_path=None, indices=indices)
        return cat

    def __init__(
        self,
        db_path: Optional[Path] = None,
        indices: Optional[Dict[str, Index]] = None
    ):
        """
        Args:
            db_path: If provided, this path should point to directory containing
                previously stored indices. If the path is provided, but no such
                database exists, the database will be created when the catalog's
                save method is called.
            indices: Optional, pre-populated indices. If not provided and path is
                provided, indices will
        """
        self.db_path = db_path
        if db_path is not None:
            self.db_path = Path(db_path)
            if not self.db_path.is_dir():
                raise RuntimeError(
                    "Path for storing catalog must be a directory."
                )
        self.indices = indices
        if indices is None and self.db_path is not None:
            self.indices = Index.load_indices(self.db_path)

    def save(self, keys: Optional[List[str]] = None) -> None:
        """
        Persist catalog if associated with a directory.
        """
        if self.db_path is None:
            return None

        if not self.db_path.exists():
            self.db_path.mkdir()

        if keys is None:
            keys = list(self.indices.keys())

        for key in keys:
            self.indices[key].save(self.db_path, append=True)

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
        pname = rec.product.name
        if self.indices is None:
            self.indices = {pname: Index(rec.product, [rec], db_path=self.db_path)}
        else:
            if pname not in self.indices:
                self.indices[pname] = Index(rec.product, [rec], db_path=self.db_path)
            else:
                index = self.indices[pname]
                index.insert(rec)


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
        if self.indices is None or prod.name not in self.indices:
            return Index(prod, db_path=self.db_path)
        return self.indices[prod.name]


    def get_local_path(self, rec: FileRecord) -> Optional[Path]:
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
        return index.get_local_path(rec)

    def to_table(self) -> rich.table.Table:
        """
        Render catalog summary as rich table.
        """
        table = rich.table.Table(
            title=rich.text.Text(
                f"🗂️ ",
                justify="left"
            ).append(rich.text.Text(
                f" {self.name}",
                style="bold"
            )).append(
                f" ({self.db_path})"
            )
        )
        table.add_column("Product")
        table.add_column("# entries")
        table.add_column("Start time")
        table.add_column("End time")
        for index_name, index in self.indices.items():
            time_range = index.time_range
            if time_range is not None:
                start = time_range.start.strftime("%Y-%m-%d %H:%M:%S")
                end = time_range.end.strftime("%Y-%m-%d %H:%M:%S")
            else:
                start = ""
                end = ""
            table.add_row(index_name, str(len(index)), start, end)
        return table



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
