"""
pansat.environment
==================

This module manages the current pansat environment. This includes
data directories that are used by default to store, cache and
index downloaded data. These can be managed on a user or project
basis and therefore require an additional abstraction layer, which
is provided by this module.
"""
import atexit
import logging
from typing import Optional, Union, List

from pathlib import Path
from pansat.catalog import Catalog, Index
from pansat.file_record import FileRecord
from pansat.granule import Granule


LOGGER = logging.getLogger(__file__)


class Registry(Catalog):
    """
    A registry is a special catalog that keeps track of the data files handled
    by pansat.
    """
    def __init__(
        self,
        name: str,
        path: Path,
        transparent: bool = True,
        parent: Optional["Registry"] = None,
    ):
        if path.is_dir():
            path = path / f"{name}.pansat.db"
        super().__init__(db_path=path)
        self.name = name
        self.transparent = transparent
        self.parent = parent

    def add(self, rec: FileRecord) -> None:
        """
        Adds file record to registry and all registries in the
        hierarchy.

        Args:
            rec: A file recrod pointing to a local file.
        """
        Catalog.add(self, rec)
        if self.transparent and self.parent is not None:
            self.parent.add(rec)

    @property
    def location(self) -> Path:
        """
        The location of the registry.
        """
        return self.path

    def find_local_path(self, rec: FileRecord) -> Optional[Path]:
        """
        Lookup the local path of a given file in the current registry
        hierarchy.

        Args:
            rec: A FileRecord object identifying a given data file.

        Return:
            A 'pathlib.Path' object pointing to the local file or 'None'
            if the file is not present in this catalog.
        """
        found = Catalog.find_local_path(self, rec)
        if found is not None:
            if not found.exists():
                LOGGER.warning(
                    "Found entry for file '%s' in registry '%s' but the "
                    "local path points to a non-existing file.",
                    rec.filename,
                    self.name
                )
                if self.parent is not None:
                    return self.parent.find_local_path(rec)
                return None
            return found
        if self.parent is not None:
            return self.parent.find_local_path(rec)
        return found

    def get_active_data_dir(self) -> Path:
        """
        Find currently active data directory in registry hierarchy.

        Since registries are not data directories, this method just delegates
        to parent of returns the current working directory.

        Return:
            A pathlib.Path object pointing to the folder to use to store
            downloaded files.
        """
        if self.parent is None:
            return Path(".")
        return self.parent.get_active_data_dir()

    def get_index(self, product, recurrent=True) -> Path:
        """
        Find an combine indices for a given product from registry hierarchy.

        Args:
            product: The product for which to find the index.
            recurrent: If 'False', only the index from the first registry
                in the registry hierarchy is returned. If 'True', the indices
                from the hierarchy will be combined.

        Return:
            An index over all currently available product data.
        """
        index = super().get_index(product)
        if not recurrent or self.parent is None:
            return index

        index = index + self.parent.get_index(product)
        return index


class DataDir(Registry):
    """
    A data directory is simply a special registry that is also used as a
    default location to store downloaded files.

    The data directory slightly diverges from the behavior of the registry
    because the folder to store the registry, represented by the DataDir's
    'path' attribute, is located in a hidden subfolder '.pansat' within
    the actual data dir.
    """

    def __init__(
        self,
        name: str,
        path: Path,
        transparent: bool = True,
        parent: Optional[Registry] = None,
    ):
        path = Path(path)
        if not path.exists() or not path.is_dir():
            raise RuntimeError(
                "A data directory must point to an existing folder. The provided "
                f" path '{path}' does not."
            )
        self._location = path
        registry_dir = path / f".{name}.pansat.db"
        registry_dir.mkdir(exist_ok=True)
        super().__init__(name, registry_dir, transparent, parent)

    @property
    def location(self) -> Path:
        """
        The location of the data dir.
        """
        return self._location

    def get_active_data_dir(self) -> Path:
        """
        Find currently active data directory in registry hierarchy.

        This returns the path associated with this data directory.

        Return:
            A pathlib.Path object pointing to the folder to use to store
            downloaded files.
        """
        return self.location


def get_active_registry() -> Registry:
    """
    Get the currently active registry.
    """
    from pansat.config import get_current_config

    config = get_current_config()
    return config.registries[-1]


def get_index(product, recurrent=True) -> Index:
    """
    Retrieve an index containing all locally available files of a given
    product.

    Args:
        product: The product for which to retrieval the index.
        recurrent: If 'True', the index will be calculated by combining
            the indices of the full hierarchy of registries. If 'False',
            only the index from the registry highest in the hierarchy is
            returned.


    """
    return get_active_registry().get_index(product, recurrent=recurrent)


def get_active_data_dir() -> Registry:
    from pansat.config import get_current_config

    config = get_current_config()
    return config.registries[-1].get_active_data_dir()


def register(rec: Union[FileRecord, Granule, List[Granule]]) -> None:
    """
    Register a downloaded file in current registry hierarchy.

    Args:
        rec: A file record, a granule or a list of granules to register
            in the registry.
    """
    reg = get_active_registry()
    reg.add(rec)


def lookup_file(rec: FileRecord) -> Optional[Path]:
    """
    Lookup a file record.

    Args:
        rec:
    """
    reg = get_active_registry()
    return reg.find_local_path(rec)
