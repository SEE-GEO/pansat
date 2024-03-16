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
import os
from tempfile import TemporaryDirectory
from typing import Optional, Union, List

from pathlib import Path
from pansat.catalog import Catalog, Index
from pansat.file_record import FileRecord
from pansat.granule import Granule


LOGGER = logging.getLogger(__file__)


def keep_files():
    """
    Determine whether or not downloaded files should be kept.
    """
    if "PANSAT_ON_THE_FLY" in os.environ:
        return False
    return True



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
        self.path = path
        super().__init__(db_path=self.path)
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
        return self.db_path

    def get_local_path(self, rec: FileRecord) -> Optional[Path]:
        """
        Lookup the local path of a given file in the current registry
        hierarchy.

        Args:
            rec: A FileRecord object identifying a given data file.

        Return:
            A 'pathlib.Path' object pointing to the local file or 'None'
            if the file is not present in this catalog.
        """
        found = Catalog.get_local_path(self, rec)
        if found is not None:
            if not found.exists():
                LOGGER.warning(
                    "Found entry for file '%s' in registry '%s' but the "
                    "local path points to a non-existing file.",
                    rec.filename,
                    self.name
                )
                if self.parent is not None:
                    return self.parent.get_local_path(rec)
                return None
            return found
        if self.parent is not None:
            return self.parent.get_local_path(rec)
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

    def print_summary(self, verbosity:int = 0) -> str:
        """
        Print summary of registry contents.
        """
        s = ""
        if self.parent is not None:
            s = self.parent.print_summary(verbosity=verbosity)

        s += self.name + ":\n"
        for name, index in self.indices.items():
            s += "\t" + name + f": {len(index)} granules\n"
        s += "\n"
        return s


class DataDir(Registry):
    """
    A data directory is  a special registry that is also used as a
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
        registry_dir = path / f".pansat_catalog"
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


class OnTheFlyDataDir(DataDir):
    """
    The OnTheFlyDataDir stores data in a temporary directory. File downloaded
    to this temporary directory are indexed in a separate registry but are not
    propagated upwards. Downloaded fiels can be cleaned up using the
    'pansat.environment.cleanup' function.
    """
    def __init__(
        self,
        parent: Optional[Registry] = None,
    ):
        self.tmp = TemporaryDirectory()
        super().__init__(
            name="on_the_fly",
            path=Path(self.tmp.name),
            transparent=False,
            parent=parent
        )

    def cleanup(self):
        """
        Delete all temporary files.
        """
        if self.tmp is not None:
            self.tmp.cleanup()
            self.tmp = None


ON_THE_FLY_DATA_DIR = None


def cleanup() -> None:
    """
    Remove temporary files if they were stored.
    """
    global ON_THE_FLY_DATA_DIR
    if ON_THE_FLY_DATA_DIR is not None:
        ON_THE_FLY_DATA_DIR.cleanup()
        ON_THE_FLY_DATA_DIR = None


def get_active_registry() -> Registry:
    """
    Get the currently active registry.

    The currently active registry will be the either the innermost presistent
    registry or the currently active on-the-fly registry if the 'PANSAT_ON_THE_FLY'
    environment variable is set.
    """
    from pansat.config import get_current_config
    global ON_THE_FLY_DATA_DIR

    config = get_current_config()
    persistent_reg =  config.registries[-1]

    if "PANSAT_ON_THE_FLY" in os.environ:
        if ON_THE_FLY_DATA_DIR is None:
            config = get_current_config()
            ON_THE_FLY_DATA_DIR = OnTheFlyDataDir(
                persistent_reg
            )
        return ON_THE_FLY_DATA_DIR

    return persistent_reg


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


def get_active_data_dir() -> DataDir:
    """
    Get the currently active data directory.
    """
    reg = get_active_registry()
    return reg.get_active_data_dir()


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
    return reg.get_local_path(rec)
