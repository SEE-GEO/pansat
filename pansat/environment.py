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
from typing import Optional, Union, List

from pathlib import Path
from pansat.catalog import Catalog
from pansat.file_record import FileRecord
from pansat.granule import Granule

ACTIVE_REGISTRIES = []


class Registry(Catalog):
    """
    A registry is a catalog that keeps track of the data files handled
    by pansat.
    """

    def __init__(
        self,
        name: str,
        path: Path,
        transparent: bool = True,
        parent: Optional["Registry"] = None,
    ):
        super().__init__(path=path)
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
        if found is not None or self.parent is None:
            return found
        return self.parent.find_local_path(rec)

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
        registry_dir = path / ".pansat"
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
    from pansat.config import get_current_config

    config = get_current_config()
    return config.registries[-1]


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


def save_registries():
    """
    Save all active registries.
    """
    from pansat.config import get_current_config

    for registry in get_current_config().registries:
        registry.save()


atexit.register(save_registries)
