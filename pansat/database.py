"""
pansat.database
===============

Implements an interface to store and load pansat indices into a SQLite
database
"""
from pathlib import Path
from typing import Dict, List, Optional, Union
from tempfile import TemporaryDirectory
import zlib

from filelock import FileLock
import numpy as np
import pandas as pd
import geopandas
import sqlalchemy
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, LargeBinary, insert, values
from sqlalchemy import create_engine, Engine
from sqlalchemy.sql.expression import (
    insert,
    select,
    and_,
    or_,
    not_
)
import shapely

from pansat.products import Product
from pansat.time import TimeRange, to_datetime
from pansat.granule import Granule


def get_engine(path: Path) -> Engine:
    """
    Get SQL engine to a given local database.

    Path:
        path: The location of the database.

    Return:
        A sqlalchemy engine to connect to the database.
    """
    engine = create_engine(
        f"sqlite:///{path}",
        connect_args={"timeout": 6000.0}
    )
    return engine


def get_table(product: Product, metadata: Optional[MetaData] = None) -> Table:
    """
    Create the table description to store the index for a given product.

    Args:
        product: The pansat Product object whose index to store.
        metadata: A SQLAlchemy meta data object to use for table creation.

    Return:
        A sqlalchemy Table object describing the table used to store the
        index for the given product.
    """
    if metadata is None:
        metadata = MetaData()

    table = Table(
        product.name,
        metadata,
        Column("key", String, primary_key=True, nullable=False),
        Column("start_time", DateTime, nullable=False),
        Column("end_time", DateTime, nullable=False),
        Column("local_path", String, nullable=False),
        Column("remote_path", String, nullable=False),
        Column("filename", String, nullable=False),
        Column("primary_index_name", String, nullable=True),
        Column("primary_index_start", Integer, nullable=True),
        Column("primary_index_end", Integer, nullable=True),
        Column("secondary_index_name", String, nullable=True),
        Column("secondary_index_start", Integer, nullable=True),
        Column("secondary_index_end", Integer, nullable=True),
        Column("geometry", LargeBinary, nullable=True),
    )
    return table


def get_dtypes() -> Dict[str, str]:
    """
    dtype mapping for parsing an index from a database.
    """
    return {
        "key": "string",
        "start_time": "datetime64[ns]",
        "end_time": "datetime64[ns]",
        "local_path": "string",
        "remote_path": "string",
        "filename": "string",
        "primary_index_name": "string",
        "primary_index_start": int,
        "primary_index_end": int,
        "secondary_index_name": str,
        "secondary_index_start": int,
        "secondary_index_end": int,
        "geometry": bytes
    }

def get_table_names(
        path: Path

) -> List[str]:
    """
    List names of tables in database.

    Args:
        path: Path pointing to the database.

    Return:
        A list of strings representing the names of the table in the database.
    """
    metadata = MetaData()
    metadata.reflect(bind=get_engine(path))
    return list(metadata.tables.keys())


class IndexData:
    """
    The index data class manages the data held by an index. The data of each index is stored
    in a separate SQLite database but can be loaded into a geopandas.GeoDataFrame.
    """
    @classmethod
    def from_geodataframe(cls, product: Product, data: geopandas.GeoDataFrame):
        index_data = cls(product, data=data)
        return index_data

    def __init__(
            self,
            product: Product,
            path: Optional[Path] = None,
            data: Optional[pd.DataFrame] = None
    ):
        """
        Args:
            product: The product of which this index holds granule data.
            path: An optional path to a directory into which to store the data. If provided,
                the index data will be stored in an sqlite3 database names '<product_name>.db'
                in the given folder. If not provided, the data is stored in an in-memory
                database.
        """
        self.product = product

        self._data = data

        if path is not None:
            path = Path(path)
            if not path.exists() or not path.is_dir():
                raise RuntimeError(
                    "Path provided to persists index data must point to an existing directory."
                )
            path = (path / (self.product.name + ".db"))

        self.db_path = path
        if self.db_path is None:
            self._tmp = TemporaryDirectory()
            self.db_path = Path(self._tmp.name) / (self.product.name + ".db")

        self.engine = get_engine(self.db_path)
        self._create_table()

    @property
    def time_range(self) -> Optional[TimeRange]:
        """
        The time range covered by the files in the index.
        """
        data = self.load()
        if len(data) == 0:
            return None
        return TimeRange(
            to_datetime(data.start_time.min()),
            to_datetime(data.end_time.max())
        )


    def __add__(self, other):
        data_self = self.load()
        data_other = other.load()
        key_names = ["filename", "primary_index_start", "secondary_index_start"]
        data = pd.concat([data_self, data_other]).drop_duplicates(key_names)
        data = data.sort_values(by="start_time").reset_index(drop=True)
        return IndexData(self.product, data=data)


    def __iadd__(self, other):
        data_self = self.load()
        data_other = other.load()
        data = pd.concat([data_self, data_other]).drop_duplicates()
        data = data.sort_values(by="start_time").reset_index(drop=True)
        self._data = data
        return self


    def _create_table(self):
        """
        Creates the table that will hold the file records.
        """
        if not self.db_path.exists():
            meta = MetaData()
            self.table = get_table(self.product, metadata=meta)
            meta.create_all(self.engine)
        else:
            self.table = get_table(self.product)


    def __len__(self):
        data = self.load()
        return len(data)

    def insert(self, data: Union[Granule, geopandas.GeoDataFrame]):
        """
        Insert granule or granule data into database.
        """
        from pansat.catalog.index import _granules_to_dataframe
        if isinstance(data, Granule):
            data_new = _granules_to_dataframe([data])
        else:
            data_new = data

        if len(data_new) == 0:
            return None

        data = self.load()

        key_names = ["filename", "primary_index_start", "secondary_index_start"]

        if len(data) > 0:
            data_new_keys = data_new[key_names].apply(tuple, 1)
            data_keys = data[key_names].apply(tuple, 1)
            mask = ~data_new_keys.isin(data_keys)
            diff = data_new[mask]
        else:
            diff = data_new

        data_updated = pd.concat([data, diff]).drop_duplicates(key_names)
        data_updated = data_updated.sort_values(by="start_time").reset_index(drop=True)
        self._data = data_updated

        geom = diff["geometry"].apply(
            lambda x: zlib.compress(shapely.wkb.dumps(x))
        )
        diff = pd.DataFrame(
            diff.drop(columns="geometry"),
        )
        diff["geometry"] = geom

        if len(diff) > 0:
            values = diff.to_dict(orient="records")
            for ind, (fname, pstart, sstart)  in enumerate(zip(
                    diff["filename"],
                    diff["primary_index_start"],
                    diff["secondary_index_start"]
            )):
                values[ind]["key"] = f"{fname}_{pstart:06}_{sstart:06}"

            lock = FileLock(self.db_path.with_suffix(".lock"))
            with lock:
                with self.engine.connect() as conn:
                    result = conn.execute(
                        insert(self.table).prefix_with("OR IGNORE", dialect="sqlite"),
                        values
                    )
                    conn.commit()


    def load(self, time_range: Optional[TimeRange] = None) -> geopandas.GeoDataFrame:
        """
        Load granule data from database.

        Args:
            time_range: Optional time-range to constrain the data loaded.
        Return:
            A geopandas.Dataframe containing the granule data.
        """
        if self._data is None:
            expr = select(self.table)

            lock = FileLock(self.db_path.with_suffix(".lock"))
            with lock:
                data = pd.read_sql(expr, self.engine, dtype=get_dtypes())
            data = data.drop(columns=["key"])

            def parse_geo(wkb):
                try:
                    geo = shapely.wkb.loads(wkb)
                    return geo
                except Exception as exc:
                    try:
                        geo = shapely.wkb.loads(zlib.decompress(wkb))
                        return geo
                    except Exception as exc:
                        return None

            data["geometry"] = data["geometry"].apply(parse_geo)
            data = geopandas.GeoDataFrame(
                data,
                geometry="geometry",
            )
            self._data = data

        if time_range is not None:
            outside = self._data["end_time"] < time_range.start
            outside += self._data["start_time"] > time_range.end
            return self._data[~outside]

        return self._data

    def persist(self, path: Path) -> None:
        """
        Stores index data to disk.

        Args:
            path: Path to the database to which to store the index data.
        """
        if not path.exists() or not path.is_dir():
            raise RuntimeError(
                "Path provided to persist index data must point to an existing directory."
            )

        if self._data is None:
            return

        data = self._data
        if len(data) == 0:
            return None

        self._data = None


        if self.db_path == path:
            data_disk = self.load()
        else:
            data_disk = []

        if len(data_disk) > 0:

            key_names = ["filename", "primary_index_start", "secondary_index_start"]
            keys_data = set(data[key_names].apply(tuple))
            keys_data_disk = set(data_disk[key_names].apply(tuple))
            keys_new = keys_data - keys_data_disk

            mask = data[key_names].apply(
                lambda name: tuple(name) in keys_new,
                axis=1
            )
            diff = data[mask]
        else:
            diff = data

        self.db_path = (path / (self.product.name + ".db"))
        self.engine = create_engine(f"sqlite:///{self.db_path}")
        lock = FileLock(self.db_path.with_suffix(".lock"))
        with lock:
            self._create_table()
        self.insert(diff)

    def get_local_path(self, file_record: "FileRecord") -> Union[Path, None]:
        """
        Get local path for a given file record.

        Args:
            file_record: A file record identifying the file whose local
                path to retrieve

        Return:
            A path object pointing to the local path or None if the file
            record isn't present in the database.
        """
        #self.load()
        #if self._data is not None:
        #    inds = self._data.filename == file_record.filename
        #    paths = np.unique(self._data.local_path.loc[inds])

        #    if len(paths) == 0:
        #        return None

        #    if len(paths) > 1:
        #        return ValueError(
        #            "Found more than one path for the given filename. Something "
        #            "seems wrong with this index."
        #        )
        #    return Path(paths[0])

        fname = file_record.filename
        table = self.table
        stmt = select(table).where(table.c.filename == fname)

        with self.engine.connect() as conn:
            res = conn.execute(stmt).first()

        if res is None:
            return res

        local_path = Path(res[3])
        return local_path
