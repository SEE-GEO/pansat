"""
pansat.database
===============

Implements an interface to store and load pansat indices into a SQLite
database
"""
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import geopandas
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, LargeBinary
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
from pansat.time import TimeRange


def get_engine(path: Path) -> Engine:
    """
    Get SQL engine to a given local database.

    Path:
        path: The location of the database.

    Return:
        A sqlalchemy engine to connect to the database.
    """
    engine = create_engine(f"sqlite:///{path}")
    return engine


def get_table(product: Product) -> Table:
    """
    Create the table description to store the index for a given product.

    Args:
        product: The pansat Product object whose index to store.

    Return:
        A sqlalchemy Table object describing the table used to store the
        index for the given product.
    """
    metadata = MetaData()
    table = Table(
        product.name,
        metadata,
        Column("start_time", DateTime, nullable=False),
        Column("end_time", DateTime, nullable=False),
        Column("local_path", String, nullable=False),
        Column("remote_path", String, nullable=False),
        Column("filename", String, primary_key=True, nullable=False),
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


def save_index_data(
        product: Product,
        data: geopandas.GeoDataFrame,
        path: Path,
        append: bool = False
) -> None:
    """
    Save index data to a database.

    Args:
        product: The pansat product associated with the index data.
        data: The GeoDataFrame containing the index data to save.
        path: The path of the database to which to save the index.
        append: Whether or not the index data should be appended to
            index data that already exists in the database.
    """
    engine = get_engine(path)
    data = pd.DataFrame(data)
    data["geometry"] = data["geometry"].apply(shapely.wkb.dumps)
    if append:
        if_exists="append"
    else:
        if_exists="fail"
    data.to_sql(
        product.name,
        engine,
        if_exists=if_exists,
        index=False,
        index_label="filename"
    )


def load_index_data(
        product: Product,
        path: Path,
        time_range: TimeRange=None
) -> geopandas.GeoDataFrame:
    """
    Load the index for a given product from a database.

    Args:
        path: The path of the database from which to load the index.
        product: Product the pansat product for which to load the index.
        time_range: An optional TimeRange object specifying a time range
            to constain the granules in the index.

    Return:
        An index object containing the granules of the given product.
    """
    table = get_table(product)
    engine = get_engine(path)
    expr = select(table)
    if time_range is not None:
        expr = expr.where(
            not_(or_(
                (table.c.start_time > time_range.end),
                (table.c.end_time < time_range.start)
            )
                 )
        )
    data = pd.read_sql(expr, engine, dtype=get_dtypes())
    data["geometry"] = data["geometry"].apply(shapely.wkb.loads)

    data = geopandas.GeoDataFrame(
        data,
        geometry="geometry",
    )
    return data


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
