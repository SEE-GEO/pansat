"""
pansat.catalog
==============

The ``catalog`` module provides functionality to organize, parse and
 list local and remote files.
"""
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import xarray as xr
import geopandas

from pansat.time import TimeRange, to_datetime64
from pansat.file_record import FileRecord
from pansat.products import Product, Granule, GranuleProduct



def _get_index_data(product, path):
    """
    Extracts index data from a single path.

    Args:
        product: A 'pansat.Product' object representing the data product
            that is being indexed.
        path: A path pointing to a data file of that product.

    Return:
        A tuple ``(start_time, end_time, local_path, geom)`` containing
        the start and end time, the path of the file as sting and
        ``geom`` a geometry representing the spatial coverage of the file.
    """
    if product.matches(path.name) is None:
        return []

    rec = FileRecord(path)

    if isinstance(product, GranuleProduct):
        return product.get_granules(rec)

    start_time, end_time = product.get_temporal_coverage(rec)
    geom = product.get_spatial_coverage(rec).to_shapely()
    local_path = str(path)

    return [Granule(rec, TimeRange(start_time, end_time), geom)]


def _pandas_to_file_record(
        product,
        data
):
    """
    Converts a pandas dataframe of file indices into a list
    of file records.

    Args:
        product: The 'pansat.Product' object representing the
            product.
        data: A pandas dataframe containing a selection of
            data file indices.

    Return:
        A list of file records pointing to the files in 'data'.
    """
    recs = []

    if "local_path" in data.columns:
        for row in data.itertuples():
            local_path = row.local_path
            recs.append(FileRecord(local_path, product))
    elif "remote_path" in data.columns:
        for row in data.itertuples():
            remote_path = row.remote_path
            filename = row.filename
            recs.append(FileRecord.from_remote(
                product,
                None,
                remote_path,
                filename
            ))
    else:
        raise ValueError(
            "Index data must provide either a 'local_path' column or "
            " a 'remote_path' column."
        )
    return recs


class Index:
    """
    A index keeps track of data files of specific product.
    """
    @classmethod
    def load(cls, path):
        """
        Load an index.

        Args:
            path: Path to an Apache parquet file containing the index.

        Return:
            The loaded index.
        """
        data = geopandas.read_parquet(path)
        product_name = path.stem
        product = Product.get_product(product_name)
        return cls(product, data)


    @classmethod
    def index(cls, product, files, n_processes=None):
        """
        Index data files.

        Args:
            product: The pansat product to index.
            files: A list of Path objects pointing to the data files to
                index.

        Return:
           An Index object containing an index of all files.
        """
        geoms = []
        start_times = []
        end_times = []
        local_paths = []

        if n_processes is None:
            for path in files:

                index_data = _get_index_data(product, path)

                for granule in index_data:
                    start_times.append(granule.time_range.start)
                    end_times.append(granule.time_range.end)
                    local_paths.append(str(granule.file_record.local_path))
                    geoms.append(granule.geometry)
        else:
            pool = ProcessPoolExecutor(
                max_workers=n_processes
            )
            tasks = []
            for path in files:
                tasks.append(pool.submit(_get_index_data, product, path))

            for task in as_completed(tasks):
                index_data = task.result()
                if index_data is None:
                    continue
                start_time, end_time, local_path, geom = index_data
                print(local_path)
                start_times.append(start_time)
                end_times.append(end_time)
                local_paths.append(str(path))
                geoms.append(geom)

        data = geopandas.GeoDataFrame(
            data={
                "start_time": start_times,
                "end_time": end_times,
                "local_path": local_paths
            },
            geometry=geoms
        )
        return cls(product, data)

    def __init__(self, product, data):
        """
        Args:
            product: The pansat product whose data files are indexed by
                this index.
            data: A geopandas.GeoDataFrame containing the start and end time
                and geographical coverage of all data files.
        """
        self.product = product
        self.data = data


    def __add__(self, other):
        """Merge two indices."""
        if not self.product == other.product:
            raise ValueError(
                "Combining to Index object requires them to refer to the"
                " same product."
            )
        product = self.product
        data = pd.merge([self.data, other.data], how="outer")
        return Index(product, data)

    def __repr__(self):
        return (
            f"<Index of '{self.product.name}' containing "
            f"{self.data.start_time.size} entries>"
        )



    def find_files(self, time_range=None, roi=None):
        """
        Find entries in Index within given time range and location.
        """
        if time_range is None and roi is None:
            return _pandas_to_file_record(
                self.product,
                self.data
            )

        if time_range is None:
            selected = self.data
        else:
            if not isinstance(time_range, TimeRange):
                time_range = TimeRange(time_range, time_range)
            selected = self.data.loc[
                (self.data.start_time <= time_range.end) *
                (self.data.end_time >= time_range.start)
            ]

        if roi is None:
            return _pandas_to_file_record(
                self.product,
                selected
            )

        roi = roi.to_shapely()
        indices = selected.intersects(roi)

        return _pandas_to_file_record(
            self.product,
            selected.loc[indices]
        )

    def save(self, path):
        """
        Save an index.

        Args:
            path: Path to a directory to which to save the index.

        Return:
            A 'Path' object pointing to the saved index.
        """
        if not path.is_dir():
            raise ValueError(
                "'path' must point to a directory."
            )
        output_file = path / f"{self.product.name}.idx"
        data = self.data.to_parquet(output_file)
        return output_file










        


class Catalog:
    """
    A catalog manages collections of data files from different
    products.
    """
    def __init__(self, path):
        self.path = Path(path)
        self.indices = {}

    def _find_files(self, folder):
        """
        Recursively search for files in folder that match the
        product regexp.

        Args:
            folder: The directory to search.

        Return:
            A list of the filename that match the regexp of the
            product.
        """
        folder = Path(folder)
        files = []
        for path in folder.iterdir():
            if path.is_dir():
                files += self._find_files(path)
            else:
                if self.product.filename_regexp.match(path.name):
                    files.append(path)
        return files

    def find_file_covering(self, time):
        """
        Find file covering a given date.
        """
        time = to_datetime64(time)
        start_times = list(map(self.product.filename_to_start_time, self.files))
        start_times = np.array(list(map(to_datetime64, start_times)))
        end_times = list(map(self.product.filename_to_end_time, self.files))
        end_times = np.array(list(map(to_datetime64, end_times)))
        candidates = np.where((start_times <= time) * (end_times >= time))[0]
        return self.files[candidates]

    def load(self, start_time, end_time, dimension="time", load_callback=None):
        """
        Load data from files within a given time range.

        Args:
            start_time: Start time of the interval for which to load the data.
            end_time: End time of the interval for which to load the data.

        Return:
            An ``xarray.Dataset`` containing the loaded data.
        """
        start_time = to_datetime64(start_time)
        end_time = to_datetime64(end_time)
        indices = np.where((self.times >= start_time) * (self.times <= end_time))[0]
        datasets = []

        if load_callback is None:
            load_callback = self.product.open

        for ind in indices:
            dataset = load_callback(self.files[ind])
            if dimension not in dataset.dims:
                dataset = dataset.expand_dims("time")
                if dimension == "time":
                    filename = self.files[ind]
                    time = to_datetime64(self.product.filename_to_date(filename))
                    dataset["time"] = [time]

            datasets.append(dataset)

        return xr.concat(datasets, dim=dimension)



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
