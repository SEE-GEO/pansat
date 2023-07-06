"""
pansat.catalog
==============

The ``catalog`` module provides functionality to organize, parse and
 list local and remote files.
"""
from pathlib import Path

import numpy as np
import xarray as xr
import geopandas

from pansat.time import to_datetime64
from pansat.file_record import FileRecord


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
        parts = product_name.split(".")
        module_name = "pansat.products." + ".".join(parts[:-1])
        module = importlib.import_module(module_name)
        product = getattr(module, parts[-1])
        return cls(product, data)


    @classmethod
    def index(self, product, files):
        """
        Index data files.

        Args:
            product: The pansat product to index.
            files: A list of Path objects pointing to the data files to
                index.

        Return:
           An Index object containing an index of all files.
        """
        self.product = product

        geoms = []
        start_times = []
        end_times = []
        local_paths = []

        for path in files:
            if product.matches(path.name) is None:
                continue

            rec = FileRecord(path)
            start_time, end_time = product.get_temporal_coverage(rec)
            start_times.append(start_time)
            end_times.append(end_time)
            geoms.append(product.get_spatial_coverage(rec).to_shapely())
            local_paths.append(str(path))

        data = geopandas.GeoDataFrame(
            data={
                "start_time": start_times,
                "end_time": end_times,
                "local_paths": local_paths
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


    def find(self, time_range=None, location=None):

        if time_range is None:
            selected = self.data
        else:
            if not isinstance(time_range, TimeRange):
                time_range = TimeRange(time_range, time_range)
            selected = (
                (self.data.start_time <= time_range.end_time) *
                (self.data.end_time >= time_range.start_time)
            )

        if location is None:
            return selected

        roi = location.to_shapely()
        indices = selected.intersects(roi)

        return selected.loc(indices)











        


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
