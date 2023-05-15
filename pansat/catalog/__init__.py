"""
pansat.catalog
==============

The ``catalog`` module provides functionality to organize, parse and
 list local and remote files.
"""
from pathlib import Path

import numpy as np
import xarray as xr

from pansat.time import to_datetime64
from pansat.file_record import FileRecord


class Catalog:
    """
    A catalog to manage collections of files of a given product.
    """

    def __init__(self, path, product):
        self.path = Path(path)
        self.product = product
        self.files = np.array(self._find_files(self.path))

        times = list(map(self.product.filename_to_date, self.files))
        self.times = np.array(list(map(to_datetime64, times)))

        indices = np.argsort(self.times)
        self.files = self.files[indices]
        self.times = self.times[indices]

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
