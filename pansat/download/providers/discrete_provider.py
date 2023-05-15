"""
pansat.download.providers.discrete_provider
===========================================

This module providers the ``DiscreteProvider`` class, which is a base class for
providers where available cannot be determined a priori but need to be looked
up on a per-day basis.
"""
from abc import abstractmethod
from datetime import timedelta
from pathlib import Path
from pansat.download.providers.data_provider import DataProvider
from pansat.time import to_datetime
import numpy as np


class DiscreteProvider(DataProvider):
    """
    The DiscreteProvider class acts as a template class for discrete data
    providers, which lookup files on a per-day basis. It only requires a
    ``get_files_by_day`` and the ``download_file`` function to be implemented
    and the extends the functionality to match the general DataProvider
    interface.
    """

    def __init__(self, product):
        super().__init__()
        self.product = product

    @abstractmethod
    def get_files_by_day(self, year, day):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.
            day(``int``): The Julian day for which to look up the files.

        Return:
            A list of strings containing the filename that are available
            for the given day.
        """

    @abstractmethod
    def download_file(self, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """

    def download(self, start_time, end_time, destination=None):
        """
        This method downloads data for a given time range from respective the
        data provider.

        Args:
            start(``datetime.datetime``): date and time for start
            end(``datetime.datetime``): date and time for end
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        start_time = to_datetime(start_time)
        end_time = to_datetime(end_time)

        if not destination:
            destination = self.product.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)

        files = self.get_files_in_range(start_time, end_time)
        if len(files) == 0:
            files = self.get_files_in_range(start_time, end_time, True)

        downloaded = []
        for f in files:
            path = destination / f
            self.download_file(f, path)
            downloaded.append(path)
        return downloaded

    def get_files_in_range(self, start_time, end_time, start_inclusive=True):
        """
        Get all files within time range.

        Retrieves a list of product files that include the specified
        time range.

        Args:

            start_time(``datetime.datetime``): Start time of the time range
            end_time(``datetime.datetime``): End time of the time range
            start_inclusive(``bool``): Whether or not the list should start with
                 the first file containing ``start_time`` (True) or the first file
                 found with start time later than ``start_time`` (False).

        Returns:

            List of filename that include the specified time range.

        """
        delta = timedelta(days=1)

        year = start_time.year
        day = int(start_time.strftime("%j"))
        files_of_day = self.get_files_by_day(year, day)
        files_of_day = sorted(files_of_day, key=self.product.filename_to_date)
        time_deltas_start = np.array(
            [
                (self.product.filename_to_date(f) - start_time).total_seconds()
                for f in files_of_day
            ]
        )
        if (
            len(time_deltas_start) == 0 or time_deltas_start.min() > 0
        ) and start_inclusive:
            previous_day = start_time - timedelta(days=1)
            year = previous_day.year
            day = int(previous_day.strftime("%j"))
            files = self.get_files_by_day(year, day)
            files_of_day = (
                sorted(files, key=self.product.filename_to_date) + files_of_day
            )

        #
        # Go over days within range an add all included files.
        #

        time = start_time
        files = []

        while (time - end_time).total_seconds() < 24 * 60 * 60:
            if time != start_time:
                year = time.year
                day = int(time.strftime("%j"))
                files_of_day = self.get_files_by_day(year, day)
                files_of_day = sorted(files_of_day, key=self.product.filename_to_date)

            time_deltas_start = np.array(
                [
                    (self.product.filename_to_date(f) - start_time).total_seconds()
                    for f in files_of_day
                ]
            )

            indices = np.where(time_deltas_start >= 0.0)[0]
            if len(indices) > 0:
                start_index = indices[0]
            else:
                start_index = -1

            time_deltas_end = np.array(
                [
                    (self.product.filename_to_date(f) - end_time).total_seconds()
                    for f in files_of_day
                ]
            )
            indices = np.where(time_deltas_end > 0.0)[0]
            if len(indices) > 0:
                end_index = indices[0]
            else:
                end_index = None

            if start_index > 0 and start_inclusive:
                start_index -= 1

            files += files_of_day[start_index:end_index]
            time += delta

        return files

    def get_file_by_date(self, time):
        """
        Get file with start time closest to a given date.

        Args:
            time(``datetime.datetime``): The date to download a file for.

        Return:
            The filename of the file with the closest start time
            before the given time.
        """
        # Check last file from previous day
        delta = timedelta(days=1)
        previous_day = time - delta
        year = previous_day.year
        day = int((previous_day.strftime("%j")))
        files = self.get_files_by_day(year, day)[-1:]

        year = time.year
        day = int(time.strftime("%j"))
        files += self.get_files_by_day(year, day)

        def time_difference(filename):
            delta = self.product.filename_to_date(filename) - time
            return delta.total_seconds()

        files_sorted = sorted(files, key=time_difference)

        def negative_only(filename):
            delta = self.product.filename_to_date(filename) - time
            return delta.total_seconds() <= 0

        files_negative = filter(negative_only, files_sorted)

        if files_negative:
            return list(files_negative)[-1]
        return files_sorted[0]
