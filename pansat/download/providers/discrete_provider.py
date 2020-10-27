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
        if not destination:
            destination = self.product.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)

        files = self.get_files_in_range(start_time, end_time)
        downloaded = []
        for f in files:
            path = destination / f
            self.download_file(f, path)
            downloaded.append(path)
        return downloaded


    def get_preceeding_file(self, filename):
        """
        Return filename of the file that preceeds the given filename in time.

        Args:
            filename(``str``): The name of the file of which to find the
            preceeding one.

        Returns:
            The filename of the file preceeding the file with the given filename
            as ``str``.
        """
        time = self.product.name_to_date(filename)

        year = time.year
        day = int((time.strftime("%j")))
        files = self.get_files_by_day(year, day)

        # If first file of day, return last file of next day.
        i = files.index(filename)
        if i == 0:
            delta = timedelta(days=1)
            time_previous = time - delta
            year = time_previous.year
            day = int((time_previous.strftime("%j")))
            return self.get_files_by_day(year, day)[-1]
        return files[i - 1]

    def get_following_file(self, filename):
        """
        Return filename of the file that follows the given filename in time.

        Args:
            filename(``str``): The name of the file of which to find the following file.

        Returns:
            The filename of the file following the file with the given filename.
        """
        time = self.product.name_to_date(filename)

        year = time.year
        day = int((time.strftime("%j")))
        files = self.get_files_by_day(year, day)

        i = files.index(filename)
        # If last file of day, return first file of next day.
        if i == len(files) - 1:
            delta = timedelta(days=1)
            following_time = time + delta
            year = following_time.year
            day = int((following_time.strftime("%j")))
            return self.get_files_by_day(year, day)[0]

        return files[i + 1]

    def get_files_in_range(self,
                           start_time,
                           end_time,
                           start_inclusive=False):
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
        time = start_time
        files = []

        while (end_time - time).total_seconds() > 0.0:
            year = time.year
            day = int(time.strftime("%j"))
            files_of_day = self.get_files_by_day(year, day)

            def file_filter(filename):
                delta_start = self.product.filename_to_date(filename) - start_time
                delta_end = self.product.filename_to_date(filename) - end_time
                file_within_range = delta_start.total_seconds() > 0
                file_within_range &= delta_end.total_seconds() <= 0
                return file_within_range

            files += filter(file_filter, files_of_day)

            time += delta

        if start_inclusive:
            f_p = self.get_preceeding_file(files[0])
            files.insert(f_p, 0)

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
            return delta <= 0
        files_negative = filter(negative_only, files_sorted)

        if files_negative:
            return next(files_negative)
        return files_sorted[0]
