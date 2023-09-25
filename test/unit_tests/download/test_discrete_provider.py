"""
This file contains tests for the discrete provider base class.
"""
from calendar import monthrange
from datetime import datetime, timedelta
import os
from pathlib import Path

import pytest

from pansat.download.providers import IcareProvider
from pansat.products.satellite.modis import modis_terra_1km
from pansat.products.satellite.dardar import dardar_cloud
from pansat.products.example import get_filename, hdf4_product
from pansat.time import TimeRange, to_datetime
from pansat.file_record import FileRecord
from pansat.download.providers.discrete_provider import (
    DiscreteProviderDay,
    DiscreteProviderMonth,
    DiscreteProviderYear,
)


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


class TestProviderDay(DiscreteProviderDay):
    """
    Dummy provider to test the DiscreteProvider functionality.
    """

    def find_files_by_day(self, product, time):
        """
        Return list of hourly filenames.

        Args:
            time: An arbitrary time within the day for which to return
                the available files.

        Return:
            A list of file records pointing to the available files.
        """
        time = to_datetime(time)
        start = datetime(time.year, time.month, time.day)
        end = start + timedelta(days=1)
        time = start

        recs = []

        while time < end:
            filename = get_filename(time, time + timedelta(hours=1), suffix="hdf")
            recs.append(
                FileRecord.from_remote(
                    product, self, Path("remote") / filename, filename
                )
            )
            time += timedelta(hours=1)
        return recs

    def download_file(self):
        pass


class TestProviderMonth(DiscreteProviderMonth):
    def find_files_by_month(self, product, time):
        """
        Return list of daily files within a month.

        Args:
            time: An arbitrary time within the month for which to return
                the available files.

        Return:
            A list of file records pointing to the available files.
        """
        time = to_datetime(time)
        start = datetime(time.year, time.month, 1)
        days = monthrange(time.year, time.month)[-1]
        end = start + timedelta(days=days)
        time = start

        recs = []

        while time < end:
            filename = get_filename(time, time + timedelta(days=1), suffix="hdf")
            print(filename)
            recs.append(
                FileRecord.from_remote(
                    product, self, Path("remote") / filename, filename
                )
            )
            time += timedelta(days=1)
        return recs

    def download_file(self):
        pass

    def get_available_products(self):
        return []


class TestProviderYear(DiscreteProviderYear):
    def find_files_by_year(self, product, time):
        """
        Return list of daily files within a year.

        Args:
            time: An arbitrary time within the year for which to return
                the available files.

        Return:
            A list of file records pointing to the available files.
        """
        time = to_datetime(time)
        start = datetime(time.year, 1, 1)
        end = datetime(time.year + 1, 1, 1)
        time = start

        recs = []

        while time < end:
            dom = monthrange(time.year, time.month)[-1]
            filename = get_filename(time, time + timedelta(days=dom), suffix="hdf")
            recs.append(
                FileRecord.from_remote(
                    product, self, Path("remote") / filename, filename
                )
            )
            time += timedelta(days=dom)
        return recs

    def download_file(self):
        pass

    def get_available_products(self):
        return []


def test_files_in_range_day():
    """
    Test discrete provider functionality for files organized by
    day.
    """
    provider = TestProviderDay()
    t_0 = datetime(2018, 1, 14, 0, 42)
    t_1 = datetime(2018, 1, 14, 0, 42)
    t_range = TimeRange(t_0, t_1)

    files = provider.find_files(hdf4_product, t_range)
    assert len(files) == 1

    t_0 = datetime(2018, 1, 14, 23, 55)
    t_1 = datetime(2018, 1, 15, 0, 5)
    t_range = TimeRange(t_0, t_1)

    files = provider.find_files(hdf4_product, t_range)
    assert len(files) == 2


def test_files_in_range_month():
    """
    Test discrete provider functionality for files organized by
    months.
    """
    provider = TestProviderMonth()
    t_0 = datetime(2018, 1, 14, 0, 42)
    t_1 = datetime(2018, 1, 14, 0, 42)
    t_range = TimeRange(t_0, t_1)

    files = provider.find_files(hdf4_product, t_range)
    assert len(files) == 1

    t_0 = datetime(2018, 1, 14, 23, 55)
    t_1 = datetime(2018, 1, 15, 0, 5)
    t_range = TimeRange(t_0, t_1)

    files = provider.find_files(hdf4_product, t_range)
    assert len(files) == 2


def test_files_in_range_year():
    """
    Test discrete provider functionality for files organized by
    months.
    """
    provider = TestProviderYear()
    t_0 = datetime(2018, 1, 14, 0, 42)
    t_1 = datetime(2018, 1, 14, 0, 42)
    t_range = TimeRange(t_0, t_1)

    files = provider.find_files(hdf4_product, t_range)
    assert len(files) == 1

    t_0 = datetime(2018, 1, 31, 23, 55)
    t_1 = datetime(2018, 2, 1, 0, 5)
    t_range = TimeRange(t_0, t_1)

    files = provider.find_files(hdf4_product, t_range)
    assert len(files) == 2
