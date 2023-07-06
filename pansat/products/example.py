"""
pansat.products.example
=======================

This module implements an example product to illustrate and
test the ideas behind pansat.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re

import numpy as np
import xarray as xr

from pansat.file_record import FileRecord
from pansat.geometry import Geometry, LonLatRect
from pansat.time import TimeRange
from pansat.products.product_description import ProductDescription

######################################################################
# Product description
######################################################################

EXAMPLE_PRODUCT_DESCRIPTION = """
[test-description]
type = properties
name = test-description

[dimension_1]
type = dimension
name = dimension_1

[dimension_2]
type = dimension
name = dimension_2

[data]
type = variable
name = surface_precip
dimensions = ["dimension_1", "dimension_2"]
description = Some test data.
unit = test

[longitude]
type = longitude_coordinate
name = longitude
dimensions = ["dimension_1"]
description = Coordinate 1.

[latitude]
type = latitude_coordinate
name = latitude
dimensions = ["dimension_2"]
description = Coordinate 2.

[attribute_1]
type = attribute
name = attribute_1
dimensions = []
description = An attribute.
"""

###############################################################################
# Test data creation
###############################################################################

def get_filename(
        start_time,
        end_time,
        lon_min=-180,
        lat_min=-90,
        lon_max=180,
        lat_max=90,
        suffix="h5"
):
    """
    Create filename for example product.

    Args:
        start_time: The start time.
        end_time: The end_time.
        lon_min: The minimum longitude of the data in the file.
        lat_min: The minimum latitude of the data in the file.
        lon_max: The maximum longitude of the data in the file.
        lat_max: The maximum latitude of the data in the file.

    Return:
        A string containing the example filename.
    """
    filename_pattern = (
        "data_file_{start_time}_{end_time}_"
        "{lon_min:+07.2f}_{lat_min:+07.2f}_"
        "{lon_max:+07.2f}_{lat_max:+07.2f}.{suffix}"
    )
    start_time = to_datetime(start_time).strftime("%Y%m%d%H%M%S")
    end_time = to_datetime(start_time).strftime("%Y%m%d%H%M%S")
    return filename_pattern.format(
        start_time=start_time,
        end_time=end_time,
        lon_min=lon_min,
        lat_min=lat_min,
        lon_max=lon_max,
        lat_max=lat_max,
        suffix=suffix
    )


def write_hdf4_product_data(path):
    """
    Populates a temporary directory with a product description and test
    files in HDF4 format.

    Args:
        path: The path to which to write the product files.

    Return:
        A list of the written product files.
    """
    from pyhdf.SD import SD, SDC

    path = Path(path)

    delta_t = timedelta(hours=1)
    files = []
    for i in range(4):
        start_time = datetime(2020, 1, 1, i)
        end_time = start_time + delta_t

        lats = np.linspace(-5, 5, 200, dtype="float32")
        lons = np.linspace(i * 10, (i + 1) * 10, 200, dtype="float32")

        filename = get_filename(
            start_time=start_time,
            end_time=end_time,
            lon_min=lons.min(),
            lat_min=lats.min(),
            lon_max=lons.max(),
            lat_max=lats.max(),
            suffix="hdf"
        )
        file_path = path / filename
        output_file = SD(str(file_path), SDC.WRITE | SDC.CREATE)

        att = output_file.attr("attribute_1")
        att.set(SDC.CHAR, 'test')

        v_lons = output_file.create(
            'longitude',
            SDC.FLOAT32,
            200
        )
        v_lons[:] = lons
        v_lats = output_file.create(
            'latitude',
            SDC.FLOAT32,
            200
        )
        v_lats[:] = lats

        surface_precip = np.random.rand(200, 200).astype("float32")
        v_sp = output_file.create(
            'surface_precip',
            SDC.FLOAT32,
            (200, 200)
        )
        v_sp[:] = surface_precip
        output_file.end()
        files.append(file_path)


def write_hdf5_product_data(path):
    """
    Populates a temporary directory with a product description and test
    files in HDF5 format.
    """
    from h5py import File

    path = Path(path)

    filename_pattern = (
        "data_file_{start_time}_{end_time}_"
        "{lon_min:06.2f}_{lat_min:06.2f}_{lon_max:06.2f}_{lat_max:06.2f}.h5"
    )
    delta_t = timedelta(hours=1)
    files = []

    for i in range(4):
        start_time = datetime(2020, 1, 1, i)
        end_time = start_time + delta_t

        lats = np.linspace(-5, 5, 200)
        lons = np.linspace(i * 10, (i + 1) * 10, 200)

        filename = get_filename(
            start_time=start_time,
            end_time=end_time,
            lon_min=lons.min(),
            lat_min=lats.min(),
            lon_max=lons.max(),
            lat_max=lats.max(),
            suffix=suffix
        )
        file_path = path / filename
        output_file = File(file_path, "w")
        v_lons = output_file.create_dataset(
            'longitude',
            200,
            dtype="float32"
        )
        v_lons[:] = lons
        v_lats = output_file.create_dataset(
            'latitude',
            200,
            dtype="float32",
        )
        v_lats[:] = lats

        surface_precip = np.random.rand(200, 200)
        v_sp = output_file.create_dataset(
            'surface_precip',
            (200, 200),
            dtype="float32",
        )
        v_sp[:] = surface_precip
        output_file.close()

        files.append(file_path)

    return files


class ExampleProduct:
    def __init__(self, suffix):
        """
        Create an instance of an example product object.

        Args:
            suffix: The file suffix of the data files.
        """
        self.filename_regex = re.compile(
            r"data_file_"
            r"(?P<start_date>\d{14})_"
            r"(?P<end_date>\d{14})_"
            r"(?P<lon_min>[\+\-\d\.]{7})_"
            r"(?P<lat_min>[\+\-\d\.]{7})_"
            r"(?P<lon_max>[\+\-\d\.]{7})_"
            r"(?P<lat_max>[\+\-\d\.]{7})"
            f".{suffix}"
        )
        self.product_description = ProductDescription(
            EXAMPLE_PRODUCT_DESCRIPTION
        )

    def match(self, path):
        """
        Determine if given path points to a product data file.
        """
        path = Path(path)
        return self.filename_regex.match(path.name) is not None

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        The time range spanned by the observations in the file.

        Args:
            rec:
        """
        match = self.filename_regex.match(rec.name)
        date_fmt = "%Y%m%d%H%M%S"
        start_date = datetime.strptime(date_fmt, match.groups("start_date"))
        end_date = datetime.strptime(date_fmt, match.groups("end_date"))
        return TimeRange(start_date, end_date)

    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Args:
            rec: File record pointing to a product data file.

        Return:
            A geometry object representing the spatial coverage of the
            observations in a given product data file.
        """
        match = self.filename_regex.match(rec.filename)
        lon_min = float(match.group("lon_min"))
        lat_min = float(match.group("lat_min"))
        lon_max = float(match.group("lon_max"))
        lat_max = float(match.group("lat_max"))
        return LonLatRect(lon_min, lat_min, lon_max, lat_max)

    def open(self, rec: FileRecord) -> xr.Dataset:
        return self.description.to_xarray_dataset(file_handle)


example_product_hdf4 = ExampleProduct(suffix="hdf")
example_product_hdf5 = ExampleProduct(suffix="h5")


