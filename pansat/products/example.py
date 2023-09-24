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
from pansat.time import TimeRange, to_datetime, to_datetime64
from pansat.products.product_description import ProductDescription
from pansat.products import Product, Granule, GranuleProduct

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
    suffix="h5",
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
    end_time = to_datetime(end_time).strftime("%Y%m%d%H%M%S")
    return filename_pattern.format(
        start_time=start_time,
        end_time=end_time,
        lon_min=lon_min,
        lat_min=lat_min,
        lon_max=lon_max,
        lat_max=lat_max,
        suffix=suffix,
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
            suffix="hdf",
        )
        file_path = path / filename
        output_file = SD(str(file_path), SDC.WRITE | SDC.CREATE)

        att = output_file.attr("attribute_1")
        att.set(SDC.CHAR, "test")

        v_lons = output_file.create("longitude", SDC.FLOAT32, 200)
        v_lons[:] = lons
        dim_1 = v_lons.dim(0)
        dim_1.setname("dimension_1")

        v_lats = output_file.create("latitude", SDC.FLOAT32, 200)
        v_lats[:] = lats
        dim_2 = v_lats.dim(0)
        dim_2.setname("dimension_2")

        surface_precip = np.random.rand(200, 200).astype("float32")
        v_sp = output_file.create("surface_precip", SDC.FLOAT32, (200, 200))
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
            suffix="h5",
        )
        file_path = path / filename
        output_file = File(file_path, "w")
        v_lons = output_file.create_dataset("longitude", 200, dtype="float32")
        v_lons[:] = lons
        v_lats = output_file.create_dataset(
            "latitude",
            200,
            dtype="float32",
        )
        v_lats[:] = lats

        surface_precip = np.random.rand(200, 200)
        v_sp = output_file.create_dataset(
            "surface_precip",
            (200, 200),
            dtype="float32",
        )
        v_sp[:] = surface_precip
        output_file.close()

        files.append(file_path)

    return files


class ExampleProduct(Product):
    def __init__(self, name, suffix):
        """
        Create an instance of an example product object.

        Args:
            suffix: The file suffix of the data files.
        """
        self._name = name
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
        self.product_description = ProductDescription(EXAMPLE_PRODUCT_DESCRIPTION)
        super().__init__()

    @property
    def name(self):
        return "example." + self._name

    @property
    def default_destination(self):
        return Path("example")

    def matches(self, path):
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
        match = self.filename_regex.match(rec.filename)
        if match is None:
            raise ValueError(
                f"Filename {rec.filename} does not match the expected "
                "filename pattern."
            )
        date_fmt = "%Y%m%d%H%M%S"
        start_date = datetime.strptime(match.group("start_date"), date_fmt)
        end_date = datetime.strptime(match.group("end_date"), date_fmt)
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


hdf4_product = ExampleProduct("hdf4_product", suffix="hdf")

hdf5_product = ExampleProduct("hdf5_product", suffix="h5")

######################################################################
# Granule product
######################################################################

EXAMPLE_GRANULE_PRODUCT_DESCRIPTION = """
[test-description]
type = properties
name = test-description
granule_dimensions = ["scans", "pixels"]
granule_partitions = [4, 2]

[scans]
type = dimension
name = scans

[pixels]
type = dimension
name = pixels

[data]
type = variable
name = surface_precip
dimensions = ["scans", "pixels"]
description = Some test data.
unit = test

[longitude]
type = longitude_coordinate
name = longitude
dimensions = ["scans", "pixels"]
description = The longitude coordinates of the measurements.

[latitude]
type = latitude_coordinate
name = latitude
dimensions = ["scans", "pixels"]
description = The latitude coordinates of the measurements.

[time]
type = time_coordinate
name = time
dimensions = ["scans"]
description = The time coordinate.
callback = _parse_times
"""


def write_hdf4_granule_product_data(path):
    """
    Populates a temporary directory with a GranuleProduct test data file
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

        lats = np.linspace(-5, 5, 100, dtype="float32")
        lats = np.tile(lats, (200, 1))

        lons = np.linspace(i * 10, (i + 1) * 10, 200, dtype="float32")
        lons = np.tile(lons[..., None], (1, 200))

        filename = get_filename(
            start_time=start_time,
            end_time=end_time,
            lon_min=lons.min(),
            lat_min=lats.min(),
            lon_max=lons.max(),
            lat_max=lats.max(),
            suffix="hdf",
        )
        file_path = path / filename
        output_file = SD(str(file_path), SDC.WRITE | SDC.CREATE)

        v_lons = output_file.create("longitude", SDC.FLOAT32, (200, 100))
        v_lons[:] = lons
        dim_1 = v_lons.dim(0)
        dim_1.setname("scans")
        dim_2 = v_lons.dim(1)
        dim_2.setname("pixels")

        v_lats = output_file.create("latitude", SDC.FLOAT32, (200, 100))
        v_lats[:] = lats
        dim_1 = v_lats.dim(0)
        dim_1.setname("scans")
        dim_2 = v_lats.dim(1)
        dim_2.setname("pixels")

        surface_precip = np.random.rand(200, 100).astype("float32")
        v_sp = output_file.create("surface_precip", SDC.FLOAT32, (200, 100))
        v_sp[:] = surface_precip
        dim_1 = v_sp.dim(0)
        dim_1.setname("scans")
        dim_2 = v_sp.dim(1)
        dim_2.setname("pixels")

        v_time = output_file.create("time", SDC.INT32, (200,))
        dim_1 = v_time.dim(0)
        dim_1.setname("scans")

        start_time = to_datetime64(start_time)
        time_delta = to_datetime64(end_time) - start_time
        time = time_delta / 200 * np.arange(200)
        v_time[:] = time.astype("timedelta64[s]").astype("int32")

        output_file.end()
        files.append(file_path)


def write_hdf5_granule_product_data(path):
    """
    Populates a temporary directory with a test file for the
    ExampleGranuleProduct in HDF5 format.
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

        lats = np.linspace(-5, 5, 100, dtype="float32")
        lats = np.tile(lats, (200, 1))
        lons = np.linspace(i * 10, (i + 1) * 10, 200, dtype="float32")
        lons = np.tile(lons[..., None], (1, 100))

        filename = get_filename(
            start_time=start_time,
            end_time=end_time,
            lon_min=lons.min(),
            lat_min=lats.min(),
            lon_max=lons.max(),
            lat_max=lats.max(),
            suffix="h5",
        )
        file_path = path / filename
        output_file = File(file_path, "w")

        v_lons = output_file.create_dataset("longitude", (200, 100), dtype="float32")
        v_lons[:] = lons
        v_lats = output_file.create_dataset(
            "latitude",
            (200, 100),
            dtype="float32",
        )
        v_lats[:] = lats

        surface_precip = np.random.rand(200, 100)
        v_sp = output_file.create_dataset(
            "surface_precip",
            (200, 100),
            dtype="float32",
        )
        v_sp[:] = surface_precip

        v_time = output_file.create_dataset(
            "time",
            (200,),
            dtype="int64",
        )
        start_time = to_datetime64(start_time)
        end_time = to_datetime64(end_time)
        time = np.arange(start_time, end_time, (end_time - start_time) / 199)

        v_time[:] = time.astype("datetime64[s]").astype("int64")

        output_file.close()
        files.append(file_path)

    return files


def _parse_times(file_handle, slices):
    """
    Callback function required to convert time stored as int back
    to datetime64 dtype.
    """
    return file_handle[slices].astype("datetime64[s]")


class ExampleGranuleProduct(GranuleProduct):
    def __init__(self, name, suffix):
        """
        This granule version of the example product models the hypothetical case
        that the data in the data files consists of two parts. The first one
        covers the first half hour of the temporal coverage of each file and the
        west-most half of the area, while the second part covers the rest of
        the domain.

        Args:
            suffix: The file suffix of the data files.
        """
        self._name = name
        self.suffix = suffix
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
            EXAMPLE_GRANULE_PRODUCT_DESCRIPTION
        )
        super().__init__()

    @property
    def name(self):
        return "example." + self._name

    @property
    def default_destination(self):
        return Path("example")

    def matches(self, path):
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
        match = self.filename_regex.match(rec.filename)
        if match is None:
            raise ValueError(
                f"Filename {rec.filename} does not match the expected "
                "filename pattern."
            )
        date_fmt = "%Y%m%d%H%M%S"
        start_date = datetime.strptime(match.group("start_date"), date_fmt)
        end_date = datetime.strptime(match.group("end_date"), date_fmt)
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

    def get_granules(self, rec: FileRecord) -> list[Granule]:
        """
        Returns the two granules representing the temporal and spatial
        coverage of the file.

        Args:
            rec: A file record identifying the file from which to extract
                the granules.

        Return:
            A list of granule objects representing the temporal and spatial
            coverage of the file identified by 'rec'.
        """
        if self.suffix == "h5":
            from pansat.formats.hdf5 import HDF5File

            file_handle = HDF5File(str(rec.local_path))
        else:
            from pansat.formats.hdf4 import HDF4File

            file_handle = HDF4File(str(rec.local_path))

        granules = []
        for granule_data in self.product_description.get_granule_data(
            file_handle, context=globals()
        ):
            granules.append(Granule(rec, *granule_data))

        return granules

    def open(self, rec: FileRecord) -> xr.Dataset:
        return self.description.to_xarray_dataset(file_handle)

    def open_granule(self, rec: FileRecord):
        pass


hdf4_granule_product = ExampleGranuleProduct("hdf4_granule_product", suffix="hdf")

hdf5_granule_product = ExampleGranuleProduct("hdf5_granule_product", suffix="h5")

######################################################################
# Thin-swath product
######################################################################

EXAMPLE_THIN_SWATH_PRODUCT_DESCRIPTION = """
[test-description]
type = properties
name = test-description
granule_dimensions = ["rays"]
granule_partitions = [10]

[rays]
type = dimension
name = rays

[data]
type = variable
name = surface_precip
dimensions = ["rays"]
description = Some test data.
unit = test

[longitude]
type = longitude_coordinate
name = longitude
dimensions = ["rays"]
description = The longitude coordinates of the measurements.

[latitude]
type = latitude_coordinate
name = latitude
dimensions = ["rays"]
description = The latitude coordinates of the measurements.

[time]
type = time_coordinate
name = time
dimensions = ["rays"]
description = The time coordinate.
callback = _parse_times
"""


def write_thin_swath_product_data(path: Path) -> list[Path]:
    """
    Populates a temporary directory with a ThinSwathProduct test data file
    files in HDF5 format.

    The thin-swath product emulates a data product whose data is organized
    along a single dimensions such as, for example, CloudSat data.

    Args:
        path: The path to which to write the product files.

    Return:
        A list of the written product files.
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

        lat_min = -20 + 10 * i
        lat_max = lat_min + 10
        lats = np.linspace(lat_min, lat_max, 101, dtype="float32")[:-1]
        lons = 13 * np.ones(100, dtype="float32")

        filename = get_filename(
            start_time=start_time,
            end_time=end_time,
            lon_min=lons.min(),
            lat_min=lats.min(),
            lon_max=lons.max(),
            lat_max=lats.max(),
            suffix="h5",
        )
        file_path = path / filename
        output_file = File(file_path, "w")

        v_lons = output_file.create_dataset("longitude", (100,), dtype="float32")
        v_lons[:] = lons
        v_lats = output_file.create_dataset(
            "latitude",
            (100,),
            dtype="float32",
        )
        v_lats[:] = lats

        surface_precip = np.random.rand(100)
        v_sp = output_file.create_dataset(
            "surface_precip",
            (100,),
            dtype="float32",
        )
        v_sp[:] = surface_precip

        v_time = output_file.create_dataset(
            "time",
            (100,),
            dtype="int64",
        )
        start_time = to_datetime64(start_time)
        end_time = to_datetime64(end_time)
        times = np.arange(start_time, end_time, (end_time - start_time) / 99)
        v_time[:] = times.astype("datetime64[s]").astype("int64")

        output_file.close()
        files.append(file_path)

    return files


class ThinSwathProduct(ExampleGranuleProduct):
    """
    The thins-swath product defines a product whose data is organized
    along a single dimension as is the case for CloudSat.
    """

    def __init__(self):
        super().__init__("thin_swath_product", "h5")
        self.product_description = ProductDescription(
            EXAMPLE_THIN_SWATH_PRODUCT_DESCRIPTION
        )


thin_swath_product = ThinSwathProduct()
