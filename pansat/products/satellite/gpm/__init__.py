"""
pansat.products.satellite.gpm
=============================

This module defines the GPM product class, which is used to represent all
GPM products.
"""
import re
from datetime import datetime
from itertools import dropwhile
from pathlib import Path

import numpy as np
import pandas as pd

import pansat.download.providers as providers
from pansat.products.product import Product
from pansat.products.product_description import ProductDescription
from pansat.exceptions import NoAvailableProvider
from pansat.formats.hdf5 import HDF5File
from pansat import geometry


class GPMProduct(Product):
    """
    Base class representing GPM products.
    """

    def __init__(self, level, platform, sensor, name, version, variant, description):
        self.level = level
        self.platform = platform
        self.sensor = sensor
        self.name = name
        self.version = version
        self.variant = variant
        self._description = description

        if self.variant:
            variant = "-" + self.variant
        else:
            variant = ""
        self.filename_regexp = re.compile(
            rf"{self.level}{variant}\.{self.platform}\.{self.sensor}"
            rf"\.{self.name}([\w-]*).(\d{{8}})-"
            r"S(\d{6})-E(\d{6})\.(\w*)\.((\w*)\.)?(HDF5|h5|nc|nc4)"
        )

    @property
    def variables(self):
        return []

    @property
    def description(self):
        return self._description

    def matches(self, filename):
        """
        Determines whether a given filename matches the pattern used for
        the product.

        Args:
            filename(``str``): The filename

        Return:
            True if the filename matches the product, False otherwise.
        """
        return self.filename_regexp.match(filename)

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a GPM product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)

        # Some files of course have to follow a different convetion.
        if match is None:
            date_string = "20" + path.name.split("_")[2]
        else:
            date_string = match.group(2) + match.group(3)
        date = datetime.strptime(date_string, "%Y%m%d%H%M%S")
        return date

    def filename_to_start_time(self, filename):
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(2) + match.group(3)
        date = datetime.strptime(date_string, "%Y%m%d%H%M%S")
        return date

    def filename_to_end_time(self, filename):
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(2) + match.group(4)
        date = datetime.strptime(date_string, "%Y%m%d%H%M%S")
        return date

    def get_spatial_coverage(self, rec):
        """
        Create geometry representing the spatial coverage of a data file.

        Args:
            rec: A 'FileRecord' object pointing to a data file.

        Return:
            A geometry object representing the the spatial coverage.
        """
        if rec.local_path is not None and rec.local_path.exists():
            with HDF5File(str(rec.local_path), "r") as input_data:
                lats = input_data["S1/Latitude"][:]
                lons = input_data["S1/Longitude"][:]
                valid = np.where(np.any(lons >= -180, 0))[0]
                left = valid[0]
                right = valid[-1]
                return geometry.parse_swath(lons[:, left:right], lats[:, left:right])
        raise ValueError(
            "A NetcdfProduct needs a local file to determine temporal coverage"
            " but the 'local_path' attribute of the provided file record "
            "does not point to an existing file."
        )

    def _get_provider(self):
        """Find a provider that provides the product."""
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProvider(
                f"Could not find a provider for the" f" product {str(self)}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for GPM product is
        ``GPM/<product_name>``>
        """
        return Path("GPM") / Path(self.name)

    def __str__(self):
        if self.variant:
            variant = f"-{self.variant}"
        else:
            variant = ""
        s = f"GPM_{self.level}{variant}_{self.platform}_{self.sensor}"
        return s

    def download(self, start_time, end_time, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start
                 date of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date
                 of the of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to
                 store the output data.
        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        return provider.download(start_time, end_time, destination)

    def open(self, filename):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The GPM file to open.
        """
        from pansat.formats.hdf5 import HDF5File

        with HDF5File(filename, "r") as file_handle:
            return self.description.to_xarray_dataset(file_handle, globals())


def _extract_scantime(scantime_group):
    """
    Extract scan time as numpy object.

    This function is use

    Args:
         scantime_group: The HDF5 group containing the scantime data.

    Returns:
         numpy.datetime64 object representing the scantime.
    """
    years = scantime_group["Year"][:]
    months = scantime_group["Month"][:]
    days = scantime_group["DayOfMonth"][:]
    hours = scantime_group["Hour"][:]
    minutes = scantime_group["Minute"][:]
    seconds = scantime_group["Second"][:]
    milli_seconds = scantime_group["MilliSecond"][:]
    n_dates = years.size
    dates = np.zeros(n_dates, dtype="datetime64[ms]")
    for i in range(n_dates):
        year = years[i]
        month = months[i]
        day = days[i]
        hour = hours[i]
        minute = minutes[i]
        second = seconds[i]
        milli_second = milli_seconds[i]
        dates[i] = np.datetime64(
            f"{year:04}-{month:02}-{day:02}"
            f"T{hour:02}:{minute:02}:{second:02}"
            f".{milli_second:03}"
        )
    return dates


def _parse_products():
    module_path = Path(__file__).parent
    for filename in module_path.iterdir():
        if filename.match("*.ini") and filename.name != "gprof.ini":
            description = ProductDescription(filename)
            python_name = filename.name.split(".")[0]
            level = description.properties["level"]
            platform = description.properties["platform"]
            sensor = description.properties["sensor"]
            name = description.properties["name"]
            version = int(description.properties["version"])
            variant = description.properties["variant"]
            globals()[python_name] = GPMProduct(
                level, platform, sensor, name, version, variant, description
            )


_parse_products()

################################################################################
# GPROF products
################################################################################


class GPROFProduct(GPMProduct):
    """
    Specialization of GPM product for GPROF products, which all have the same
    data format.
    """

    def __init__(self, platform, sensor, version, variant=""):
        module_path = Path(__file__).parent
        description = ProductDescription(module_path / "gprof.ini")
        super().__init__("2A", platform, sensor, "GPROF", version, variant, description)


l2a_gprof_gpm_gmi = GPROFProduct("GPM", "GMI", 5)
l2a_gprof_metopb_mhs = GPROFProduct("METOPB", "MHS", 5)


################################################################################
# GPM merged IR
################################################################################


class GPMMergeIR:
    """
    The GPM merged IR product.
    """

    def __init__(self):
        pattern = r"merg_(\d{10,10})_4km-pixel.nc"
        self.filename_regexp = re.compile(pattern)

    def matches(self, filename):
        """
        Determines whether a given filename matches the pattern used for
        the product.

        Args:
            filename(``str``): The filename

        Return:
            True if the filename matches the product, False otherwise.
        """
        return self.filename_regexp.match(filename)

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a GPM product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(1)
        date = datetime.strptime(date_string, "%Y%m%d%H")
        self.variant = ""
        return date

    def _get_provider(self):
        """Find a provider that provides the product."""
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProvider(
                f"Could not find a provider for the" f" product {str(self)}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default destination for GPM product is
        ``GPM/<product_name>``>
        """
        return Path("GPM") / Path(self.name)

    @property
    def name(self):
        return "gpm_mergeir"

    def __str__(self):
        return self.name

    def download(self, start_time, end_time, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start
                 date of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date
                 of the of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to
                 store the output data.
        """

        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        return provider.download(start_time, end_time, destination)

    def open(self, filename):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The GPM file to open.
        """
        from pansat.formats.hdf5 import HDF5File

        file_handle = HDF5File(filename, "r")
        return self.description.to_xarray_dataset(file_handle, globals())


gpm_mergeir = GPMMergeIR()


def _imerg_parse_time(seconds_since_1970):
    """
    Helper function to convert time from IMERG HDF5 files to
    numpy datetime.
    """
    return np.datetime64("1970-01-01T00:00:00") + seconds_since_1970[:].astype(
        "timedelta64[s]"
    )


def _gpm_l1c_parse_time(scan_time_group):
    """
    Helper function to convert time from GPM L1C to
    numpy datetime.
    """
    year = scan_time_group["Year"][:]
    month = scan_time_group["Month"][:]
    day = scan_time_group["DayOfMonth"][:]
    hour = scan_time_group["Hour"][:]
    minute = scan_time_group["Minute"][:]
    second = scan_time_group["Second"][:]
    milli_second = scan_time_group["MilliSecond"][:]

    return pd.to_datetime(
        pd.DataFrame(
            {
                "year": year,
                "month": month,
                "day": day,
                "hour": hour,
                "minute": minute,
                "second": second,
            }
        )
    )
    return scan_time


def parse_l1c_header(dataset, file_handle):
    """
    Callback to parse header of GPM L1C file.

    Args:
        dataset: ``xarray.Dataset`` containing the data loaded from the file.
        file_handle: File handle to the ``h5py`` ``File`` object.
    """
    attrs = {}
    for attr in file_handle.attrs["FileHeader"].decode().split("\n"):
        try:
            key, value = attr.split("=")
            attrs[key] = value[:-1]
        # If we can't split it, it's not an attribute.
        except ValueError:
            pass
    dataset.attrs.update(attrs)


def parse_frequencies(field):
    """
    Callback to parse frequencies from GPM L1C files.

    Args:
        field: The h5py variable containing the brightness temperatures.
    """
    lines = field.attrs["LongName"].decode().split()
    freqs = []
    c = 1
    c_name = f"{c})"

    for w1, w2 in zip(lines[:-1], lines[1:]):
        if w1 == c_name:
            freqs.append(float(w2.split("+")[0]))
            c += 1
            c_name = f"{c})"
    return np.array(freqs)


def parse_offsets(field):
    """
    Callback to parse frequency-offsets from GPM L1C files.

    Args:
        field: The h5py variable containing the brightness temperatures.
    """
    lines = field.attrs["LongName"].decode().split()
    lines.append("x")
    lines.append("x")
    offs = []
    c = 1
    c_name = f"{c})"
    for w1, w2, w3, w4 in zip(lines[:-4], lines[1:-2], lines[2:-1], lines[3:]):
        if w1 == c_name:
            freq = w2.split("+")
            if len(freq) > 1:
                offs.append(float(freq[1][1:]))
            elif w3.startswith("+"):
                if w3 == "+/-":
                    offs.append(float(w4))
                elif w3.startswith("+/-"):
                    offs.append(float(w3[3:]))
                else:
                    offs.append(float(w3[1:]))
            else:
                offs.append(0.0)
            c += 1
            c_name = f"{c})"

    return np.array(offs)
