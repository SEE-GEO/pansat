"""
pansat.products.satellite.gpm
=============================

This module defines the GPM product class, which is used to represent all
GPM products.
"""
import re
from calendar import monthrange
from datetime import datetime, timedelta
from itertools import dropwhile
from pathlib import Path
from traceback import print_exc
import warnings

import numpy as np
import pandas as pd

import pansat
from pansat import products
import pansat.download.providers as providers
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.products import Granule
from pansat.products.product import Product
from pansat.products.product_description import ProductDescription
from pansat.exceptions import NoAvailableProvider
from pansat.formats.hdf5 import HDF5File
from pansat import geometry


class GPMProduct(Product, products.GranuleProduct):
    """
    Base class representing GPM products.
    """
    def __init__(
            self,
            level,
            platform,
            sensor,
            algorithm,
            version,
            variant,
            description
    ):
        self.level = level.lower()
        self.platform = platform.lower()
        self.sensor = sensor.lower()
        self.algorithm = algorithm.lower()
        self.version = version.lower()
        self.variant = variant.lower()
        self._description = description
        if self.variant:
            variant = "-" + self.variant.upper()
        else:
            variant = ""

        level = level.upper()
        platform = platform.upper()
        sensor = sensor.upper()
        algorithm = algorithm.upper()

        self.filename_regexp = re.compile(
            rf"{level}{variant}\.{platform}\.{sensor}"
            rf"\.{algorithm}([\w-]*).(\d{{8}})-"
            r"S(\d{6})-E(\d{6})\.(\w*)\.((\w*)\.)?(HDF5|h5|nc|nc4)"
        )
        super().__init__()

    @property
    def variables(self):
        return []

    @property
    def description(self):
        return self._description

    @property
    def name(self):
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace('/', ".")
        algo = self.algorithm.replace("-", "")
        lvl = self.level
        sensor = self.sensor
        version = self.version
        pltfrm = self.platform

        if self.variant == "":
            name = f"l{lvl}_{algo}_{pltfrm}_{sensor}_v{version}"
        else:
            variant = self.variant
            name = (
                f"l{lvl}_{variant}_{algo}_{pltfrm}_{sensor}_v{version}"
            )
        return ".".join([prefix , name])

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

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Implements interface to extract temporal coverage of file.
        """
        match = self.filename_regexp.match(rec.filename)
        date = match[2]
        start = match[3]
        end = match[4]
        fmt = "%Y%m%d%H%M%S"
        start = datetime.strptime(date + start, fmt)
        end = datetime.strptime(date + end, fmt)
        if self.variant.startswith("mo"):
            end += timedelta(days=monthrange(start.year, start.month)[-1] - 1)
        if end < start:
            end += timedelta(days=1)
        return TimeRange(start, end)

    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        if rec.local_path is None:
            raise ValueError(
                "This products reuqires a local file is to determine "
                " the spatial coverage."
            )

        with HDF5File(rec.local_path, "r") as file_handle:
            lons, lats = self.description.load_lonlats(
                file_handle, slice(0, None, 1)
            )
        poly = geometry.parse_swath(lons, lats, m=10, n=1)
        return geometry.ShapelyGeometry(poly)

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
        return Path("gpm") / self.level / self.platform / self.sensor

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

    def get_granules(self, rec):
        from pansat.formats.hdf5 import HDF5File

        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)

        granules = []
        with HDF5File(rec.local_path, "r") as file_handle:
            for granule_data in self.description.get_granule_data(
                    file_handle,
                    globals()):
                granules.append(Granule(rec, *granule_data))
        return granules

    def open_granule(self, granule):
        from pansat.formats.hdf5 import HDF5File
        filename = granule.file_record.local_path
        with HDF5File(filename, "r") as file_handle:
            return self.description.to_xarray_dataset(
                file_handle,
                context=globals(),
                slcs=granule.get_slices()
            )


def _extract_scantime(scantime_group, slcs=None):
    """
    Extract scan time as numpy object.

    This function is used to extract the scantime from GPM files as
    an array of numpy.datetime64 objects.

    Args:
         scantime_group: The HDF5 group containing the scantime data.

    Returns:
         numpy.datetime64 object representing the scantime.
    """
    if slcs is None:
        slcs = slice(0, None)
    years = scantime_group["Year"][slcs]
    months = scantime_group["Month"][slcs]
    days = scantime_group["DayOfMonth"][slcs]
    hours = scantime_group["Hour"][slcs]
    minutes = scantime_group["Minute"][slcs]
    seconds = np.minimum(scantime_group["Second"][slcs], 59)
    milli_seconds = scantime_group["MilliSecond"][slcs]
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
        try:
            if filename.match("*.ini") and filename.name != "gprof.ini":
                description = ProductDescription(filename)
                python_name = description.properties.name
                level = description.properties["level"]
                platform = description.properties["platform"]
                sensor = description.properties["sensor"]
                algorithm = description.properties["algorithm"]
                version = description.properties["version"]
                variant = description.properties["variant"]

                product = GPMProduct(
                    level, platform, sensor, algorithm, version, variant, description
                )
                globals()[python_name] = product
                if "alias" in description.properties:
                    alias = description.properties["alias"]
                    globals()[alias] = product
        except Exception:
            warnings.warn(
                "Error encountered while trying to parse "
                f"the product file '{filename}': \n {print_exc()}",
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
    def __init__(self, gprof_algorithm, platform, sensor, version, variant=""):
        module_path = Path(__file__).parent
        description = ProductDescription(module_path / "gprof.ini")
        super().__init__(
            "2A",
            platform,
            sensor,
            gprof_algorithm,
            version,
            variant,
            description
        )


l2a_gprof_gpm_gmi = GPROFProduct("GPROF2021v1", "GPM", "GMI", "07a")
l2a_gprof_noaa18_mhs = GPROFProduct("GPROF2021v1", "NOAA18", "mhs", "07a")
l2a_gprof_noaa19_mhs = GPROFProduct("GPROF2021v1", "NOAA19", "mhs", "07a")
l2a_gprof_metopa_mhs = GPROFProduct("GPROF2021v1", "metopa", "mhs", "07a")
l2a_gprof_metopb_mhs = GPROFProduct("GPROF2021v1", "metopb", "mhs", "07a")
l2a_gprof_metopc_mhs = GPROFProduct("GPROF2021v1", "metopc", "mhs", "07a")
l2a_gprof_noaa20_atms = GPROFProduct("GPROF2021v1", "NOAA20", "atms", "07a")
l2a_gprof_npp_atms = GPROFProduct("GPROF2021v1", "npp", "atms", "07a")


################################################################################
# GPM merged IR
################################################################################


class GPMMergedIR:
    """
    The GPM merged IR product.
    """

    def __init__(self):
        pattern = r"merg_(\d{10,10})_4km-pixel.nc"
        self.filename_regexp = re.compile(pattern)

    @property
    def name(self):
        return "satellite.gpm.merged_ir"

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

    def get_temporal_coverage(self, rec):
        """
        GPM merged IR files cover 1h.
        """
        match = self.filename_regexp.match(rec.filename)
        date = datetime.strptime(match.group(1), "%Y%m%d%H")
        start_time = date
        end_time = date + timedelta(hours=1)
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, *args):
        """
        The GPM merged IR product has fixed coverage covering all longitudes
        and latitude -60 to 60.
        """
        return LonLatRect(-180, -90, 180, 90)


    @property
    def default_destination(self):
        """
        The default destination for GPM products is
        ``GPM/<product_name>``>
        """
        return Path("gpm") / "merged_ir"


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

    def open(self, path):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The GPM file to open.
        """
        xr.load_dataset(path)


merged_ir = GPMMergedIR()


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
