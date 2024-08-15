"""
pansat.products.satellite.noaa.gaasp
====================================

Provides a pansat product for the AMSR2 L1B files produced by
the GCOM-W1 AMSR2 Algorithm Software Package.
"""

from datetime import datetime
from pathlib import Path
import re
from typing import List, Optional, Tuple

import h5py
import numpy as np
import xarray as xr

import pansat
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.granule import Granule
from pansat.products import GranuleProduct, FilenameRegexpMixin
from pansat.products.product_description import ProductDescription
from pansat import geometry


class GAASPProduct(FilenameRegexpMixin, GranuleProduct):
    """
    Class representing GAASP products.
    """

    def __init__(self):
        self._name = "l1b_gcomw1_amsr2"
        module_path = Path(__file__).parent
        self.product_description = ProductDescription(
            module_path / "l1b_gaasp_gcomw1_amsr2.ini"
        )
        self.filename_regexp = re.compile(
            rf"GAASP-L1B_v\dr\d_GW1_s(\d*)_e(\d*)_c(\d*).h5"
        )
        GranuleProduct.__init__(self)

    @property
    def name(self) -> str:
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, self._name])

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a NOAA GRAASP file.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)

        # Some files of course have to follow a different convention.
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
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                f"Provided file record with filename {rec.filename} doest not "
                " match the products filename regexp "
                f"{self.filename_regexp.pattern}. "
            )

        start_date = match.group(1)
        start_date = datetime.strptime(start_date, "%Y%m%d%H%M%S0")
        end_date = match.group(2)
        end_date = datetime.strptime(end_date, "%Y%m%d%H%M%S0")
        return TimeRange(start_date, end_date)

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
            lons, lats = self.product_description.load_lonlats(
                file_handle, slice(0, None, 1)
            )
        poly = geometry.parse_swath(lons, lats, m=10, n=1)
        return geometry.ShapelyGeometry(poly)

    @property
    def default_destination(self):
        """
        Not used since data is not publicly available.
        """
        return Path("noaa")

    def __str__(self):
        return self.name

    def open(self, rec: FileRecord, slcs: Optional[dict[str, slice]] = None):
        """
        Open file as xarray dataset.

        Args:
            rec: A FileRecord whose local_path attribute points to a local NOAA GRAASP file to open.
            slcs: An optional dictionary of slices to use to subset the
                data to load.

        Return:
            An xarray.Dataset containing the loaded data.
        """
        from pansat.formats.hdf5 import HDF5File

        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        with HDF5File(rec.local_path, "r") as file_handle:
            return self.product_description.to_xarray_dataset(
                file_handle, context=globals(), slcs=slcs
            )

    def get_granules(self, rec: FileRecord) -> List[Granule]:
        """
        Get granules describing spatiotemporal coverage of file.

        Args:
            rec: A file record pointing to a file from which to extract granules.
        """
        from pansat.formats.hdf5 import HDF5File

        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)

        granules = []
        with HDF5File(rec.local_path, "r") as file_handle:
            for granule_data in self.product_description.get_granule_data(
                file_handle, globals()
            ):
                granules.append(Granule(rec, *granule_data))
        return granules

    def open_granule(self, granule: Granule) -> xr.Dataset:
        """
        Load data from granule.

        Args:
            granule: The granule for which to load the data

        Return:
            An xarray.Dataset containing the loaded data.
        """
        from pansat.formats.hdf5 import HDF5File

        filename = granule.file_record.local_path
        with HDF5File(filename, "r") as file_handle:
            return self.product_description.to_xarray_dataset(
                file_handle, context=globals(), slcs=granule.get_slices()
            )


def load_channels(
    file_handle: h5py.File, channels: List[str], slices: Optional[Tuple[slice]] = None
) -> np.ndarray:
    """
    Load variables from HDF5 file handle and stack along last dimension.

    Args:
        file_handle: The file handle to the opened HDF5 file containing the L1B data.
        channels: List of the channels names to load.
        slices: An iptional tuple containing slices to subset the data to load.

    Return:
        The loaded data as an np.ndarray.
    """

    tbs = []
    for chan in channels:
        if slices is None:
            slices = slice(0, None)
        scaling = file_handle[chan].attrs["SCALE FACTOR"][0]
        if isinstance(slices, tuple):
            slices = slices[:2]
        tbs.append(scaling * file_handle[chan].__getitem__(slices))

    tbs = np.stack(tbs, -1)
    return tbs


def load_tbs_low_res_amsr2(
    file_handle: h5py.File, slices: Optional[Tuple[slice]] = None
):
    """
    Load Tbs from AMSR2 low-resolution channels.

    Args:
        file_handle: File handle pointing to the opened HDF5 file from which
            to load the data.
        slices: Optional tuples of slices to use to subset the data to load.

    Return:
        A numpy.ndarray containing the loaded data.
    """
    low_res_chans = [
        "6.9GHz,H",
        "6.9GHz,V",
        "7.3GHz,H",
        "7.3GHz,V",
        "10.7GHz,H",
        "10.7GHz,V",
        "18.7GHz,H",
        "18.7GHz,V",
        "23.8GHz,H",
        "23.8GHz,V",
        "36.5GHz,H",
        "36.5GHz,V",
    ]
    low_res_chans = [f"Brightness Temperature ({freq})" for freq in low_res_chans]
    return load_channels(file_handle, low_res_chans, slices=slices)


def load_tbs_89a_amsr2(file_handle: h5py.File, slices: Optional[Tuple[slice]] = None):
    """
    Load Tbs from AMSR2 89GHz-A channels.

    Args:
        file_handle: File handle pointing to the opened HDF5 file from which
            to load the data.
        slices: Optional tuples of slices to use to subset the data to load.

    Return:
        A numpy.ndarray containing the loaded data.
    """
    chans = [
        "89.0GHz-A,H",
        "89.0GHz-A,V",
    ]
    chans = [f"Brightness Temperature ({freq})" for freq in chans]
    return load_channels(file_handle, chans, slices=slices)


def load_tbs_89b_amsr2(file_handle: h5py.File, slices: Optional[Tuple[slice]] = None):
    """
    Load Tbs from AMSR2 89GHz-B channels.

    Args:
        file_handle: File handle pointing to the opened HDF5 file from which
            to load the data.
        slices: Optional tuples of slices to use to subset the data to load.

    Return:
        A numpy.ndarray containing the loaded data.
    """
    chans = [
        "89.0GHz-B,H",
        "89.0GHz-B,V",
    ]
    chans = [f"Brightness Temperature ({freq})" for freq in chans]
    return load_channels(file_handle, chans, slices=slices)


def load_scan_time_amsr2(vrbl, slices=None) -> np.ndarray:
    """
    Load scan time into datetime64 array.

    Args:
        vrbl: Pointer to the HDF5 variable containing the scan time in
            seconds.
        slices: A optional slice object to subset the data to load.

    Return:
        A datetime64 array containing the absolute scan time.
    """
    if slices is None:
        slices = slice(0, None)
    d_t = vrbl.__getitem__(slices).astype("timedelta64[s]")
    return np.datetime64("1993-01-01T00:00:00") + d_t


def load_spacecraft_lon(vrbl, slices=None) -> np.ndarray:
    """
    Load longitude coordinate of sensor from naviation data.

    Args:
        vrbl: Pointer to the HDF5 variable containing the sensor navigation
            data.
        slices: A optional slice object to subset the data to load.

    Return:
        A numpy.ndarray containig the longitude coordinates of the
        sensor at each scan position.
    """
    ndata = vrbl[slices]
    alt = np.sqrt(ndata[:, 0] ** 2 + ndata[:, 1] ** 2 + ndata[:, 2] ** 2)
    sensor_lat = np.rad2deg(np.arcsin(ndata[:, 2] / alt))
    sensor_lon = np.rad2deg(
        np.arccos(ndata[:, 0] / (alt * np.cos(np.deg2rad(sensor_lat))))
    )
    return sensor_lon


def load_spacecraft_lat(vrbl, slices=None) -> np.ndarray:
    """
    Load latitude coordinate of sensor from naviation data.

    Args:
        vrbl: Pointer to the HDF5 variable containing the sensor navigation
            data.
        slices: A optional slice object to subset the data to load.

    Return:
        A numpy.ndarray containig the latitude coordinates of the
        sensor at each scan position.
    """
    ndata = vrbl[slices]
    alt = np.sqrt(ndata[:, 0] ** 2 + ndata[:, 1] ** 2 + ndata[:, 2] ** 2)
    sensor_lat = np.rad2deg(np.arcsin(ndata[:, 2] / alt))
    sensor_lon = np.rad2deg(
        np.arccos(ndata[:, 0] / (alt * np.cos(np.deg2rad(sensor_lat))))
    )
    return sensor_lon


def load_spacecraft_alt(vrbl, slices=None) -> np.ndarray:
    """
    Load altitude of sensor from naviation data.


    Args:
        vrbl: Pointer to the HDF5 variable containing the sensor navigation
            data.
        slices: A optional slice object to subset the data to load.

    Return:
        A numpy.ndarray containig the latitude coordinates of the
        sensor at each scan position.
    """
    ndata = vrbl[slices]
    alt = np.sqrt(ndata[:, 0] ** 2 + ndata[:, 1] ** 2 + ndata[:, 2] ** 2)
    sensor_lat = np.rad2deg(np.arcsin(ndata[:, 2] / alt))
    return alt


l1b_gaasp_gcomw1_amsr2 = GAASPProduct()
l1b_gcomw1_amsr2 = l1b_gaasp_gcomw1_amsr2
