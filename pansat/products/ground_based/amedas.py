"""
pansat.products.ground_based.amedas
===================================

Functionality to read AMeDAS data.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re

import numpy as np
import xarray as xr

import pansat
from pansat import TimeRange, FileRecord
from pansat.exceptions import MissingDependency
from pansat.geometry import LonLatRect
from pansat.products import Product, FilenameRegexpMixin


AMEDAS_DOMAIN = LonLatRect(118, 20, 150, 48)


def grib_to_xarray(file_path: Path):
    """
    Parse grib into xarray.

    This is needed because xarray crashes when trying to read AMeDAS
    grid files.
    """
    try:
        import pygrib
    except ImportError as e:
        raise MissingDependency(
            f"""
            Reading AMeDAS files requires the pygrib package to be installed.
            You will also need to a the template product definition template.4.50008.def.
            """
        )
    grbs = pygrib.open(file_path)

    data_vars = {}
    coords = {}

    for grb in grbs:
        var_name = grb.shortName
        values = grb.values
        lat, lon = grb.latlons()

        if 'latitude' not in coords:
            coords['latitude'] = (['latitude', 'longitude'], lat)
            coords['longitude'] = (['latitude', 'longitude'], lon)

        data_vars[var_name] = (['latitude', 'longitude'], values, {
            'units': grb.units,
            'description': grb.name
        })

    grbs.close()
    dataset = xr.Dataset(data_vars, coords)
    return dataset


class AMeDASProduct(FilenameRegexpMixin, Product):
    """
    This class represents AMeDAS radar products.
    """

    def __init__(
        self
    ):
        """
        Create AMeDAS product.
        """
        self.filename_regexp = re.compile(
            "Z__C_RJTD_\d{14}_SRF_GPV_Ggis1km_Prr60lv_ANAL_grib2.bin"
        )
        Product.__init__(self)

    @property
    def default_destination(self):
        """Stores AMeDAS files in a folder called amedas."""
        return Path("amedas")

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, "amedas", "precip_rate"])

    def filename_to_date(self, filename):
        """
        Extract data corresponding to MRMS file.
        """
        name = Path(filename).name.split("_")[4]
        return datetime.strptime(name, "%Y%m%d%H%M%S")

    def get_temporal_coverage(self, rec: FileRecord):
        if isinstance(rec, (str, Path)):
            rec = FileRecord(Path(rec))

        start_time = self.filename_to_date(rec.filename)
        end_time = start_time + timedelta(minutes=30)
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord):
        return AMeDAS_DOMAIN

    def __str__(self):
        return self.name

    def open(self, rec):
        """
        Open a grib2 file containing MRMS precipitation rates using xarray.

        Args:
             rec: A string, pathlib.Path of FileRecord pointing to the local
                 file to open.

        Return:
             An xarray dataset containing the data in the file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec, product=self)

        return grib_to_xarray(rec.local_path)


precip_rate = AMeDASProduct()
