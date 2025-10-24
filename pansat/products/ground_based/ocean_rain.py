"""
pansat.products.ground_based.ocean_rain
======================================

Functionality to read and index OceanRain data in NetCDF format.
"""
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Dict, List, Optional, Union

import numpy as np
import xarray as xr

import pansat
from pansat import TimeRange, FileRecord
from pansat.exceptions import MissingDependency
from pansat.geometry import LonLatRect, LineString
from pansat.granule import Granule
from pansat.products import Product, FilenameRegexpMixin


class OceanRainProduct(FilenameRegexpMixin, Product):
    """
    This class represents AMeDAS radar products.
    """

    def __init__(
        self,
        ship_name: str

    ):
        """
        Create OceanRAIN product.
        """
        self.ship_name = ship_name
        self.filename_regexp = re.compile(
            rf"OceanRAIN-W_{ship_name}_\w*_([\w\d-]*)_UHAM-ICDC_v1_0.nc"
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
        ship_name = self.ship_name.lower().replace("-", "_")
        return ".".join([prefix, "ocean_rain", f"oean_rain_{ship_name}"])

    def filename_to_date(self, filename: Union[str, Path, FileRecord]) -> TimeRange:
        """
        Extract time range from OceanRAIN file.
        """

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Get temporal coverage.

        Args:
            b

        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(local_path=Path(rec))

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise ValueError(
                "Filename doesn't match expected filename pattern for "
                "OceanRAIN files."
            )
        time_str = match.group(1)
        start_time = datetime.strptime(time_str.split("-")[0], "%b%Y")
        end_time = datetime.strptime(time_str.split("-")[1], "%b%Y")
        return TimeRange(start_time, end_time)

    def get_granules(self, rec: FileRecord) -> List[Granule]:
        """
        Return granules in file.

        Args:
            rec: A file record object pointing to a local CloudSat file.

        Return:
            A list of granules representing the temporal and spatial coverage
            of the data.
        """
        if not isinstance(rec, FileRecord):
            rec = FileRecord(rec)
        granules = []
        with xr.open_dataset(rec.local_path) as data:
            data = data[["latitude", "longitude"]].load()
            for ind in range(0, data.time.size, 60):
                ind_r = min(ind + 60, data.time.size - 1)
                start_time = data.time[ind].data
                end_time = data.time[ind_r].data
                time_range = TimeRange(start_time, end_time)
                lon_start = data.longitude[ind].item()
                lon_end = data.longitude[ind_r].item()
                lat_start = data.latitude[ind]
                lat_end = data.latitude[ind_r]
                geometry = LineString([[lon_start, lat_start], [lon_end, lat_end]])
                granules.append(Granule(
                    file_record=rec,
                    primary_index_name="time",
                    primary_index_range=(ind, ind_r),
                    time_range=time_range,
                    geometry=geometry
                ))
        return granules

    def get_spatial_coverage(self, rec: FileRecord):
        """
        Spatial coverage of the SatRain product.
        """
        return LonLatRect(-180, -90, 180, 90)

    def __str__(self):
        return self.name

    def open(self, rec, slcs: Optional[Dict[str, slice]] = None):
        """
        Open OceanRain file.

        Args:
             rec: A string, pathlib.Path of FileRecord pointing to the local
                 file to open.

        Return:
             An xarray dataset containing the data in the file.
        """
        if slcs is not None:
            with xr.open_dataset(rec.local_path) as data:
                return data[slcs].load()
        return xr.load_dataset(rec.local_path)


ocean_rain_ms_the_world = OceanRainProduct(
    "MS-The-World",
)
ocean_rain_rv_investigator = OceanRainProduct(
    "RV-Investigator",
)
ocean_rain_rv_maria_s_merian = OceanRainProduct(
    "RV-Maria-S-Merian",
)
ocean_rain_rv_meteor = OceanRainProduct(
    "RV-Meteor",
)
ocean_rain_rv_polarstern = OceanRainProduct(
    "RV-Polarstern"
)
ocean_rain_rv_roger_revelle = OceanRainProduct(
    "RV-Roger-Revelle"
)
ocean_rain_rv_sonneii = OceanRainProduct(
    "RV-SonneII"
)
