"""
pansat.products.reanalysis.era5
===============================
This module defines the ERA5 product class, which represents all
supported ERA5 products.


"""
import re
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Union

import xarray

import pansat
import pansat.download.providers as providers
from pansat import FileRecord, TimeRange
from pansat.geometry import Geometry, LonLatRect
from pansat.products import Product, FilenameRegexpMixin
from pansat.exceptions import NoAvailableProvider


class ERA5Product(FilenameRegexpMixin, Product):
    """
    The ERA5 class defines a generic interface for ERA5 products.

    Attributes:
        subset(``str``): "surface", "pressure" or "land". <surface> contains surface data
                          and column-integrated values, pressure levels contains data throughout
                          the atmosphere column and <land> contains data from surface to soil depth
        name(``str``): The full name of the product according to Copernicus webpage
        variables(``list``): List of variable names provided by this
            product.
        domain(``Geometry``): A geometry defining a spatial domain to which to limit the dataset.
    """
    def __init__(
            self,
            subset: str,
            time_step: str,
            variables: List[str],
            domain: Union[Geometry, Tuple[float]] = None
    ):
        """
        Args:
            subset: Specifies the sub-dataset of ERA5: 'surface', 'pressure', or 'land'
            time_step: The temporal resolution of the data: "hourly" or "monthly".
            variables:


        """
        self.variables = variables

        if domain is None:
            lon_min, lat_min, lon_max, lat_max = -180, -90, 180, 90
        else:
            if isinstance(domain, Geometry):
                bbox = geometry.bounding_box_corners()
            else:
                bbox = domain
            lon_min, lat_min, lon_max, lat_max = bbox
        self.domain = LonLatRect(lon_min, lat_min, lon_max, lat_max)
        if (lon_min > -180) or (lat_min > -90) or (lon_max < 180) or (lat_max < 90):
            domain_str = f"{int(lon_min)},{int(lat_min)},{int(lon_max)},{int(lat_max)}"
        else:
            domain_str = ""
        self.domain_str = domain_str

        self.subset = subset
        if self.subset == "surface":
            subset_str = "single-levels"
        elif self.subset == "pressure_levels":
            subset_str = "pressure_levels"
        elif self.subset == "land":
            subset_str = "land"
        else:
            raise Exception("Supported ERA5 subsets are 'surface', 'pressure' and 'land'.")

        self.time_step = time_step
        if self.time_step == "monthly":
            time_step_str = "_monthly-means"
        elif self.time_step == "hourly":
            time_step_str = ""

        self.dataset_name = f"reanalysis-era5-{subset_str}{time_step_str}"

        variable_str = ",".join(variables)
        self.variable_str = variable_str

        if self.time_step == "monthly":
            time_step_str = "_monthly"
        elif self.time_step == "hourly":
            time_step_str = "_hourly"
        self.time_step_str = time_step_str

        if domain_str == "":
            self.filename_regexp = re.compile(
                rf"era5_{self.subset}{time_step_str}_(\d*)_(\d*)_\[{variable_str}\].nc"
            )
            self._name = f"era5_{self.subset}{time_step_str}_[{variable_str}]"
        else:
            self.filename_regexp = re.compile(
                rf"era5_{self.subset}{time_step_str}_(\d*)_(\d*)_\[{variable_str}\]_\[{domain_str}\].nc"
            )
            self._name = f"era5_{self.subset}{time_step_str}_[{variable_str}]_[{domain_str}]"

        super().__init__()


    @property
    def name(self) -> str:
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return ".".join([prefix, self._name])

    @property
    def bounding_box_string(self) -> str:
        """
        String representation of the domain covered by the product.
        """
        bbox = self.domain
        lon_min = bbox.lon_min
        lat_min = bbox.lat_min
        lon_max = bbox.lon_max
        lat_max = bbox.lat_max

        if (lon_min > -180) or (lat_min > -90) or (lon_max < 180) or (lat_max < 90):
            domain_str = f"{lat_max:.2f}/{lon_min:.2f}/{lat_min:.2f}/{lon_max:.2f}"
        else:
            domain_str = ""
        return domain_str

    def get_time_steps(self, rec: FileRecord) -> List[str]:
        """
        Get list of time steps contained within a given ERA5 file.
        """
        if self.time_step == "monthly":
            return ["00:00"]
        time_range = self.get_temporal_coverage(rec)
        time_steps = []
        time = time_range.start
        while time < time_range.end:
            time_steps.append(f"{time.hour:02}:00")
            time = time + timedelta(hours=1)
        return time_steps

    def get_filename(self, time_range: TimeRange):
        """
        Get filename for a given ERA5 record.
        """
        if self.time_step == "hourly":
            start_time = time_range.start.strftime("%Y%m%d%H")
            end_time = time_range.end.strftime("%Y%m%d%H")
        else:
            start_time = time_range.start.strftime("%Y%m")
            end_time = time_range.end.strftime("%Y%m")

        variable_str = "_".join(self.variables)
        if self.time_step == "monthly":
            time_step_str = "_monthly"
        elif self.time_step == "hourly":
            time_step_str = "_hourly"

        filename = (
            f"era5_{self.subset}{self.time_step_str}_{start_time}_{end_time}"
            f"_{self.variable_str}{self.domain_str}.nc"
        )
        return filename


    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Determine temporal coverage of ERA5 file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        match = self.filename_regexp.match(rec.filename)

        if match is None:
            raise ValueError(
                f"Provided file record with filename {rec.filename} doesn't match "
                "the ERA5 filename pattern."
            )

        start_date = match.group(1)
        end_date = match.group(2)

        if self.time_step == "hourly":
            start = datetime.strptime(start_date, "%Y%m%d%H")
            end = datetime.strptime(end_date, "%Y%m%d%H")
            return TimeRange(start, end)

        start = datetime.strptime(start_date, "%Y%m")
        end = datetime.strptime(end_date, "%Y%m")
        return TimeRange(start, end)


    def get_spatial_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Determine spatial coverage of ERA5 file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        match = self.filename_regexp.match(rec.filename)

        if match is None:
            raise ValueError(
                f"Provided file record with filename {rec.filename} doesn't match "
                "the ERA5 filename pattern."
            )

        parts = rec.filename.split("_")
        try:
            parts = parts[-1][1:-4].split(",")
            lon_min = float(parts[0])
            lat_min = float(parts[1])
            lon_max = float(parts[2])
            lat_max = float(parts[3])
        except ValueError:
            lon_min, lat_min, lon_max, lat_max = -180, -90, 180, 90

        return LonLatRect(lon_min, lat_min, lon_max, lat_max)

    @property
    def default_destination(self):
        """
        The default destination for ERA5 product is
        ``ERA5/<product_name>``>
        """
        return Path("era5")

    def __str__(self):
        """The name of the product that identifies it within pansat."""
        return self.name

    @classmethod
    def open(cls, filename):
        """Opens a given ERA5 product as xarray.

        Args:
            filename(``str``): name of the file to be opened

        Returns:
            datasets(``xarray.Dataset``): xarray dataset object for opened file
        """

        datasets = xarray.open_dataset(filename)

        return datasets


class ERA5Monthly(ERA5Product):
    """

    Child Class of ERA5Product for monthly ERA5 data.

    """

    def __init__(self, levels, variables, domain=None):
        super().__init__(levels, time_step="monthly", variables=variables, domain=domain)


class ERA5Hourly(ERA5Product):
    """

    Child Class of ERA5Product for hourly ERA5 data.

    """

    def __init__(self, levels, variables, domain=None):
        super().__init__(levels, time_step="hourly", variables=variables, domain=domain)



def get_product(product_name: str) -> Product:
    """
    Return ERA5 product object given its string representation.

    Args:
        product_name: The product name as a string.

    Return:
        The corresponding ERA5 product object.
    """
    parts = product_name.split("_")[1:]

    subset = parts[0]
    if parts[1] == "hourly":
        product_class = ERA5Hourly
    else:
        product_class = ERA5Monthly

    var_start = product_name.find("[")
    var_end = product_name.find("]")
    variables = product_name[var_start + 1:var_end].split(",")

    dom_start = product_name.find("[", var_end)
    dom_end = product_name.find("]", dom_start)
    if dom_start > 0:
        coords = product_name[dom_start + 1:dom_end].split(",")
        domain = tuple(map(float, coords))
    else:
        domain = None

    return product_class(subset, variables=variables, domain=domain)
