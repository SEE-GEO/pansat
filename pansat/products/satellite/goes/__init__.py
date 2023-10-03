"""
pansat.products.satellite.goes
==============================

This module defines the GOES product class, which is used to represent all
products from the GOES series of geostationary satellites.
"""
import re
from datetime import datetime
from pathlib import Path
from typing import Union, List

import xarray as xr

import pansat
from pansat.products import Product
from pansat.exceptions import NoAvailableProvider
from pansat.file_record import FileRecord
from pansat.geometry import Geometry
from pansat.time import TimeRange

REGIONS = {"F": "full_disk", "M": "meso_scale_sector", "C": "conus"}


class GOESProduct(Product):
    """
    Base class for products from any of the currently operational
    GOES satellites (GOES 16, 17 and 18).
    """

    def __init__(
        self,
        level: str,
        series_index: int,
        instrument: str,
        product_name: str,
        region: str,
        channel: Union[int, List[int]],
    ):
        """
        Args:
            level: Level string specifying the processing level, i.e.,
                '1b' for level 1B products.
            series_index: Series index identifying the satellite.
            instrument: Name of the underlying instrument.
            product_name: Name of the GOES product.
            region: Single letter specifying the GOES region.
            channel: Single integer of list of integers specifying the
                one or multiple channels.
        """
        self.level = level
        self.series_index = series_index
        self.instrument = instrument
        self.product_name = product_name
        self.region = region
        self.channel = channel
        instr_str = instrument.upper()

        if isinstance(channel, list):
            channels = "(" + "|".join([f"{c:02}" for c in channel]) + ")"

            self.filename_regexp = re.compile(
                rf"OR_{instr_str}-L{level}-{product_name}{region}-\w\wC{channels}"
                r"_\w\w\w_s(\d*)_e(\d*)_c(\d*).nc"
            )
        else:
            self.filename_regexp = re.compile(
                rf"OR_{instr_str}-L{level}-{product_name}{region}-\w\wC"
                rf"({self.channel:02})_\w\w\w_s(\d*)_e(\d*)_c(\d*).nc"
            )
        super().__init__()

    @property
    def default_destination(self):
        name = f"goes_{self.series_index:02}"
        return Path(name)

    @property
    def name(self):
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")

        if isinstance(self.channel, list):
            if len(self.channel) == 16:
                channel_str = "all"
            elif set(self.channel) == {1, 2, 3}:
                channel_str = "rgb"
            else:
                channel_str = "".join([f"{chan:02}" for chan in self.channel])
        else:
            channel_str = f"{self.channel:02}"

        region_str = REGIONS[self.region]
        prod_str = self.product_name.lower()

        name = (
            f"{prefix}.l{self.level}_goes{self.series_index}_{prod_str}"
            f"_{region_str}_{channel_str}"
        )
        return name

    def matches(self, rec: FileRecord) -> bool:
        """
        Determines whether a given filename matches the pattern used for
        the product.

        Args:
            rec: A file record pointing to a local or remote file.

        Return:
            True if the filename matches the product, False otherwise.
        """
        return self.filename_regexp.match(rec.filename) is not None

    def get_temporal_coverage(self, rec: FileRecord):
        """
        Determine the temporal coverage of a GOES file.

        Args:
            rec: A 'FileRecord' object pointing to the file from which to
                deduce the temporal coverage.

        Return:
            A 'TimeRange' object representing the time range covered by the
            data file.
        """
        match = self.filename_regexp.match(rec.filename)
        start_time = datetime.strptime(match.group(2)[:-1], "%Y%j%H%M%S")
        end_time = datetime.strptime(match.group(3)[:-1], "%Y%j%H%M%S")
        return TimeRange(start_time, end_time)

    def get_spatial_coverage(self, rec: FileRecord) -> Geometry:
        """
        Determine the spatial coverage of a data file.

        Args:
            rec: A file record representing the file of which to determine
                the spatial extent.

        Return:
            A 'Geometry' object representing the spatial that the given
            datafile covers.
        """
        pass

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open file as xarray Dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The GOES file to open.

        Return:
            An xarray.Dataset containing the loaded product data.
        """
        path = rec.local_path
        if path is None:
            raise RuntimeError(
                f"The file {rec.filename} does not seem to be available locally."
            )
        return xr.open_dataset(path)


class GOES16L1BRadiances(GOESProduct):
    """
    Class representing GOES16 L1 radiance products.
    """

    def __init__(self, region, channel):
        super().__init__("1b", 16, "ABI", "Rad", region, channel)


class GOES17L1BRadiances(GOESProduct):
    """
    Class representing GOES 17 L1 radiance products.
    """

    def __init__(self, region, channel):
        super().__init__("1b", 17, "ABI", "Rad", region, channel)


class GOES18L1BRadiances(GOESProduct):
    """
    Class representing GOES 18 L1 radiance products.
    """

    def __init__(self, region, channel):
        super().__init__("1b", 18, "ABI", "Rad", region, channel)


l1b_goes_16_rad_c01_full_disk = GOES16L1BRadiances("F", 1)
l1b_goes_16_rad_c02_full_disk = GOES16L1BRadiances("F", 2)
l1b_goes_16_rad_c03_full_disk = GOES16L1BRadiances("F", 3)
l1b_goes_16_rad_c04_full_disk = GOES16L1BRadiances("F", 4)
l1b_goes_16_rad_rgb_full_disk = GOES16L1BRadiances("F", [1, 2, 3])
l1b_goes_16_rad_all_full_disk = GOES16L1BRadiances("F", list(range(1, 17)))
l1b_goes_16_rad_c01_conus = GOES16L1BRadiances("C", 1)
l1b_goes_16_rad_c02_conus = GOES16L1BRadiances("C", 2)
l1b_goes_16_rad_c03_conus = GOES16L1BRadiances("C", 3)
l1b_goes_16_rad_c04_conus = GOES16L1BRadiances("C", 4)
l1b_goes_16_rad_rgb_conus = GOES16L1BRadiances("C", [1, 2, 3])
l1b_goes_16_rad_all_conus = GOES16L1BRadiances("C", list(range(1, 17)))
l1b_goes_16_rad_c01_meso_scale_sector = GOES16L1BRadiances("M", 1)
l1b_goes_16_rad_c02_meso_scale_sector = GOES16L1BRadiances("M", 2)
l1b_goes_16_rad_c03_meso_scale_sector = GOES16L1BRadiances("M", 3)
l1b_goes_16_rad_c04_meso_scale_sector = GOES16L1BRadiances("M", 4)
l1b_goes_16_rad_rgb_meso_scale_sector = GOES16L1BRadiances("M", [1, 2, 3])
l1b_goes_16_rad_all_meso_scale_sector = GOES16L1BRadiances("M", list(range(1, 17)))

l1b_goes_17_rad_c01_full_disk = GOES17L1BRadiances("F", 1)
l1b_goes_17_rad_c02_full_disk = GOES17L1BRadiances("F", 2)
l1b_goes_17_rad_c03_full_disk = GOES17L1BRadiances("F", 3)
l1b_goes_17_rad_c04_full_disk = GOES17L1BRadiances("F", 4)
l1b_goes_17_rad_rgb_full_disk = GOES17L1BRadiances("F", [1, 2, 3])
l1b_goes_17_rad_all_full_disk = GOES17L1BRadiances("F", list(range(1, 17)))
l1b_goes_17_rad_c01_conus = GOES17L1BRadiances("C", 1)
l1b_goes_17_rad_c02_conus = GOES17L1BRadiances("C", 2)
l1b_goes_17_rad_c03_conus = GOES17L1BRadiances("C", 3)
l1b_goes_17_rad_c04_conus = GOES17L1BRadiances("C", 4)
l1b_goes_17_rad_rgb_conus = GOES17L1BRadiances("C", [1, 2, 3])
l1b_goes_17_rad_all_conus = GOES17L1BRadiances("C", list(range(1, 17)))
l1b_goes_17_rad_c01_meso_scale_sector = GOES17L1BRadiances("M", 1)
l1b_goes_17_rad_c02_meso_scale_sector = GOES17L1BRadiances("M", 2)
l1b_goes_17_rad_c03_meso_scale_sector = GOES17L1BRadiances("M", 3)
l1b_goes_17_rad_c04_meso_scale_sector = GOES17L1BRadiances("M", 4)
l1b_goes_17_rad_rgb_meso_scale_sector = GOES17L1BRadiances("M", [1, 2, 3])
l1b_goes_17_rad_all_meso_scale_sector = GOES17L1BRadiances("M", list(range(1, 17)))

l1b_goes_18_rad_c01_full_disk = GOES18L1BRadiances("F", 1)
l1b_goes_18_rad_c02_full_disk = GOES18L1BRadiances("F", 2)
l1b_goes_18_rad_c03_full_disk = GOES18L1BRadiances("F", 3)
l1b_goes_18_rad_c04_full_disk = GOES18L1BRadiances("F", 4)
l1b_goes_18_rad_rgb_full_disk = GOES18L1BRadiances("F", [1, 2, 3])
l1b_goes_18_rad_all_full_disk = GOES18L1BRadiances("F", list(range(1, 17)))
l1b_goes_18_rad_c01_conus = GOES18L1BRadiances("C", 1)
l1b_goes_18_rad_c02_conus = GOES18L1BRadiances("C", 2)
l1b_goes_18_rad_c03_conus = GOES18L1BRadiances("C", 3)
l1b_goes_18_rad_c04_conus = GOES18L1BRadiances("C", 4)
l1b_goes_18_rad_rgb_conus = GOES18L1BRadiances("C", [1, 2, 3])
l1b_goes_18_rad_all_mese_scale_sector = GOES18L1BRadiances("M", list(range(1, 17)))
l1b_goes_18_rad_c01_mese_scale_sector = GOES18L1BRadiances("M", 1)
l1b_goes_18_rad_c02_mese_scale_sector = GOES18L1BRadiances("M", 2)
l1b_goes_18_rad_c03_mese_scale_sector = GOES18L1BRadiances("M", 3)
l1b_goes_18_rad_c04_mese_scale_sector = GOES18L1BRadiances("M", 4)
l1b_goes_18_rad_rgb_mese_scale_sector = GOES18L1BRadiances("M", [1, 2, 3])
l1b_goes_18_rad_all_mese_scale_sector = GOES18L1BRadiances("M", list(range(1, 17)))
