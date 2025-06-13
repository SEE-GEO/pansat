"""
pansat.products.satellite.goes
==============================

This module defines the GOES product class, which is used to represent all
products from the GOES series of geostationary satellites.
"""
import re
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Union, List, Optional, Tuple

import numpy as np
from PIL import Image
from pyresample.geometry import AreaDefinition
from pyproj import Proj, transform

import satpy
import xarray as xr

import pansat
from pansat.products import Product, FilenameRegexpMixin
from pansat.exceptions import NoAvailableProvider, MissingInformation
from pansat.file_record import FileRecord
from pansat.geometry import Geometry, lonlats_to_polygon, MultiPolygon
from pansat.time import TimeRange

REGIONS = {"F": "full_disk", "M": "meso_scale_sector", "C": "conus"}


def get_lonlats(data: xr.Dataset) -> Tuple[np.ndarray, np.ndarray]:
    """
    Get longitude and latitude coordinates for GOES observations.
    """
    lon_0 = data["nominal_satellite_subpoint_lon"].data
    height = data["nominal_satellite_height"].data
    goes_proj = Proj(
        proj='geos',
        h=height * 1e3,
        lon_0=lon_0,
        sweep='x',
        a=6378137.0,
        b=6356752.31414,
        unit="m"
    )
    R = 35786023.0
    xx, yy = np.meshgrid(data.x.data, data.y.data)
    lon, lat = goes_proj(R * xx, R * yy, inverse=True)
    return lon, lat


class GOESProduct(FilenameRegexpMixin, Product):
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
                channel_str = "c" + "".join([f"{chan:02}" for chan in self.channel])
        else:
            channel_str = f"c{self.channel:02}"

        region_str = REGIONS[self.region]
        prod_str = self.product_name.lower()

        name = (
            f"{prefix}.l{self.level}_goes{self.series_index}_{prod_str}"
            f"_{channel_str}_{region_str}"
        )
        return name

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
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        match = self.filename_regexp.match(rec.filename)
        start_time = datetime.strptime(match.group(2)[:-1], "%Y%j%H%M%S")
        end_time = datetime.strptime(match.group(3)[:-1], "%Y%j%H%M%S")
        return TimeRange(start_time, end_time)

    def _load_geometry_from_file(self, rec: FileRecord) -> Geometry:
        """
        Parse geometry object representing the spatial coverage of a
        file from a locally available file.

        NOTE: Needs satpy.

        Args:
            rec: A file record pointing to a local file.

        Return:
            A geometry object representing the spatial coverage of the
            file.
        """
        try:
            from satpy import Scene
            scn = Scene(files=[rec.local_path])
            lons, lats = scn.coarsest_area().get_lonlats()
            return lonlats_to_polygon(lons, lats, n_points=8)
        except ImportError:
            raise RuntimeError(
                "Parsing the spatial extent of a GOES meso-scale sector"
                " file requires 'satpy' to be installed."
            )

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
        if self.region.lower() == "M":
            if rec.local_path is not None:
                raise MissingInformation(
                    """
                    Cannot determing the spatial coverage of a GOES meso scale
                    sector file without downloading the file.
                    """
                )
            return self._load_geometry_from_file(rec)

        filename = f"goes_{self.series_index:02}_{REGIONS[self.region]}.json"
        path = Path(__file__).parent / filename
        return MultiPolygon.load(path)


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

        data = xr.open_dataset(path)
        lons, lats = get_lonlats(data)
        data["longitude"] = (("y", "x"), lons)
        data["latitude"] = (("y", "x"), lats)
        return data


    def render_satpy(self, recs: Union[FileRecord, List[FileRecord]], dataset: str, area: Optional[AreaDefinition] = None) -> xr.Dataset:
        """
        Render a given satpy dataset or composite to an image file.

        Args:
            rec: A FileRecord pointing to a local GOES file.
            dataset: The name of the dataset or composite.

        Return:
            A PIL.Image containing the rendered image.

        """
        if not isinstance(recs, list):
            recs = recs
        recs = [FileRecord(rec) if isinstance(rec, (str, Path)) else rec for rec in recs]

        with TemporaryDirectory() as tmp:
            files = [str(rec.local_path) for rec in recs]
            scene = satpy.Scene(files, reader="abi_l1b")
            scene.load([dataset], generate=False)

            if area is not None:
                scene = scene.resample(area)

            img_path = Path(tmp) / "dataset.png"
            scene.save_dataset(dataset, str(img_path))
            img = Image.open(img_path)
            return img


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


l1b_goes_16_rad_rgb_full_disk = GOES16L1BRadiances("F", [1, 2, 3])
l1b_goes_16_rad_all_full_disk = GOES16L1BRadiances("F", list(range(1, 17)))
l1b_goes_16_rad_rgb_conus = GOES16L1BRadiances("C", [1, 2, 3])
l1b_goes_16_rad_all_conus = GOES16L1BRadiances("C", list(range(1, 17)))
l1b_goes_16_rad_rgb_meso_scale_sector = GOES16L1BRadiances("M", [1, 2, 3])
l1b_goes_16_rad_all_meso_scale_sector = GOES16L1BRadiances("M", list(range(1, 17)))

l1b_goes_17_rad_rgb_full_disk = GOES17L1BRadiances("F", [1, 2, 3])
l1b_goes_17_rad_all_full_disk = GOES17L1BRadiances("F", list(range(1, 17)))
l1b_goes_17_rad_rgb_conus = GOES17L1BRadiances("C", [1, 2, 3])
l1b_goes_17_rad_all_conus = GOES17L1BRadiances("C", list(range(1, 17)))
l1b_goes_17_rad_rgb_meso_scale_sector = GOES17L1BRadiances("M", [1, 2, 3])
l1b_goes_17_rad_all_meso_scale_sector = GOES17L1BRadiances("M", list(range(1, 17)))

l1b_goes_18_rad_rgb_full_disk = GOES18L1BRadiances("F", [1, 2, 3])
l1b_goes_18_rad_all_full_disk = GOES18L1BRadiances("F", list(range(1, 17)))
l1b_goes_18_rad_rgb_conus = GOES18L1BRadiances("C", [1, 2, 3])
l1b_goes_18_rad_all_mese_scale_sector = GOES18L1BRadiances("M", list(range(1, 17)))
l1b_goes_18_rad_rgb_mese_scale_sector = GOES18L1BRadiances("M", [1, 2, 3])
l1b_goes_18_rad_all_mese_scale_sector = GOES18L1BRadiances("M", list(range(1, 17)))

for chan in range(1, 17):
    for reg in ["F", "C", "M"]:
        name = f"l1b_goes_16_rad_c{chan:02}_{REGIONS[reg]}"
        globals()[name] = GOES16L1BRadiances(reg, chan)
        name = f"l1b_goes_17_rad_c{chan:02}_{REGIONS[reg]}"
        globals()[name] = GOES17L1BRadiances(reg, chan)
        name = f"l1b_goes_18_rad_c{chan:02}_{REGIONS[reg]}"
        globals()[name] = GOES18L1BRadiances(reg, chan)
