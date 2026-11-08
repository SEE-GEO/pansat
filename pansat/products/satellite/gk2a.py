"""
pansat.products.satellite.gk2a
==============================

This module defines the GK2A product class, which is used to represent L1B
observations from the GEO-KOMPSAT-2A satellite.
"""
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Union, List, Optional, Tuple

import numpy as np
from PIL import Image
from pyresample.geometry import AreaDefinition
from pyproj import Proj, transform

import satpy
import pyresample.geometry
import xarray as xr

import pansat
from pansat.products import Product, FilenameRegexpMixin
from pansat.exceptions import NoAvailableProvider, MissingInformation
from pansat.file_record import FileRecord
from pansat.geometry import Geometry, lonlats_to_polygon, MultiPolygon, LonLatRect
from pansat.time import TimeRange


AMI_CHANNELS = [
    "VI004",
    "VI005",
    "VI006",
    "VI008",
    "NR013",
    "NR016",
    "SW038",
    "WV063",
    "WV069",
    "WV073",
    "IR087",
    "IR096",
    "IR105",
    "IR112",
    "IR123",
    "IR133"
]


class GK2AProduct(FilenameRegexpMixin, Product):
    """
    Base class for products from any of the currently operational
    GOES satellites (GOES 16, 17 and 18).
    """

    def __init__(
        self,
        region: str,
        channel: Union[str, List[str]],
    ):
        """
        Args:
            region: Single letter specifying the GOES region.
            channel: The name of specific channel of list of channels.
        """
        self.region = region
        self.channel = channel

        if isinstance(channel, list):
            channels = "(" + "|".join([chan.lower() for chan in channel]) + ")"
        else:
            channels = channel.lower()
        self.filename_regexp = re.compile(
            rf"gk2a_ami_le1b_{channels}_{region.lower()}\d\d\dge_(\d{{12}}).nc"
        )
        super().__init__()

    @property
    def default_destination(self):
        name = f"gk2a"
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
            elif set(self.channel) == ["VI004", "VI005", "VI006"]:
                channel_str = "rgb"
            else:
                channel_str = "c" + "".join([f"{chan:02}" for chan in self.channel])
        else:
            channel_str = self.channel.lower()

        region_str = self.region

        name = (
            f"{prefix}.gk2a.l1b_gk2a_{channel_str}_{region_str}"
        )
        return name

    def matches(self, name):
        return super().matches(name)


    def get_spatial_coverage(self, rec: FileRecord):
        """
        Approximate spatial coverage of the GK2A observations.
        """
        return LonLatRect(40, -60, 160, 60)

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
        nominal_time = datetime.strptime(match.group(1), "%Y%m%d%H%M")
        return TimeRange(
            nominal_time - timedelta(minutes=5),
            nominal_time + timedelta(minutes=5)
        )

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open file as xarray Dataset.

        Args:
            rec(``FileRecord``): A file record object pointing to the local file to open.

        Return:
            An xarray.Dataset containing the loaded product data.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

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

    @contextmanager
    def open_remote(self, rec: FileRecord) -> xr.Dataset:
        """
        Open file as xarray Dataset.

        Args:
            rec(``FileRecord``): A file record object with the 'cloud_path' attribute set.

        Return:
            An xarray.Dataset containing the loaded product data.
        """
        cloud_path = rec.cloud_path
        if cloud_path is None:
            raise RuntimeError(
                f"The file {rec.filename} does not have an associated cloud path."
            )
        try:
            with rec.provider.open_remote(rec) as remote_data:
                data = xr.open_dataset(remote_data, engine="h5netcdf")
                yield data
        finally:
            data.close()


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


l1b_gk2a_all_fd = GK2AProduct("fd", AMI_CHANNELS)
l1b_gk2a_rgb_fd = GK2AProduct("fd", AMI_CHANNELS[:3])
for chan in AMI_CHANNELS:
    name = f"l1b_gk2a_{chan.lower()}_fd"
    globals()[name] = GK2AProduct("fd", chan)
