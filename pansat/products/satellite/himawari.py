"""
pansat.products.satellite.himawari
===================================

This module defines the Himawari product class, which is used to represent all
products from the Himawari series of geostationary satellites.
"""
import re
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Union, List, Optional

import numpy as np
from PIL import Image
from pyresample.geometry import AreaDefinition
import satpy
import xarray as xr

import pansat
from pansat.time import TimeRange
import pansat.download.providers as providers
from pansat.file_record import FileRecord
from pansat.products import Product, FilenameRegexpMixin
from pansat.geometry import Geometry, LonLatRect
from pansat.exceptions import NoAvailableProvider


class HimawariProduct(FilenameRegexpMixin, Product):
    """
    Base class for products from any of the currently operational
    Himawari satellites.

    Attributes:
        series_index(``int``): Index identifying the Himawari satellite
            in the Himawari seris.
        level(``int``): The operational level of the product.
        name(``str``): The name of the product.
        channel(``int``): The channel index.
    """

    def __init__(self, series_index, channel):
        self.series_index = series_index
        self.channel = channel
        if type(channel) == list:
            channels = "B(" + "|".join([f"{c:02}" for c in channel]) + ")"
        else:
            channels = f"B{channel:02}"

        self.filename_regexp = re.compile(
            rf"HS_H{self.series_index:02}_(\d{{8}})_(\d{{4}})_{channels}_FLDK_R\d\d_S\d{{4}}.DAT.bz2"
        )


    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a GOES product.

        Returns:
            ``datetime`` object representing the timestamp of the
                filename.
        """
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(1) + match.group(2)
        date = datetime.strptime(date_string, "%Y%m%d%H%M")
        return date

    @property
    def default_destination(self):
        """
        The default destination for GOES product is
        ``GOES-<index>/<product_name>``>
        """
        return Path(f"Himawari-{self.series_index:02}")

    @property
    def name(self):
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


        name = (
            f"{prefix}.himawari.l1b_himawari{self.series_index}_all"
        )
        return name


    def open(self, filename):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The GOES file to open.
        """
        return xarray.open_dataset(filename)


    def get_temporal_coverage(self, rec: FileRecord):
        """
        Determine the temporal coverage of a HIMAWARI file.

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
        start_time = datetime.strptime(match.group(1) + match.group(2), "%Y%m%d%H%M")
        end_time = start_time + np.timedelta64(10, "m")
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
        return LonLatRect(-180, -90, 180, 90)


    def render_satpy(self, recs: Union[FileRecord, List[FileRecord]], dataset: str, area: Optional[AreaDefinition] = None) -> xr.Dataset:
        """
        Render a given satpy dataset or composite to an image file.

        Args:
            rec: A FileRecord pointing to a local HIMAWARI file.
            dataset: The name of the dataset or composite.

        Return:
            A PIL.Image containing the rendered image.

        """
        if not isinstance(recs, list):
            recs = recs
        recs = [FileRecord(rec) if isinstance(rec, (str, Path)) else rec for rec in recs]

        with TemporaryDirectory() as tmp:
            files = [str(rec.local_path) for rec in recs]
            scene = satpy.Scene(files, reader="ahi_hsd")
            scene.load([dataset], generate=False)

            scene = scene.resample(scene.coarsest_area())

            if area is not None:
                scene = scene.resample(area)

            img_path = Path(tmp) / "dataset.png"
            scene.save_dataset(dataset, str(img_path))
            img = Image.open(img_path)
            return img


l1b_himawari8_all = HimawariProduct(
    8,
    list(range(1, 17))
)
l1b_himawari9_all = HimawariProduct(
    9,
    list(range(1, 17))
)
