"""
==================================
pansat.products.satellite.meteosat
==================================

This module provides product classes and object for satellite products
derived from the Meteosat Second Generation (MSG) satellites.
"""
from pathlib import Path
import re
from zipfile import ZipFile
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
from typing import Optional

from PIL import Image
from pyresample.geometry import AreaDefinition
import satpy
import xarray as xr

import pansat
from pansat import FileRecord, Geometry, TimeRange
from pansat.geometry import LonLatRect
import pansat.download.providers as providers
from pansat.products import Product, FilenameRegexpMixin
from pansat.exceptions import NoAvailableProvider


def _extract_file(filename):
    """
    Extracts the data file from the .zip archive downloaded from the
    provider and deletes the original archive.
    """
    path = Path(filename)
    data = path.stem + ".nat"
    with ZipFile(path) as archive:
        archive.extract(data, path=path.parent)
    path.unlink()
    return path.parent / data


class MSGSeviriL1BProduct(FilenameRegexpMixin, Product):
    """
    Base class for Meteosat Second Generation (MSG) SEVIRI L1B products.
    """

    def __init__(self, location=None):
        """
        Create MSG Seviri L1B product.

        Args:
            location: None for the 0-degree position of MSG and "IO" for the
                 the position over the Indian Ocean.

        """
        self._name = "l1b_msg_seviri"
        Product.__init__(self)

        if location is not None:
            if location.lower() == "io":
                self._name = "l1b_msg_seviri_io"
            else:
                raise ValueError(
                    "'location' kwarg of MSGSeviriProduct should be None for "
                    " the 0-degree position or 'io' for the Indian Ocean "
                    "location."
                )
        self.filename_regexp = re.compile(
            "MSG\d-SEVI-MSG\d*-0100-NA-(\d{14})\.\d*Z-NA(.nat)?"
        )

    @property
    def name(self):
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        return prefix + "." + self._name

    @property
    def default_destination(self):
        return Path("msg")

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        filename = rec.filename
        match = self.filename_regexp.match(filename)
        if match is None:
            raise ValueError(
                f"Given filename '{filename}' does not match the expected "
                f"filename format of MSG Seviri L1B files."
            )
        time = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
        # Time stamp corresponds to last scan time.
        return TimeRange(time - timedelta(minutes=15), time)

    def get_spatial_coverage(self, rec:FileRecord) -> Geometry:
        return LonLatRect(-180, -90, 180, 90)

    def open(self, rec: FileRecord) -> xr.Dataset:
        """
        Open local SERVIRI file and load data into xarray.Dataset.

        Args:
            rec: A FileRecord pointing to a local SEVIRI file.

        Return:
            An 'xarray.Dataset' containing the loaded data.

        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        with TemporaryDirectory() as tmp:
            with ZipFile(rec.local_path, 'r') as zip_ref:
                zip_ref.extractall(tmp)
            files = list(Path(tmp).glob("*.nat"))
            scene = satpy.Scene(files, reader="seviri_l1b_native")
            datasets = scene.available_dataset_names()
            scene.load(datasets)

            data = {}

            n_y = None
            n_x = None

            dim_ind = 0
            for area, datasets in scene.iter_by_area():
                lons, lats = area.get_lonlats()

                for name in datasets:
                    name = name.get("name")
                    data[name] = ((f"y_{dim_ind}", f"x_{dim_ind}",), scene[name].compute().data)

                data[f"longitude_{dim_ind}"] = ((f"y_{dim_ind}", f"x_{dim_ind}"), lons)
                data[f"latitude_{dim_ind}"] = ((f"y_{dim_ind}", f"x_{dim_ind}"), lats)
                dim_ind += 1

            return xr.Dataset(data)

    def open_satpy(self, rec: FileRecord) -> satpy.Scene:
        """
        Open observations as satpy.Scene.

        Args:
            rec: A FileRecord pointing to a local SEVIRI file.

        Return:
            A satpy.Scene containing the loaded imagery.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        with TemporaryDirectory() as tmp:
            with ZipFile(rec.local_path, 'r') as zip_ref:
                zip_ref.extractall(tmp)
            files = list(Path(tmp).glob("*.nat"))
            scene = satpy.Scene(files, reader="seviri_l1b_native")
            scene.load([dataset])

            if area is not None:
                scene = scene.resample(area)

            img_path = Path(tmp) / "dataset.png"
            scene.save_dataset(dataset, str(img_path))
            img = Image.open(img_path)
            return img


    def render_satpy(self, rec: FileRecord, dataset: str, area: Optional[AreaDefinition] = None) -> xr.Dataset:
        """
        Render a given satpy dataset or composite to an image file.

        Args:
            rec: A FileRecord pointing to a local SEVIRI file.
            dataset: The name of the dataset or composite.

        Return:
            A PIL.Image containing the rendered image.

        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        with TemporaryDirectory() as tmp:
            with ZipFile(rec.local_path, 'r') as zip_ref:
                zip_ref.extractall(tmp)
            files = list(Path(tmp).glob("*.nat"))
            scene = satpy.Scene(files, reader="seviri_l1b_native")
            scene.load([dataset])

            if area is not None:
                scene = scene.resample(area)

            img_path = Path(tmp) / "dataset.png"
            scene.save_dataset(dataset, str(img_path))
            img = Image.open(img_path)
            return img


l1b_msg_seviri = MSGSeviriL1BProduct()
l1b_msg_seviri_io = MSGSeviriL1BProduct(location="io")


class MSGSeviriRapidScanL1BProduct(MSGSeviriL1BProduct):
    """
    Base class for Meteosat Second Generation (MSG) SEVIRI L1B products.
    """

    def __init__(self, location=None):
        """
        Specialization of L1B MSG SEVIRI product for rapid scan

        Args:
            location: None for the 0-degree position of MSG and "IO" for the
                 the position over the Indian Ocean.

        """
        super().__init__(location=location)
        self._name = "l1b_rs_msg_seviri"
        self.filename_regexp = re.compile(
            "MSG-R\d-SEVI-MSG\d*-0100-NA-(\d{14})\.\d*Z-NA(.nat)?"
        )

        if location is not None:
            if location.lower() == "io":
                self._name = "l1b_rs_msg_seviri_io"
            else:
                raise ValueError(
                    "'location' kwarg of MSGSeviriProduct should be None for "
                    " the 0-degree position or 'io' for the Indian Ocean "
                    "location."
                )

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)
        filename = rec.filename

        match = self.filename_regexp.match(filename)
        if match is None:
            raise ValueError(
                f"Given filename '{filename}' does not match the expected "
                f"filename format of MSG Seviri L1B files."
            )
        time = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
        return TimeRange(time, time + timedelta(minutes=5))


l1b_rs_msg_seviri = MSGSeviriRapidScanL1BProduct()
