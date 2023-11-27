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

import xarray as xr
import satpy

import pansat
from pansat import FileRecord, Geometry, TimeRange
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

        if location is not None:
            if location.lower() == "io":
                self._name = "l1b_msg_seviri_io"
            else:
                raise ValueError(
                    "'location' kwarg of MSGSeviriProduct should be None for "
                    " the 0-degree position or 'io' for the Indian Ocean "
                    "location."
                )
        self.filename_regex = re.compile(
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
        if not isinstance(rec, FileRecord):
            rec = FileRecord(local_path(rec))

        filename = rec.filename
        match = self.filename_regex.match(filename)
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
        with TemporaryDirectory() as tmp:
            with ZipFile(rec.local_path, 'r') as zip_ref:
                zip_ref.extractall(tmp)
            files = list(Path(tmp).glob("*.nat"))
            scene = satpy.Scene(files)
            datasets = scene.available_dataset_names()
            scene.load(datasets)

            data = {}

            n_y = None
            n_x = None

            lons, lats = scene.coarsest_area().get_lonlats()

            for name in datasets:
                if name.startswith("HR"):
                    data[name] = (("y_hr", "x_hr",), scene[name].compute().data)
                else:
                    data[name] = (("y", "x",), scene[name].compute().data)
            data["longitude"] = (("y", "x"), lons)
            data["latitude"] = (("y", "x"), lats)

            return xr.Dataset(data)


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
        if not isinstance(rec, FileRecord):
            rec = FileRecord(local_path(rec))
        filename = rec.filename

        match = self.filename_regex.match(filename)
        if match is None:
            raise ValueError(
                f"Given filename '{filename}' does not match the expected "
                f"filename format of MSG Seviri L1B files."
            )
        time = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")
        return TimeRange(time, time + timedelta(minutes=5))


l1b_rs_msg_seviri = MSGSeviriRapidScanL1BProduct()
