"""
pansat.products.satellite.ccic
==============================

Implements product classes for the Chalmers Cloud Ice Climatology (CCIC).
"""
from datetime import datetime, timedelta
from pathlib import Path
import re
from typing import Optional

import pansat
from pansat import FileRecord, TimeRange
from pansat import geometry
from pansat.products import Product, FilenameRegexpMixin


class CCICProduct(FilenameRegexpMixin, Product):
    """
    Product class for instantaneous CCIC retrievals.
    """
    def __init__(self, variant):
        self.variant = variant.lower()
        self.filename_regexp = re.compile(
            rf"ccic_{variant}_(\d{{14}}).(nc|zarr)"
        )
        Product.__init__(self)


    @property
    def name(self) -> str:
        """
        The product name that uniquely identifies the product within pansat.
        """
        module = Path(__file__).with_suffix("")
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        name = f"ccic_{self.variant}"
        return ".".join([prefix, name])

    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Implements interface to extract temporal coverage of file.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        match = self.filename_regexp.match(rec.filename)
        if match is None:
            raise RuntimeError(
                "Provided file record does not match the CCIC naming pattern."
            )

        if self.variant == "cpcir":
            t_res = timedelta(minutes=30)
        else:
            t_res = timedelta(hours=3)

        time = datetime.strptime(match.group(1), "%Y%m%d%H%M%S")

        return TimeRange(time - 0.5 * t_res, time + 0.5 * t_res)


    def get_spatial_coverage(self, rec: FileRecord) -> geometry.Geometry:
        """
        Implements interface to extract spatial coverage of file.
        """
        return geometry.LonLatRect(-180, -90, 180, 90)


    @property
    def default_destination(self):
        """
        The default destination for GPM product is
        ``GPM/<product_name>``>
        """
        return Path("ccic") / self.variant

    def __str__(self):
        return self.name

    def open(self, rec: FileRecord, slcs: Optional[dict[str, slice]] = None):
        """
        Open file as xarray dataset.

        Args:
            rec: A FileRecord whose local_path attribute points to a local
                GPM file to open.
            slcs: An optional dictionary of slices to use to subset the
                data to load.

        Return:
            An xarray.Dataset containing the loaded data.
        """
        if isinstance(rec, (str, Path)):
            rec = FileRecord(rec)

        dataset = xr.open_dataset(rec.local_path)
        if slcs is None:
            return dataset
        return dataset[slcs]


ccic_gridsat = CCICProduct(variant="gridsat")
ccic_cpcir = CCICProduct(variant="cpcir")
