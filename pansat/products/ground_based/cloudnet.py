"""
pansat.products.ground_based
============================

This module defines products provided by Cloudnet. The official
data portal for CloudNet is available
`here <https://cloudnet.fmi.fi/>`_.
"""
from datetime import datetime
from pathlib import Path
import re

import pansat
from pansat import TimeRange, FileRecord
from pansat.exceptions import NoAvailableProvider
from pansat.products import Product, FilenameRegexpMixin
from pansat.download import providers


class CloudnetProduct(FilenameRegexpMixin, Product):
    """
    This class represents all Cloudnet products. Since Cloudnet data
    is derived from specific stations a product can have an associated
    location in which case only data of the product collected at the
    given location is represetned by the product.
    """

    def __init__(self, level, product_name, description, location=None):
        """
        Args:
            level: A string representing the procssing level of the product.
            product_name: The name of the product.
            description: A string describing the product.
            location: An optional string specifying the a specific
                 Cloudnet location.
        """
        self.level = level
        self.product_name = product_name
        self._description = description
        self.location = location
        Product.__init__(self)

        if location is not None:
            self.filename_regexp = re.compile(rf"(\d{{8}})_{location}_[-\w\d]*.nc")
        else:
            self.filename_regexp = re.compile(rf"(\d{{8}})_([\w-]*)_[-\w\d]*.nc")

    @property
    def name(self):
        """
        The name of the product used to identify it within pansat.
        """
        module = Path(__file__).parent
        root = Path(pansat.products.__file__).parent
        prefix = str(module.relative_to(root)).replace("/", ".")
        name = f"l{self.level.lower()}_{self.product_name}"
        return ".".join([prefix, name])

    @property
    def description(self):
        return self._description


    def get_temporal_coverage(self, rec: FileRecord) -> TimeRange:
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a Cloudnet product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        if isinstance(rec, str):
            rec = FileRecord(rec)

        parts = filename.split("/")
        if len(parts) > 1:
            filename = parts[-1]
        path = Path(filename)
        match = self.filename_regexp.match(path.name)

        date_string = match.group(1)
        date = datetime.strptime(date_string, "%Y%m%d")

        return TimeRange(date, date + timedelta(hours=23, minutes=59, seconds=59))

    def get_spatial_coverage(self, rec: FileRecord):

        if self.location is None:
            return LonLatRect(-180, -90, 180, 90)


    @property
    def default_destination(self):
        """
        The default location for Cloudnet products is cloudnet/<product_name>
        """
        return Path("cloudnet") / Path(self.product_name)


    def open(self, filename):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The Cloudnet file to open.
        """
        return xr.load_dataset(filename)


l2_iwc = CloudnetProduct("l2", "iwc", "IWC calculated from Z-T method.")
l1_radar = CloudnetProduct("l1", "radar", "L1b radar data.")


def retrieve_site_locations():
    """
    Retrieves all site locations from the cloudnet portal.
    """
    url = "https://cloudnet.fmi.fi/api/sites"
    req = cache.get(url)
    req.raise_for_status()
    sites = json.loads(req.text)
    return sites
