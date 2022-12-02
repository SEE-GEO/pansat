from datetime import datetime
from pathlib import Path
import re

from pansat.exceptions import NoAvailableProvider
from pansat.products.product import Product
from pansat.download import providers


class CloudNetProduct(Product):
    """
    """
    def __init__(self, product_name, description):
        self.product_name = product_name
        self._description = description
        self.filename_regexp = re.compile(
            rf"(\d{{8}})_([\w-]*)_{self.product_name}[-\w]*.nc"
        )

    @property
    def description(self):
        return self._description

    def matches(self, filename):
        """
        Determines whether a given filename matches the pattern used for
        the product.

        Args:
            filename(``str``): The filename

        Return:
            True if the filename matches the product, False otherwise.
        """
        return self.filename_regexp.match(filename)

    def filename_to_date(self, filename):
        """
        Extract timestamp from filename.

        Args:
            filename(``str``): Filename of a GPM product.

        Returns:
            ``datetime`` object representing the timestamp of the
            filename.
        """
        parts = filename.split("/")
        if len(parts) > 1:
            filename = parts[-1]
        print(rf"(\d{{8}})_\w*_{self.product_name}[-\w]*.nc")
        path = Path(filename)
        match = self.filename_regexp.match(path.name)
        date_string = match.group(1)
        date = datetime.strptime(date_string, "%Y%m%d")
        return date

    def _get_provider(self):
        """ Find a provider that provides the product. """
        available_providers = [
            p
            for p in providers.ALL_PROVIDERS
            if str(self) in p.get_available_products()
        ]
        if not available_providers:
            raise NoAvailableProvider(
                f"Could not find a provider for the" f" product {self.name}."
            )
        return available_providers[0]

    @property
    def default_destination(self):
        """
        The default location for CloudNet products is cloudnet/<product_name>
        """
        return Path("cloudnet") / Path(self.product_name)

    def __str__(self):
        s = f"CloudNet_{self.product_name}"
        return s

    def download(self, start_time, end_time, destination=None, provider=None):
        """
        Download data product for given time range.

        Args:
            start_time(``datetime``): ``datetime`` object defining the start
                 date of the time range.
            end_time(``datetime``): ``datetime`` object defining the end date
                 of the of the time range.
            destination(``str`` or ``pathlib.Path``): The destination where to
                 store the output data.
        """
        if not provider:
            provider = self._get_provider()

        if not destination:
            destination = self.default_destination
        else:
            destination = Path(destination)
        destination.mkdir(parents=True, exist_ok=True)
        provider = provider(self)

        return provider.download(start_time, end_time, destination)

    def open(self, filename):
        """
        Open file as xarray dataset.

        Args:
            filename(``pathlib.Path`` or ``str``): The GPM file to open.
        """
        return xr.load_dataset(filename)


l2_iwc = CloudNetProduct("iwc", "IWC calculated from Z-T method.")
