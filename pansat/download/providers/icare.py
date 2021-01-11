"""
pansat.download.providers.icare
===============================

This module providers the ``IcareProvider`` class, which implementes a data provider
class for downloading data from the
`Icare datacenter <https://www.icare.univ-lille.fr/>`_.
"""
from datetime import datetime
from ftplib import FTP
from pansat.download.providers.discrete_provider import DiscreteProvider
from pansat.download.accounts import get_identity

ICARE_PRODUCTS = {
    "CloudSat_1B-CPR": ["SPACEBORNE", "CLOUDSAT", "1B-CPR"],
    "CloudSat_2B-CLDCLASS": ["SPACEBORNE", "CLOUDSAT", "2B-CLDCLASS"],
    "CloudSat_2B-CLDCLASS-LIDAR": ["SPACEBORNE", "CLOUDSAT", "2B-CLDCLASS-LIDAR"],
    "CloudSat_2B-CWC-RO": ["SPACEBORNE", "CLOUDSAT", "2B-CWC-RO"],
    "CloudSat_2B-CWC-RVOD": ["SPACEBORNE", "CLOUDSAT", "2B-CWC-RVOD"],
    "CloudSat_2B-FLXHR": ["SPACEBORNE", "CLOUDSAT", "2B-FLXHR"],
    "CloudSat_2B-FLXHR-LIDAR": ["SPACEBORNE", "CLOUDSAT", "2B-FLXHR-LIDAR"],
    "CloudSat_2B-GEOPROF": ["SPACEBORNE", "CLOUDSAT", "2B-GEOPROF"],
    "CloudSat_2B-GEOPROF-LIDAR": ["SPACEBORNE", "CLOUDSAT", "2B-GEOPROF-LIDAR"],
    "CloudSat_2B-TAU": ["SPACEBORNE", "CLOUDSAT", "2B-TAU"],
    "CloudSat_2C-ICE": ["SPACEBORNE", "CLOUDSAT", "2B-ICE"],
    "CloudSat_2C-PRECIP-COLUMN": ["SPACEBORNE", "CLOUDSAT", "2B-PRECIP-COLUMN"],
    "CloudSat_2C-RAIN-PROFILE": ["SPACEBORNE", "CLOUDSAT", "2B-PRECIP-COLUMN"],
    "CloudSat_2C-SNOW-PROFILE": ["SPACEBORNE", "CLOUDSAT", "2B-GEOPROF-LIDAR"],
    "Calipso_333mCLay": ["SPACEBORNE", "CALIOP", "333mCLay"],
    "Calipso_01kmCLay": ["SPACEBORNE", "CALIOP", "01kmCLay"],
    "Calipso_05kmAPro": ["SPACEBORNE", "CALIOP", "05kmAPro"],
    "Dardar_DARDAR-CLOUD": ["SPACEBORNE", "MULTI_SENSOR", "DARDAR-CLOUD"],
    "Dardar_DARDAR_CLOUD": ["SPACEBORNE", "MULTI_SENSOR", "DARDAR_CLOUD"],
}


class IcareProvider(DiscreteProvider):
    """
    Base class for data products available from the ICARE ftp server.
    """

    base_url = "ftp.icare.univ-lille1.fr"

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

            product(``Product``): Product class object with specific product for ICARE

        """
        if str(product) not in ICARE_PRODUCTS:
            available_products = list(ICARE_PRODUCTS.keys())
            raise ValueError(
                f"The product {product} is  not a available from the ICARE data"
                f" provider. Currently available products are: "
                f"{available_products}."
            )
        super().__init__(product)
        self.product_path = "/".join(ICARE_PRODUCTS[str(product)])
        self.cache = {}

    def _ftp_listing_to_list(self, path, item_type=int):
        """
        Retrieve directory content from ftp listing as list.

        Args:

            path(``str``): The path from which to retrieve the ftp listing.

            item_type(``type``): Type constructor to apply to the elements of the
                listing. To retrieve a list of strings use t = str.

        Return:

            A list containing the content of the ftp directory.

        """
        if not path in self.cache:
            with FTP(IcareProvider.base_url) as ftp:
                user, password = get_identity("Icare")
                ftp.login(user=user, passwd=password)
                try:
                    ftp.cwd(path)
                except:
                    raise Exception(
                        "Can't find product folder "
                        + path
                        + "on the ICARE ftp server. Are you sure this is"
                        "a ICARE multi sensor product?"
                    )
                listing = ftp.nlst()
            listing = [item_type(l) for l in listing]
            self.cache[path] = listing
        return self.cache[path]

    @classmethod
    def get_available_products(cls):
        return ICARE_PRODUCTS.keys()

    def get_files_by_day(self, year, day):
        """
        Return all files from given year and julian day.

        Args:
            year(``int``): The year from which to retrieve the filenames.
            day(``int``): Day of the year of the data from which to retrieve the
                the filenames.

        Return:
            List of the filenames of this product on the given day.
        """
        day_str = str(day)
        day_str = "0" * (3 - len(day_str)) + day_str
        date = datetime.strptime(str(year) + str(day_str), "%Y%j")
        path = "/".join([self.product_path, str(year), date.strftime("%Y_%m_%d")])
        listing = self._ftp_listing_to_list(path, str)
        files = [name for name in listing if name[-3:] == "hdf"]
        return files

    def download_file(self, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        date = self.product.filename_to_date(filename)
        path = "/".join([self.product_path, str(date.year), date.strftime("%Y_%m_%d")])

        user, password = get_identity("Icare")
        with FTP(self.base_url) as ftp:
            ftp.login(user=user, passwd=password)
            ftp.cwd(path)
            with open(destination, "wb") as file:
                ftp.retrbinary("RETR " + filename, file.write)
