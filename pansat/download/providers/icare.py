"""
pansat.download.providers.icare
===============================

This module providers the ``IcareProvider`` class, which implementes a data provider
class for downloading data from the
`Icare datacenter <https://www.icare.univ-lille.fr/>`_.
"""
from copy import copy
from pathlib import Path 
from datetime import datetime
from ftplib import FTP
import logging
import re 
from typing import Optional

from pansat.download.providers.discrete_provider import DiscreteProvider
from pansat.download.accounts import get_identity

from pansat.download.providers.discrete_provider import (
    DiscreteProviderDay,
)
from pansat.file_record import FileRecord
from pansat.time import to_datetime
from pansat import cache


LOGGER = logging.getLogger(__name__)


ICARE_PRODUCTS = {
    "satellite.cloudsat.l1b_cpr": ["SPACEBORNE", "CLOUDSAT", "1B-CPR"],
    "satellite.cloudsat.l2b_cldclass": ["SPACEBORNE", "CLOUDSAT", "2B-CLDCLASS"],
    "satellite.cloudsat.l2b_cldclass_lidar": ["SPACEBORNE", "CLOUDSAT", "2B-CLDCLASS-LIDAR.v05.06"],
    #"CloudSat_2B-CWC-RO": ["SPACEBORNE", "CLOUDSAT", "2B-CWC-RO"],
    #"CloudSat_2B-CWC-RVOD": ["SPACEBORNE", "CLOUDSAT", "2B-CWC-RVOD"],
    #"CloudSat_2B-FLXHR": ["SPACEBORNE", "CLOUDSAT", "2B-FLXHR"],
    #"CloudSat_2B-FLXHR-LIDAR": ["SPACEBORNE", "CLOUDSAT", "2B-FLXHR-LIDAR"],
    "satellite.cloudsat.l2b_geoprof": ["SPACEBORNE", "CLOUDSAT", "2B-GEOPROF"],
    #"CloudSat_2B-GEOPROF-LIDAR": ["SPACEBORNE", "CLOUDSAT", "2B-GEOPROF-LIDAR"],
    #"CloudSat_2B-TAU": ["SPACEBORNE", "CLOUDSAT", "2B-TAU"],
    #"CloudSat_2C-PRECIP-COLUMN": ["SPACEBORNE", "CLOUDSAT", "2B-PRECIP-COLUMN"],
    "satellite.cloudsat.cstrack_cs_modis_aux": ["SPACEBORNE", "CLOUDSAT", "CSTRACK_CS-MODIS-AUX"]
    #"satellite.cloudsat.l2c": ["SPACEBORNE", "CLOUDSAT", "2B-PRECIP-COLUMN"],
    #"CloudSat_2C-SNOW-PROFILE": ["SPACEBORNE", "CLOUDSAT", "2B-GEOPROF-LIDAR"],
    #"Calipso_333mCLay": ["SPACEBORNE", "CALIOP", "333mCLay"],
    #"Calipso_01kmCLay": ["SPACEBORNE", "CALIOP", "01kmCLay"],
    #"Calipso_05kmAPro": ["SPACEBORNE", "CALIOP", "05kmAPro"],
    #"Calipso_CAL_LID_L1": ["SPACEBORNE", "CALIOP", "CAL_LID_L1.C3"],
    #"Dardar_DARDAR_CLOUD": ["SPACEBORNE", "CLOUDSAT", "DARDAR-CLOUD.v3.00"],
    #"MODIS_Terra_MOD021KM": ["SPACEBORNE", "MODIS", "MOD021KM.061"],
    #"MODIS_Terra_MOD03": ["SPACEBORNE", "MODIS", "MOD03.061"],
    #"MODIS_Aqua_MYD021KM": ["SPACEBORNE", "MODIS", "MYD021KM.061"],
    #"MODIS_Aqua_MYD03": ["SPACEBORNE", "MODIS", "MYD03.061"],
    #"MODIS_Aqua_MYD35_l2": ["SPACEBORNE", "MODIS", "MYD35_L2.061"],
}



class IcareProvider(DiscreteProviderDay):
    """
    Base class for data products available from the ICARE ftp server.
    """
    base_url = "ftp.icare.univ-lille1.fr"
    file_pattern = re.compile(r'"[^"]*\.(?:HDF5|h5)"')

    def __init__(self):
        """
        Create a new product instance.
        """
        super().__init__()
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
                    listing = ftp.nlst()
                    listing = [item_type(l) for l in listing]
                except:
                    LOGGER.exception(
                        "An error was encountered when listing files on "
                        "ICARE ftp server."
                    )
                    listing = []

            self.cache[path] = listing
        return self.cache[path]

    def provides(self, product) -> bool:
        """
        Whether or not the provider provides a given produc.t
        """
        return product.name in ICARE_PRODUCTS

    def find_files_by_day(self, product, time, roi=None):
        """
        Find files available data files at a given day.

        Args:
            product: A 'pansat.Product' object identifying the product
               for which to retrieve available data files.
            time: A time object specifying the day for which to retrieve
               available products.
            roi: An optional geometry object to limit the files to
               only those that cover a certain geographical region.

        Return:
            A list of file records identifying the files from the requested
            day.
        """
        LOGGER.debug(
            "Looking up files for day %s",
            time
        )
        time = to_datetime(time)
        product_path ="/".join(ICARE_PRODUCTS[product.name])

        rel_url = "/".join([product_path, str(time.year), time.strftime("%Y_%m_%d")])
        url = self.base_url +'/' + rel_url 

        # response for FTP file listing 
        user, pw = get_identity('Icare')
        with FTP(self.base_url) as ftp: 
            ftp.login(user = user, passwd = pw)
            ftp.cwd(rel_url)
            files = ftp.nlst()

        recs = [
            FileRecord.from_remote(product, self, rel_url, fname)
            for fname in files
        ]
        return recs


    def download(
        self, rec: FileRecord, destination: Optional[Path] = None) -> FileRecord:
        """
        Download a product file to a given destination.

        Args:
            rec: A FileRecord identifying the
            destination:

        Return:
            An updated file record whose 'local_path' attribute points
            to the downloaded file.
        """
        if destination is None:
            destination = rec.product.default_destination
            destination.mkdir(exist_ok=True, parents=True)
        else:
            destination = Path(destination)

        if destination.is_dir():
            destination = destination / rec.filename


        url = rec.remote_path

        LOGGER.debug(
            "Starting download of file %s from %s.",
            rec.filename, url
        )

        self.download_url(rec.remote_path, rec.filename, destination)
        new_rec = copy(rec)
        new_rec.local_path = destination
        return new_rec


    def download_url(self, path, filename, destination):
        """
        Downloads file from ICARE server using the 'Icare' identity.
        """
        user, password = get_identity("Icare")
        with FTP(self.base_url) as ftp:
            ftp.login(user=user, passwd=password)
            ftp.cwd(path)
            with open(destination, "wb") as file:
                ftp.retrbinary("RETR " + filename, file.write)


icare_provider = IcareProvider()
