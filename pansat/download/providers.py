"""
pansat.download.providers
=========================

The providers sub-module provides an abstract data class defining the interface
for data providers from which a specific product can be downloaded. The generic
interface defines functions to list and download files for given days or time
ranges.
"""
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from ftplib import FTP
import os
import numpy as np

from pansat.download.accounts import get_identity

class DataProvider(metaclass=ABCMeta):
    """
    The DataProvider class implements generic methods related to querying
    satellite product files.
    """

    def __init__(self):
        pass

    @abstractmethod
    def get_files(self, year, day):
        """
        This method should return a list of strings containing all files
        available for a given day.

        Args:

            year(int): 4-digit number representing the year from which to
            retrieve the data.
            day(int): The Julian day of the year from which to retrieve the data.

        Returns:

            list of filenames that are available
        """
        pass

    def get_preceeding_file(self, filename):
        """
        Return filename of the file that preceeds the given filename in time.

        Args:

            filename(str): The name of the file of which to find the preceeding one.

        Returns:

            The filename of the file preceeding the file with the given filename.

        """
        t = self.product.name_to_date(filename)

        year = t.year
        day = int((t.strftime("%j")))
        files = self.get_files(year, day)

        i = files.index(filename)

        if i == 0:
            dt = timedelta(days=1)
            t_p = t - dt
            year = t_p.year
            day = int((t_p.strftime("%j")))
            return self.get_files(year, day)[-1]
        else:
            return files[i - 1]

    def get_following_file(self, filename):
        """
        Return filename of the file that follows the given filename in time.

        Args:

            filename(str): The name of the file of which to find the following file.

        Returns:

            The filename of the file following the file with the given filename.

        """
        t = self.product.name_to_date(filename)

        year = t.year
        day = int((t.strftime("%j")))
        files = self.get_files(year, day)

        i = files.index(filename)

        if i == len(files) - 1:
            dt = timedelta(days=1)
            t_p = t + dt
            year = t_p.year
            day = int((t_p.strftime("%j")))
            return self.get_files(year, day)[0]
        else:
            return files[i + 1]

    def get_files_in_range(self, t0, t1, t0_inclusive=False):
        """
        Get all files within time range.

        Retrieves a list of product files that include the specified
        time range.

        Args:

            t0(datetime.datetime): Start time of the time range

            t1(datetime.datetime): End time of the time range

            t0_inclusive(bool): Whether or not the list should start with
            the first file containing t0 (True) or the first file found
            with start time later than t0 (False).

        Returns:

            List of filename that include the specified time range.

        """
        dt = timedelta(days=1)

        t = t0
        files = []

        while (t1 - t).total_seconds() > 0.0:

            year = t.year
            day = int((t.strftime("%j")))

            fs = self.get_files(year, day)

            ts = [self.product.name_to_date(f) for f in fs]

            dts0 = [self.product.name_to_date(f) - t0 for f in fs]
            pos0 = [dt.total_seconds() >= 0.0 for dt in dts0]

            dts1 = [self.product.name_to_date(f) - t1 for f in fs]
            pos1 = [dt.total_seconds() > 0.0 for dt in dts1]

            inds = [i for i, (p0, p1) in enumerate(zip(pos0, pos1)) if p0 and not p1]
            files += [fs[i] for i in inds]

            t += dt

        if t0_inclusive:
            f_p = self.get_preceeding_file(files[0])
            files = [f_p] + files

        if not pos1[-1] and not files == []:
            try:
                files += [self.get_following_file(files[-1])]
            except:
                pass

        return files

    def get_file_by_date(self, t):
        """
        Get file with start time closest to a given date.

        Args:

            t(datetime): A date to look for in a file.

        Return:

            The filename of the file with the closest start time
            before the given time.
        """

        # Check last file from previous day
        dt = timedelta(days=1)
        t_p = t - dt
        year = t_p.year
        day = int((t_p.strftime("%j")))
        files = self.get_files(year, day)[-1:]

        year = t.year
        day = int(t.strftime("%j"))
        files += self.get_files(year, day)

        ts = [self.product.name_to_date(f) for f in files]
        dts = [tf - t for tf in ts]
        dts = np.array([dt.total_seconds() for dt in dts])
        inds = np.argsort(dts)
        indices = np.where(dts[inds] < 0.0)[0]

        if len(indices) == 0:
            ind = len(dts) - 1
        else:
            ind = inds[indices[-1]]

        return files[ind]


################################################################################
# icare.univ-lille.fr
################################################################################

icare_products = {
    "CloudSat_2b_GeoProf": ["SPACEBORNE", "CLOUDSAT", "2B-GEOPROF"],
    "CloudSat_1b_CPR": ["SPACEBORNE", "CLOUDSAT", "1B-CPR"],
    "CloudSat_MODIS_Aux": ["SPACEBORNE", "CLOUDSAT", "MODIS-AUX"],
    "CloudSat_ECMWF_Aux": ["SPACEBORNE", "CLOUDSAT", "ECMWF-AUX"],
}


class IcareProvider(DataProvider):
    """
    Base class for data products available from the ICARE ftp server.
    """

    base_url = "ftp.icare.univ-lille1.fr"

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

        product_path(str): The path of the product. This should point to
        the folder that bears the product name and contains the directory
        tree which contains the data files sorted by date.

        """
        if not product.__name__ in icare_products:
            available_products = list(icare_products.keys())
            raise ValueError(
                f"{product.__name__}  not a available from the ICARE data"
                " provider. Currently available products are: "
                " {available_products}."
            )
        self.product = product
        self.product_path = os.path.join(*icare_products[product.__name__])
        self.cache = {}

    def __ftp_listing_to_list__(self, path, t=int):
        """
        Retrieve directory content from ftp listing as list.

        Args:

           path(str): The path from which to retrieve the ftp listing.

           t(type): Type constructor to apply to the elements of the
           listing. To retrieve a list of strings use t = str.

        Return:

            A list containing the content of the ftp directory.

        """
        if not path in self.cache:
            with FTP(IcareProvider.base_url) as ftp:
                identity = get_identity("Icare")
                ftp.login(user=identity["user"], passwd=identity["password"])
                try:
                    ftp.cwd(path)
                except:
                    raise Exception(
                        "Can't find product folder "
                        + path
                        + "on the ICARE ftp server.. Are you sure this is"
                        "a  ICARE multi sensor product?"
                    )
                listing = ftp.nlst()
            listing = [t(l) for l in listing]
            self.cache[path] = listing
        return self.cache[path]

    def get_files(self, year, day):
        """
        Return all files from given year and julian day. Files are returned
        in chronological order sorted by the file timestamp.

        Args:

            year(int): The year from which to retrieve the filenames.

            day(int): Day of the year of the data from which to retrieve the
            the filenames.

        Return:

            List of all HDF files available of this product on the given date.
        """
        day_str = str(day)
        day_str = "0" * (3 - len(day_str)) + day_str
        date = datetime.strptime(str(year) + str(day_str), "%Y%j")
        path = os.path.join(self.product_path, str(year), date.strftime("%Y_%m_%d"))
        listing = self.__ftp_listing_to_list__(path, str)
        files = [name for name in listing if name[-3:] == "hdf"]
        return files

    def download(self, filename, dest):
        """
        Download file with given name and store to location.

        Args:
            filename(``str``): The name of the file
            dest(``dest``): The path to which to store the file.

        """
        date = self.product.name_to_date(filename)
        path = os.path.join(
            self.product_path, str(date.year), date.strftime("%Y_%m_%d")
        )

        identity = get_identity("Icare")
        with FTP(self.base_url) as ftp:
            ftp.login(user=identity["user"], passwd=identity["password"])
            ftp.cwd(path)
            with open(dest, "wb") as file:
                ftp.retrbinary("RETR " + filename, file.write)
