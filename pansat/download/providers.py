"""
pansat.download.providers
=========================

The providers sub-module provides an abstract data class defining the interface
for data providers from which a specific product can be downloaded. The generic
interface defines functions to list and download files for given days or time
ranges.
"""
from abc import ABCMeta, abstractmethod, abstractclassmethod
from datetime import datetime, timedelta
from ftplib import FTP
import os
import numpy as np
import itertools

from pansat.download.accounts import get_identity


class DataProvider(metaclass=ABCMeta):
    """
    The DataProvider class implements generic methods related to querying
    satellite product files.
    """

    def __init__(self):
        pass

    @abstractclassmethod
    def get_available_products(self):
        pass

    @abstractmethod
    def download(self, start, end, dest=None):
        """
        This method should downloads data for a given range from respective DataProvider childclass.

        Args:

            start(datetime.datetime): date and time for start
            end(datetime.datetime): date and time for end
            dest(str): path to location where output should be stored
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

            ts = [self.product.filename_to_date(f) for f in fs]

            dts0 = [self.product.filename_to_date(f) - t0 for f in fs]
            pos0 = [dt.total_seconds() >= 0.0 for dt in dts0]

            dts1 = [self.product.filename_to_date(f) - t1 for f in fs]
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

        ts = [self.product.filename_to_date(f) for f in files]
        dts = [tf - t for tf in ts]
        dts = np.array([dt.total_seconds() for dt in dts])
        inds = np.argsort(dts)
        indices = np.where(dts[inds] < 0.0)[0]

        if len(indices) == 0:
            ind = len(dts) - 1
        else:
            ind = inds[indices[-1]]

        return files[ind]


###############################################################################
# cds.climate.copernicus.eu
##############################################################################

copernicus_products = [
    "reanalysis-era5-land",
    "reanalysis-era5-land-monthly-means",
    "reanalysis-era5-pressure-levels",
    "reanalysis-era5-pressure-levels-monthly-means",
    "reanalysis-era5-single-levels",
    "reanalysis-era5-single-levels-monthly-means",
]


class CopernicusProvider(DataProvider):
    """
    Base class for reanalysis products available from Copernicus.
    """

    def __init__(self, product):
        """
        Create a new product instance.

        Args:

        product(str): product name, available products are land, single-level, pressure-level for hourly and monthly resolution
        """
        super().__init__()
        self.product = product

        if not product.name in copernicus_products:
            available_products = copernicus_products
            raise ValueError(
                f"{product.name} not a available from the Copernicus data"
                " provider. Currently available products are: "
                f" {available_products}."
            )


    @classmethod
    def get_available_products(cls):
        """
        The products available from this dataprovider.
        """
        return copernicus_products


    def download(self, start, end, destination):
        """Downloads files for given time range and stores at specified location.
        Hourly data products are saved per hour and monthly data products are saved per month.
        Note that you have to install the CDS API key before download is possible: https://cds.climate.copernicus.eu/api-how-to

        Args:

        start(datetime.datetime) : start date and time (year, month, day, hour), if hour is not specified for hourly dataproduct, all hours are downloaded for each date
        end(datetime.datetime) : end date and time (year, month, day, hour), if hour is not specified for hourly dataproduct, all hours are downloaded for each date
        destination(str) : output path
        """

        # open new client instance
        import cdsapi

        c = cdsapi.Client()

        # subset region, if requested
        if self.product.domain== None:
            area = ""
        else:
            area = "/".join(self.product.domain)
    
        ################### create time range for monthly data products ##############################
        if "monthly" in self.product.name:
            # handling data ranges over multiple years:
            if start.year != end.year:
                # get years with complete nr. of months
                full_years_range = range(start.year + 1, end.year)
                full_years = list(
                    itertools.chain.from_iterable(
                        itertools.repeat(x, 12) for x in full_years_range
                    )
                )
                all_months = np.arange(1, 13).astype(str)

                # get months of incomplete years
                months_first_year = list(np.arange((start.month + 1), 13).astype(str))
                months_last_year = list(np.arange(1, (end.month + 1)).astype(str))

                # create lists for years with months
                years = (
                    [str(start.year)] * len(months_first_year)
                    + [str(f) for f in full_years]
                    + [str(end.year)] * len(months_last_year)
                )
                dates = (
                    months_first_year
                    + [str(m) for m in all_months] * len(full_years_range)
                    + months_last_year
                )
            else:
                # getting all month for the specified year
                dates = np.arange(start.month, end.month + 1).astype(str)
                nr_of_months = np.shape(dates)[0]
                years = [str(start.year)] * nr_of_months
        else:
            ############### create time range for hourly data products ##############################

            # get list with all years, months, days, hours between the two dates
            delta = end - start
            hour = delta / 3600
            dates = []
            for i in range(hour.seconds + 1):
                h = start + timedelta(hours=i)
                dates.append(h)

        # container to save list of downloaded files 
        files= []

        # send API request for each specific month or hour
        for idx, date in enumerate(dates):
            if "monthly" in self.product.name:
                # define download parameters for monthly download
                month = date
                year = years[idx]
                day = ""
                hour = "00:00"
                download_key = "monthly_averaged_reanalysis"

            else:
                # define download parameters for hourly download
                year = str(dates[idx].year)
                month = str(dates[idx].month)
                day = str(dates[idx].day)
                hour = str(dates[idx].hour)

                # get download key
                download_key = "reanalysis"
                if "land" in self.product.name:
                    download_key = ""

                # zero padding for day
                if int(day) < 10:
                    day = '0' + str(day)

            # zero padding for month
            if int(month) < 10:
                month = '0'+ str(month)

            filename = (
                "era5-"
                + self.product.name
                + "_"
                + year
                + month
                + day
                + hour
                + "_"
                + "-".join(self.product.variables)
                + "-".join(area)
                + ".nc"
                )

            # set output path and file name
            out = str(destination) + "/" + str(filename)

            # only download if file not already already exists
            if os.path.exists(out) == True:
                print(destination, " already exists.")

            else:
                c.retrieve(
                    self.product.name,
                    {
                        "product_type": download_key,
                        "format": "netcdf",
                        "area": area,
                        "variable": self.product.variables,
                        "year": year,
                        "month": month,
                        "day": day,
                        "time": hour,
                    },
                    out,
                )
                print("file downloaded and saved as", out)

                files.append(out)

            return files



################################################################################
# icare.univ-lille.fr
################################################################################

icare_products = {
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
}



class Icare(DataProvider):
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
        if not str(product) in icare_products:
            available_products = list(icare_products.keys())
            raise ValueError(
                f"The product {product} is  not a available from the ICARE data"
                f" provider. Currently available products are: "
                f"{available_products}."
            )
        self.product = product
        self.product_path = "/".join(icare_products[str(product)])
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
            with FTP(Icare.base_url) as ftp:
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
            listing = [t(l) for l in listing]
            self.cache[path] = listing
        return self.cache[path]

    @classmethod
    def get_available_products(cls):
        return icare_products.keys()

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
        path = "/".join([self.product_path, str(year), date.strftime("%Y_%m_%d")])
        listing = self.__ftp_listing_to_list__(path, str)
        files = [name for name in listing if name[-3:] == "hdf"]
        return files

    def download(self, filename, destination):
        """
        Download file with given name and store to location.

        Args:
            filename(``str``): The name of the file
            destination(``destination``): The path to which to store the file.

        """
        date = self.product.filename_to_date(filename)
        path = "/".join([self.product_path, str(date.year), date.strftime("%Y_%m_%d")])

        user, password = get_identity("Icare")
        with FTP(self.base_url) as ftp:
            ftp.login(user=user, passwd=password)
            ftp.cwd(path)
            with open(destination, "wb") as file:
                ftp.retrbinary("RETR " + filename, file.write)


all_providers = [Icare, CopernicusProvider]
