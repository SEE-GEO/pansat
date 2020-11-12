"""
pansat.download.providers.ges_disc
==================================

This module contains a data provider for NASA's Goddard Earth Sciences Data and
Information Services Center (GES DISC).

Reference
---------
"""
import datetime

from pansat.download import accounts
from pansat.download.providers.discrete_provider import DiscreteProvider
import requests
import re

GESDISC_PRODUCTS = ["GPM_2ADPR.06",
                    "GPM_2ADPRENV.06",
                    "PM_2AGPROFAQUAAMSRE_CLIM.05",
                    "GPM_2AGPROFF11SSMI_CLIM.05",
                    "GPM_2AGPROFF13SSMI_CLIM.05",
                    "GPM_2AGPROFF14SSMI_CLIM.05",
                    "GPM_2AGPROFF15SSMI_CLIM.05",
                    "GPM_2AGPROFF16SSMIS.05",
                    "GPM_2AGPROFF16SSMIS_CLIM.05",
                    "GPM_2AGPROFF17SSMIS.05",
                    "GPM_2AGPROFF17SSMIS_CLIM.05",
                    "GPM_2AGPROFF18SSMIS.05",
                    "GPM_2AGPROFF18SSMIS_CLIM.05",
                    "GPM_2AGPROFF19SSMIS.05",
                    "GPM_2AGPROFF19SSMIS_CLIM.05",
                    "GPM_2AGPROFGCOMW1AMSR2.05",
                    "GPM_2AGPROFGCOMW1AMSR2_CLIM.05",
                    "GPM_2AGPROFGPMGMI.05",
                    "GPM_2AGPROFGPMGMI_CLIM.05",
                    "GPM_2AGPROFMETOPAMHS.05",
                    "GPM_2AGPROFMETOPAMHS_CLIM.05",
                    "GPM_2AGPROFMETOPBMHS.05",
                    "GPM_2AGPROFMETOPBMHS_CLIM.05",
                    "GPM_2AGPROFMETOPCMHS.05",
                    "GPM_2AGPROFMETOPCMHS_CLIM.05",
                    "GPM_2AGPROFNOAA15AMSUB_CLIM.05",
                    "GPM_2AGPROFNOAA16AMSUB_CLIM.05",
                    "GPM_2AGPROFNOAA17AMSUB_CLIM.05",
                    "GPM_2AGPROFNOAA18MHS.05",
                    "GPM_2AGPROFNOAA18MHS_CLIM.05",
                    "GPM_2AGPROFNOAA19MHS.05",
                    "GPM_2AGPROFNOAA19MHS_CLIM.05",
                    "GPM_2AGPROFNOAA20ATMS.05",
                    "GPM_2AGPROFNOAA20ATMS_CLIM.05",
                    "GPM_2AGPROFNPPATMS.05",
                    "GPM_2AGPROFNPPATMS_CLIM.05",
                    "GPM_2AKa.06",
                    "GPM_2AKaENV.06",
                    "GPM_2AKu.06",
                    "GPM_2AKuENV.06",
                    "GPM_2APRPSMT1SAPHIR.06",
                    "GPM_2APRPSMT1SAPHIR_CLIM.06",
                    "GPM_2BCMB.06",
                    "GPM_2HCSH.06",
                    "GPM_2HSLH.06"]


class GesdiscProvider(DiscreteProvider):
    """
    Dataprovider class for for products available from the
    gpm1.gesdisc.eosdis.nasa.gov domain.
    """

    base_url = "gpm1.gesdisc.eosdis.nasa.gov"
    file_pattern = re.compile('"[^"]*.HDF5"')

    def __init__(self, product):
        """
        Create new GesDisc provider.

        Args:
            product: The product to download.
        """
        self.product_name = str(product)
        self.level = self.product_name.split("_")[1][:2]
        super().__init__(product)

    @classmethod
    def get_available_products(cls):
        """
        Return the names of products available from this data provider.

        Return:
            A list of strings containing the names of the products that can
            be downloaded from this data provider.
        """
        return GESDISC_PRODUCTS

    @property
    def _request_string(self):
        """The URL containing the data files for the given product."""
        base_url = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L2/{product}"
        base_url = base_url.format(level=self.level, product=self.product_name)
        return base_url + "/{year}/{day}/{filename}"

    def get_files_by_day(self, year, day):
        """
        Return list of available files for a given day of a year.

        Args:
            year(``int``): The year for which to look up the files.
            day(``int``): The Julian day for which to look up the files.

        Return:
            A list of strings containing the filename that are available
            for the given day.
        """
        day = str(day)
        day = "0" * (3 - len(day)) + day
        request_string = self._request_string.format(year=year, day=day, filename="")
        print("request string:", request_string)
        response = requests.get(request_string)
        files = list(set(GesdiscProvider.file_pattern.findall(response.text)))
        return [f[1:-1] for f in files]

    def download_file(self, filename, destination):
        """
        Download file from data provider.

        Args:
            filename(``str``): The name of the file to download.
            destination(``str`` or ``pathlib.Path``): path to directory where
                the downloaded files should be stored.
        """
        t = self.product.filename_to_date(filename)
        year = t.year
        day = t.strftime("%j")
        day = "0" * (3 - len(day)) + day
        request_string = self._request_string.format(
            year=year, day=day, filename=filename
        )
        auth = accounts.get_identity("GES DISC")
        r = requests.get(request_string, auth=auth)
        with open(destination, "wb") as f:
            for chunk in r:
                f.write(chunk)
