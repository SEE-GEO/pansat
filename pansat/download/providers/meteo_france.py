from datetime import datetime, timedelta
import http
import time
from pathlib import Path
from xml.etree import ElementTree

from pansat.download.providers.data_provider import DataProvider
from pansat.download.accounts import get_identity


def ensure_extension(path, ext):
    if not any([path[-len(e) :] == e for e in ext]):
        path = path + ext[0]
    return path


class GeoservicesProvider(DataProvider):
    """
    Base class for data products available from the ICARE ftp server.
    """

    base_url = "https://geoservices.meteofrance.fr/services"

    def __init__(self, product):
        """
        Create a new product instance.

        Arguments:

        product_path(str): The path of the product. This should point to
            the folder that bears the product name and contains the directory
            tree which contains the data files sorted by date.

        name_to_date(function): Funtion to convert filename to datetime object.
        """
        super().__init__()
        self.product = product
        name, password = get_identity("GeoservicesMeteofrance")
        request = (
            GeoservicesProvider.base_url
            + f"/GetAPIKey?username={name}&password={password}"
        )
        c = http.client.HTTPSConnection("geoservices.meteofrance.fr")
        c.request("GET", request)
        r = c.getresponse().read().decode()
        root = ElementTree.fromstring(r)
        self.token = root.text

    @classmethod
    def get_available_products(cls):
        return [
            "OPERA_RAINFALL_RATE",
            "OPERA_MAXIMUM_REFLECTIVITY",
            "OPERA_HOURLY_RAINFALL",
        ]

    def _get_times_in_range(self, start_time, end_time):
        seconds_since_hour = start_time.minute * 60 + start_time.second
        d_t = (15 * 60 - seconds_since_hour) % (15 * 60)
        d_t = timedelta(seconds=d_t)
        time = start_time + d_t
        while time < end_time:
            yield time
            time += timedelta(minutes=15)

    def _get_filename(self, time):
        product_name = str(self.product)[6:]
        year = time.year
        day = time.timetuple().tm_yday
        hour = time.hour
        minute = time.minute
        filename = f"OPERA_{product_name}_{year}_{day:03}_" f"{hour:02}_{minute:02}.hdf"
        return filename

    def _download_file(self, time, destination):
        """
        Download a given product file.

        Arguments:

            filename(str): The name of the file to download.

            dest(str): Where to store the file.
        """
        # if Path(destination).exists:
        #    return destination
        product_name = str(self.product)[6:]
        c = http.client.HTTPSConnection("geoservices.meteofrance.fr")
        time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ")
        request = (
            GeoservicesProvider.base_url
            + f"/odyssey?product={product_name}"
            + f"&time={time_str}&token={self.token}&format=HDF5"
        )
        c.request("GET", request)
        r = c.getresponse()
        with open(destination, "wb") as f:
            f.write(r.read())
        return destination

    def download(self, start_time, end_time, destination):
        destination = Path(destination)
        files = []
        for time in self._get_times_in_range(start_time, end_time):
            filename = self._get_filename(time)
            files.append(self._download_file(time, destination / filename))
        return files

    def name_to_date(self, name):
        s = "_".join(name.split("_")[-4:])
        return datetime.strptime(s.split(".")[0], "%Y_%j_%H_%M")
