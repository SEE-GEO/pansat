"""
pansat.download.providers.cloudnet
==================================

A data provider to download Cloudnet data.
"""
from datetime import datetime
from pathlib import Path
import requests

from pansat.download.providers.discrete_provider import DiscreteProvider


FILE_URL = "https://cloudnet.fmi.fi/api/files"


class CloudnetProvider(DiscreteProvider):
    """
    Provider class to download data from the cloudnet API.
    """

    @classmethod
    def get_available_products(cls):
        return [
            "ground_based::Cloudnet::radar",
            "ground_based::Cloudnet::iwc",
            "ground_based::Cloudnet::classification",
        ]

    def get_files_by_day(self, year, day):
        """
        Return files available on a given day.

        Args:
            year: Integer specifying the year.
            day: Integer specfiying the day of the yaer.

        Return:
            A list containing the available files.
        """
        date = datetime.strptime(f"{year}{day:03}", "%Y%j")
        payload = {
            "product": self.product.product_name,
            "date": date.strftime("%Y-%m-%d"),
        }
        if self.product.location is not None:
            payload["site"] = self.product.location
        response = requests.get(FILE_URL, payload)
        response.raise_for_status()
        files = [res["downloadUrl"].split("/")[-1] for res in response.json()]
        return [filename for filename in files if self.product.matches(filename)]

    def download_file(self, filename, destination):
        """
        Download a file.

        Args:
            filename: The name of the file.
            destination: Path to the file to which to write the
                 downloaded data.
        """
        filename = Path(filename)
        date, site, *_ = filename.name.split("_")
        payload = {
            "product": self.product.product_name,
            "site": site,
            "date": f"{date[:4]}-{date[4:6]}-{date[6:]}",
        }
        files = requests.get(FILE_URL, payload).json()

        url = files[0]["downloadUrl"]
        response = requests.get(url)
        with open(destination, "wb") as output:
            output.write(response.content)
