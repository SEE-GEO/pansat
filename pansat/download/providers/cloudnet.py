"""
pansat.download.providers.cloudnet
==================================

A data provider to download Cloudnet data.
"""
from datetime import datetime
from pathlib import Path
import requests

from pansat.download.providers.discrete_provider import DiscreteProvider
from pansat.file_record import FileRecord


FILE_URL = "https://cloudnet.fmi.fi/api/files"


class CloudnetProvider(DiscreteProvider):
    """
    Provider class to download data from the cloudnet API.
    """

    @classmethod
    def get_available_products(cls):
        return [
            "ground_based.cloudnet.l1_radar",
            "ground_based.cloudnet.l2_iwc",
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
        urls = [res["downloadUrl"] for res in response.json()]
        filenames = [url.split("/")[-1] for url in urls]
        frecs = [
            FileRecord.from_remote(self.product, self, url, filename)
            for url, filename in zip(urls, filenames)
        ]
        return frecs

    def download_file(self, file_record, destination):
        """
        Download a file.

        Args:
            file_record: File record specifying the file to download.
            destination: Path to the file to which to write the
                 downloaded data.
        """
        filename = file_record.filename
        date, site, *_ = filename.name.split("_")
        payload = {
            "product": self.product.product_name,
            "site": site,
            "date": f"{date[:4]}-{date[4:6]}-{date[6:]}",
        }
        files = requests.get(FILE_URL, payload).json()

        url = files[0]["downloadUrl"]
        response = requests.get(url)
        response.raise_for_status()
        with open(destination, "wb") as output:
            output.write(response.content)
