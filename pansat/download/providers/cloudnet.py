"""
pansat.download.providers.cloudnet
==================================

A data provider to download Cloudnet data.
"""
from datetime import datetime
from pathlib import Path
import requests

from pansat.download.providers.discrete_provider import DiscreteProvider



FILE_URL = 'https://cloudnet.fmi.fi/api/files'


class CloudnetProvider(DiscreteProvider):

    @classmethod
    def get_available_products(cls):
        return [
            "Cloudnet_iwc",
            "Cloudnet_radar"
        ]

    def __init__(self, product):
        self.product = product

    def get_files_by_day(self, year, day):

        date = datetime.strptime(f"{year}{day:03}", "%Y%j")
        payload = {
            "product": self.product.product_name,
            "date": date.strftime("%Y-%m-%d")
        }
        response = requests.get(FILE_URL, payload).json()
        files = [res["downloadUrl"].split("/")[-1] for res in response]
        return [
            filename for filename in files if self.product.matches(filename)
        ]

    def download_file(self, filename, destination):

        filename = Path(filename)
        date, site, *_ = filename.name.split("_")
        payload = {
            "product": self.product.product_name,
            "site": site,
            "date": f"{date[:4]}-{date[4:6]}-{date[6:]}"
        }
        files = requests.get(FILE_URL, payload).json()

        url = files[0]["downloadUrl"]
        response = requests.get(url)
        with open(destination, "wb") as output:
            output.write(response.content)
