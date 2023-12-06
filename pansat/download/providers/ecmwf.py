"""
pansat.download.providers.ecwmf
===============================

Data provider for public ECMWF datasets, which currently include the S2S
 database and TIGGE dataset.

Requirements
````````````

This data provider requires the ``ecmwf-api-client`` package to be installed.

Configuration
`````````````

Using this dataprovider requires registering with ECMWF and adding the
the ``ecwmf`` provider and API key to the pansat identities using the
registered email as user name.

.. code-block:: console

    pansat account add ecmwf <email>
"""
from typing import Optional, List
from pathlib import Path

from pansat.download.providers.data_provider import DataProvider
from pansat.download import accounts
from pansat.file_record import FileRecord
from pansat.time import TimeRange
from pansat.geometry import Geometry
from pansat.products import Product


ECMWF_URL = "https://api.ecmwf.int/v1"


class ECMWF(DataProvider):
    """
    Provides download access for the public datasets currently (S2S and TIGGE)
    provided by ECMWF.
    """

    def __init__(self):
        super().__init__()

    def _get_server(
        self,
    ) -> ECMWFDataServer:
        """
        Get ECMWFDataServer instance with credentials from saved identities.
        """
        from ecmwfapi import ECMWFDataServer

        url = ECMWF_URL
        email, api_key = accounts.get_identity("ecmwf")
        return ECMWFDataServer(url=url, email=email, api_key=api_key)

    def provides(self, product: Product) -> bool:
        """
        Provides ECMWF datasets.
        """
        return product.name.startswith("model.ecmwf")

    def find_files(
        self, product: Product, time_range: TimeRange, roi: Optional[Geometry] = None
    ) -> List[FileRecord]:
        """
        Find all files available within a given time range.

        Args:
            product: A Product object identifying the dataset.
            time_range: The time range within which to find files.

        Return:
            A list of FileRecords identifying the available files.
        """
        time = time_range.start
        end = time_range.end
        recs = []
        while time < end:
            filename = Path(product.get_filename(time))
            recs.append(
                FileRecord.from_remote(
                    product=product,
                    provider=self,
                    remote_path=filename,
                    filename=filename.name,
                )
            )
            time += product.temporal_extent(time)
        return recs

    def download(
        self, file_record: FileRecord, destination: Optional[Path] = None
    ) -> FileRecord:
        """
        Download a given file.

        Args:
            file_record: A FileRecord object identifying the file to download.
            destination: Path pointing to a folder or file to which to write the
                 downloaded data.

        Return:
            A new file record object whose 'local_path' object points to the
            downloaded data.
        """
        if destination.is_dir():
            destination = destination / file_record.filename

        request = file_record.product.get_request(file_record)
        server = self._get_server()

        request["target"] = str(destination)
        server.retrieve(request)

        rec_new = copy(file_record)
        rec_new.local_path = destination
        return rec_new


ecmwf = ECMWF()
