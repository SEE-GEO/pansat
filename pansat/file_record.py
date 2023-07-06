"""
pansat.file_record
==================

Defines a file record class that contains information about a local or remote
file.
"""
from dataclasses import dataclass
from pathlib import Path


@dataclass
class FileRecord:
    """
    A file record represents a data file of a given product that is either
    available locally or remotely.
    """

    filename: str
    local_path: Path = None
    product: "pansat.Product" = None
    provider: "pansat.DataProvider" = None
    remote_path: str = None

    @staticmethod
    def from_remote(
        product: "pansat.Product",
        provider: "pansat.DataProvider",
        remote_path: str = None,
        filename: str = None
    ):
        """
        Create file record from a remote file.

        Args:
            product: A pansat product representing the data product.
            remote_path: String describing the remote location of the
                data file.
            filename: The filename of the data file.

        Return:
            A 'FileRecord' object representing the file.
        """
        rec = FileRecord(product, None)
        rec.remote_path = remote_path
        rec.filename = filename
        rec.provider = provider
        return rec

    def __init__(self, local_path, product=None):
        """
        Create file record from product and local file path.

        Args:
            product: A pansat product representing the data product.
            local_path: The local path of the file.
        """
        local_path = Path(local_path)
        self.local_path = local_path
        self.filename = local_path.name
        self.product = product
