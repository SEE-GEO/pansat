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
        remote_path: str,
        filename: str
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
        rec = FileRecord(None, product=product, filename=filename)
        rec.remote_path = remote_path
        rec.filename = filename
        rec.provider = provider
        return rec

    def __init__(self, local_path, product=None, filename=None):
        """
        Create file record from product and local file path.

        Args:
            product: A pansat product representing the data product.
            local_path: The local path of the file.
            filename: The name of the file. This needs to be provided if
                no local path is given.
        """
        if local_path is not None:
            local_path = Path(local_path)
        self.local_path = local_path

        if filename is None:
            if local_path is None:
                raise ValueError(
                    "If 'local_path' is None, the 'filename' kwarg must be "
                    " provided and not None."
                )
            filename = local_path.name
        self.filename = filename
        self.product = product

    def download(self, destination):
        """
        Download corresponding file.

        Downloading a remote file requires that record to have an associated
        remote path and corresponding data_provider.

        Args:
            destination: A path pointing to a directory or file to which
                 to write the downloaded file.

        Return:
            The local path of the downloaded file.
        """
        if self.remote_path is None:
            raise ValueError(
                "The file record does not have an associated remote path."
                " Downloading the corresponding file is therefore not "
                " possible."
            )
        if self.provider is None:
            raise ValueError(
                "The file record lacks an associated provider."
                " Downloading the corresponding file is therefore not "
                " possible."
            )
        return self.provider.download(self, destination)
