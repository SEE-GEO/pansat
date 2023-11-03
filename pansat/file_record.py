"""
pansat.file_record
==================

Defines a file record class that contains information about a local or remote
file.
"""
from copy import copy
from typing import Optional

from dataclasses import dataclass
import json
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
        filename: str,
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

    def __init__(
        self, local_path, product=None, filename=None, provider=None, remote_path=None
    ):
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
        self.provider = provider
        self.remote_path = remote_path

    def download(
            self,
            destination: Optional[Path] = None
    ) -> "FileRecord":
        """
        Download a remote corresponding file.

        Downloading a remote file requires that record to have an associated
        remote path and corresponding data_provider.

        Args:
            destination: A path pointing to a directory or file to which
                 to write the downloaded file.

        Return:
            This file record but updated so that its 'local_path' attribute
            points to the path of the downloaded file.
        """
        import pansat.environment as penv

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
        if destination is None:
            destination = (
                penv.get_active_data_dir() / self.product.default_destination
            )
            destination.mkdir(parents=True, exist_ok=True)

        new_rec = self.provider.download(self, destination=destination)
        self.local_path = new_rec.local_path

        penv.register(new_rec)

        return self

    def get(
            self,
            destination: Optional[Path] = None
    ) -> "FileRecord":
        """
        Get local file or download if necessary.

        Args:
            destination: A path pointing to a directory or file to which
                 to write the downloaded file.

        Return:
            A file record whose local path points to an existing version of
            the requested file.
        """
        import pansat.environment as penv

        local_path = penv.lookup_file(self)
        if local_path is None:
            return self.download(destination)

        new_rec = copy(self)
        new_rec.local_path = local_path
        return new_rec


    @classmethod
    def from_dict(cls, dct):
        """
        Create FileRecord from dictionary representation data.

        Args:
            dct: A dictionary containing the parsed json data.

        Return:
            A FileRecord representing the loaded data.
        """
        from pansat import products

        product = dct["product"]
        if product is not None:
            dct["product"] = products.get_product(product)
        local_path = dct["local_path"]
        if local_path is not None:
            dct["local_path"] = Path(local_path)
        return FileRecord(**dct)

    def to_dict(self):
        """
        Return dictionary representation containing only primitive types.
        """
        return {
            "filename": self.filename,
            "local_path": str(self.local_path),
            "product": self.product.name if self.product is not None else None,
            "provider": self.provider,
            "remote_path": self.remote_path,
        }

    def to_json(self):
        """
        Return json representation of the file record.
        """
        return json.dumps({"FileRecord": self.to_dict()})
