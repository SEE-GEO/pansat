"""
pansat.file_record
==================

Defines a file record class that contains information about a local or remote
file.
"""
from copy import copy
from datetime import timedelta
import logging
from typing import Optional, List, Union
from tempfile import TemporaryDirectory

from dataclasses import dataclass
import json
from pathlib import Path
import numpy as np


from pansat.time import TimeRange
from pansat.geometry import Geometry


LOGGER = logging.getLogger(__name__)


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

    @property
    def temporal_coverage(self) -> TimeRange:
        """
        The temporal coverage of the file identified by this file record.
        """
        return self.product.get_temporal_coverage(self)

    @property
    def spatial_coverage(self) -> Geometry:
        """
        The spatial coverage of the file identified by this file record.
        """
        return self.product.get_spatial_coverage(self)

    @property
    def central_time(self) -> TimeRange:
        """
        Returns a time range corresponding to the central time of the time range
        covered by the file identified by this file record.
        """
        time_range = self.temporal_coverage
        t_c = time_range.start + 0.5 * (time_range.end - time_range.start)
        return TimeRange(t_c, t_c)


    def find_closest_in_time(
            self,
            others: List["FileRecord"]
    ) -> List["FileRecord"]:
        """
        Find file records that are closest in time or overlap with 'self'.

        Args:
            others: A list of file records among which to find the temporally
                closest records.

        Return:
            A list containing the file records with temporal overlap with self
            or the file record that minimizes the time difference between the
            coverage of the two files.
        """
        time_range = self.temporal_coverage
        other_ranges = [other.temporal_coverage for other in others]
        closest = time_range.find_closest_ind(other_ranges)
        return [others[ind] for ind in closest]

    def find_exact_match(
            self,
            others: List["FileRecord"]
    ) -> Union["FileRecord", None]:
        """
        Find file record that exactly matches the time period of the file record.

        Args:
            others: A list of file records among which to find the one that matches
                the time period of the record.

        Return:
            The first file record the exatly matches the time period of 'self' or
            'None' if no such file record is persent in 'others'.
        """
        time_range = self.temporal_coverage
        other_ranges = [other.temporal_coverage for other in others]
        time_range = self.temporal_coverage
        matches = [
            other for other in others if other.temporal_coverage == time_range
        ]
        if len(matches) == 0:
            return None
        return matches[0]


    def time_difference(self, other: "FileRecord") -> timedelta:
        """
        The temporal difference between the temporal coverage of two
        file records.
        """
        return self.temporal_coverage.time_diff(other.temporal_coverage)


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
            data_dir = penv.get_active_data_dir()
            destination = (
                data_dir / self.product.default_destination
            )
            destination.mkdir(parents=True, exist_ok=True)

        new_rec = self.provider.download(self, destination=destination)
        self.local_path = new_rec.local_path

        try:
            penv.register(new_rec)
        except:
            LOGGER.exception(
                "Encountered an error when trying to insert file record for file %s into"
                " the registry.",
                new_rec.local_path,
            )
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

        if self.local_path is not None and self.local_path.exists():
            new_rec = copy(self)
            new_rec.local_path = self.local_path
            return new_rec

        try:
            LOGGER.debug("Looking up file %s", self)
            local_path = penv.lookup_file(self)
        except Exception:
            LOGGER.exception(
                "Encountered an error when trying to look up file record for file '%s' in"
                " the regristry.",
                self.filename
            )
            local_path = None

        if local_path is None:
            LOGGER.info("Downloading file %s to %s", self, destination)
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
