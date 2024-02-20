"""
pansat.download.providers.cloudsat_dpc
======================================

This module defines a provider for the CloudSat DPC SFTP server. To use this
 provider you need to setup your CloudSat DPC account
at https://www.cloudsat.cira.colostate.edu/data-products/2c-ice.

Setup
`````

The CloudSatDPC provider provides access to the SFTP servers of the
 CloudSat DPC. To use it, you need to add a public SSH key to your
account at the DPC website. To connect to the SFTP server,
``pansat`` needs access to the corresponding private key. Therefore,
you will have to add
 1. your SFTP username and
 2. the SSH private key corresponding to the public key you
    have uploaded to the DPC website
as username and password in your pansat identities file.

The private key can be provided either as a path pointing to a
 local file, or by directly storing the base-64 encoded key
as the password.

.. warning::
   Make sure to create a separate SSH key pair for this purpose.
   Do not use the same keys that you are using for other
   purposes as you cannot expect them to be save when added to
   pansat.
"""
from datetime import datetime
from io import StringIO
import paramiko
from pathlib import Path

from pansat.download.accounts import get_identity
from pansat.download.providers.discrete_provider import DiscreteProvider


PRODUCTS = {
    "CloudSat_2C-ICE": "2C-ICE.P1_R05",
    "CloudSat_2B-CLDCLASS": "2B-CLDCLASS.P1_R05",
    "CloudSat_2B-CLDCLASS-LIDAR": "2B-CLDCLASS-LIDAR.P1_R05",
    "CloudSat_2C-RAIN PROFILE": "2C-RAIN-PROFILE.P1_R05",
}

######################################################################
# SFTP Connection
######################################################################


class SFTPConnection:
    """
    Helper class that manages the lifetime of a paramiko SFTP connection.
    """

    def __init__(self, host, provider):
        """
        Open connection to host. Credentials are obtained from the pansat
        identities database.
        """
        self.host = host
        self.provider = provider

        self.transport = None
        self.sftp = None
        self._connect()

    def _connect(self):
        user_name, key = get_identity(self.provider)

        if not Path(key).exists():
            key_buffer = StringIO()
            key_buffer.write("-----BEGIN OPENSSH PRIVATE KEY-----\n")
            key_buffer.write(key + "\n")
            key_buffer.write("-----END OPENSSH PRIVATE KEY-----\n")
        else:
            with open(key, "r") as key_file:
                key_buffer = StringIO(key_file.read())
        key_buffer.seek(0)
        key = paramiko.RSAKey.from_private_key(key_buffer)
        self.transport = paramiko.Transport(self.host)
        self.transport.connect(username=user_name, pkey=key)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)

    def list_files(self, path):
        """
        List files in given remote directory.

        Args:
            path: The path to the directory for which to list the files.

        Return:
            List containing the files in the given directory.
        """
        return self.sftp.listdir(path)

    def download(self, path, destination):
        """
        Download file to destination.

        Args:
            path: The remote path of the file to download.
            destination: Path to the file to which to write
                the results.
        """
        self.sftp.getfo(path, open(destination, "wb"))

    def ensure_connection(self):
        """
        Ensure that SSH connection is still alive.
        """
        try:
            self.transport.send_ignore()
        except EOFError:
            self._connect()

    def __del__(self):
        """Close connection."""
        if self.sftp is not None:
            self.sftp.close()
            self.sftp = None
        if self.transport is not None:
            self.transport.close()
            self.transport = None


######################################################################
# SFTP Connection
######################################################################


class CloudSatDPCProvider(DiscreteProvider):
    """
    Data provider class for the CloudSat DPC SFTP server.
    """

    def __init__(self, product):
        """
        Args:
            product: The pansat product to download.
        """
        super().__init__(product)
        self.product = product
        self._connection = None

    @property
    def connection(self):
        """SFTP connection object to the data server."""
        if self._connection is None:
            self._connection = SFTPConnection(
                "www.cloudsat.cira.colostate.edu", "CloudSatDPC"
            )
        return self._connection

    @connection.setter
    def connection(self, con):
        self._connection = con

    @staticmethod
    def get_available_products():
        return list(PRODUCTS.keys())

    def get_files_by_day(self, year, day):
        """
        Get the available for a given day of a year.

        Args:
            year: The year
            day: The Julian day

        Return:
            List of files available for the given day.
        """
        self.connection = SFTPConnection(
            "www.cloudsat.cira.colostate.edu", "CloudSatDPC"
        )
        try:
            directory = f"/Data/{PRODUCTS[self.product.name]}/{year:04}/{day:03}"
            self.connection.ensure_connection()
            return self.connection.list_files(directory)
        except FileNotFoundError as e:
            return []

    def download_file(self, filename, destination):
        """
        Download a given file and write the results to the given destination.

        Args:
            filename: The filename of the file to download.
            destination: The destination to which to write the downloaded file.
        """
        date = self.product.filename_to_date(filename)

        year = date.year
        day = (date - datetime(date.year, 1, 1)).days + 1
        path = f"/Data/{PRODUCTS[self.product.name]}/" f"{year:04}/{day:03}/{filename}"
        self.connection.ensure_connection()
        self.connection.download(path, destination)
