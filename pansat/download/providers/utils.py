"""
pansat.download.providers.utils
===============================

Provides utility functionality for pansat's data providers.
"""
import paramiko
from pansat.download.accounts import get_identity


class SFTPConnection:
    """
    Helper class that manages the lifetime of a paramiko SFTP connection.
    """
    def __init__(self, host, provider, mode="password"):
        """
        Open connection to host. Credentials are obtained from the pansat
        identities database.
        """
        self.host = host
        self.provider = provider
        self.mode = mode

        self.transport = None
        self.sftp = None

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, *_):
        if self.sftp is not None:
            self.sftp.close()
            self.sftp = None
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    def _connect(self):
        """Establish SFTP connection."""
        user_name, key = get_identity(self.provider)
        if self.mode.lower() == "key":
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
            kwargs = {"pkey": key}
        else:
            kwargs = {"password": key}
        self.transport = paramiko.Transport(self.host)
        self.transport.connect(username=user_name, **kwargs)
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
        if self.transport is None:
            return self._connect()
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
