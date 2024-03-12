"""
Tests for the pansat.download.providers.utils module.
"""
import os

import pytest

from pansat.download.providers.utils import SFTPConnection


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_password_connection():
    """
    Establish connection with ICARE SFTP server using password authentification.
    """
    conn = SFTPConnection("sftp.icare.univ-lille.fr", "Icare")
