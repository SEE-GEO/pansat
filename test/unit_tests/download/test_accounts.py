"""
Tests for management of user accounts.
"""
import os
from pathlib import Path, PurePath
import shutil
import pytest
import pansat.download.accounts as accs


def test_initialize_identity_file(monkeypatch, tmpdir):
    """
    This tests creates a new identities.json file in a temporary directory,
    adds an identity to it, re-reads it from disk and extracts user and
    entered password.
    """
    identity_file = Path(tmpdir / "identities.json")

    monkeypatch.setattr("getpass.getpass", lambda: "abcd")
    monkeypatch.setattr("pansat.download.accounts._IDENTITY_FILE", identity_file)
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)
    accs.parse_identity_file()

    assert (tmpdir / "identities.json").exists()

    accs.add_identity("provider", "user_name")
    user_name, password = accs.get_identity("provider")

    assert user_name == "user_name"
    assert password == "abcd"


def test_add_identity_file(monkeypatch, tmpdir):
    """
    This file tests adding a new account to an existing identities file to ensure
    that the identities file is decrypted before an account is added to it.
    """
    identity_file = Path(tmpdir / "identities.json")
    shutil.copyfile(accs._IDENTITY_FILE, identity_file)

    monkeypatch.setattr("getpass.getpass", lambda: "abcd")
    monkeypatch.setattr("pansat.download.accounts._IDENTITY_FILE", identity_file)
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)
    accs.parse_identity_file()

    assert (tmpdir / "identities.json").exists()

    accs.add_identity("provider", "user_name")
    user_name, password = accs.get_identity("provider")

    assert user_name == "user_name"
    assert password == "abcd"


@pytest.mark.usefixtures("test_identities")
def test_parse_identity_failure(monkeypatch, tmpdir):
    """
    This test reads the identity file from the ``test_data`` folder and
    tries to authenticate with a wrong password. This test asserts that
    an exception is thrown.
    """
    test_data = Path(PurePath(__file__).parent / "test_data" / "identities.json")

    monkeypatch.setattr("os.environ", {})
    monkeypatch.setattr("getpass.getpass", lambda: "abcd")

    with pytest.raises(accs.WrongPasswordError):
        accs.parse_identity_file()
        accs.authenticate()


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
@pytest.mark.usefixtures("test_identities")
def test_parse_data_provider_failure(monkeypatch):
    """
    This test reads the identity file from the ``test_data`` folder and
    request login information for an unknown provider. This test asserts
    that the expected exception is thrown.
    """
    accs.authenticate()
    with pytest.raises(accs.MissingProviderError):
        accs.get_identity("Unknown")


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
def test_parse_identity(monkeypatch):
    """
    This test reads the identity file from the ``test_data`` folder and
    tries to  authenticates with the password read from the PANSAT_PASSWORD
    environment variable.
    """
    accs.authenticate()
    login = accs.get_identity("Icare")
    assert login
