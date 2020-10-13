from pathlib import Path, PurePath
import pytest
import os

"""
Tests for management of user accounts.
"""


def test_initialize_identity_file(monkeypatch, tmpdir):
    """
    This tests creates a new identities.json file in a temporary directory,
    adds an identity to it, re-reads it from disk and extracts user and
    entered password.
    """
    monkeypatch.setattr("getpass.getpass", lambda: "abcd")
    monkeypatch.setattr("appdirs.user_config_dir", lambda x, y: tmpdir)

    import pansat.download.accounts as accs

    assert (tmpdir / "identities.json").exists()

    accs.add_identity("provider", "user_name")
    user_name, password = accs.get_identity("provider")

    assert user_name == "user_name"
    assert password == "abcd"


def test_parse_identity_failure(monkeypatch, tmpdir):
    """
    This test reads the identity file from the ``test_data`` folder and
    tries to authenticate with a wrong password. This test asserts that
    an exception is thrown.
    """
    test_data = Path(PurePath(__file__).parent / "test_data" / "identities.json")
    import pansat.download.accounts as accs

    monkeypatch.setattr("os.environ", {})
    monkeypatch.setattr("pansat.download.accounts._IDENTITY_FILE", test_data)
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)
    monkeypatch.setattr("getpass.getpass", lambda: "abcd")

    with pytest.raises(accs.WrongPasswordError):
        accs.parse_identity_file()
        accs.authenticate()


HAS_PANSAT_PASSWORD = "PANSAT_PASSWORD" in os.environ


@pytest.mark.skipif(not HAS_PANSAT_PASSWORD, reason="Pansat password not set.")
def test_parse_data_provider_failure(monkeypatch):
    """
    This test reads the identity file from the ``test_data`` folder and
    request login information for an unknown provider. This test asserts
    that the expected exception is thrown.
    """
    test_data = Path(PurePath(__file__).parent / "test_data" / "identities.json")
    import pansat.download.accounts as accs

    monkeypatch.setattr("pansat.download.accounts._IDENTITY_FILE", test_data)
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)

    accs.parse_identity_file()
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
    test_data = Path(PurePath(__file__).parent / "test_data" / "identities.json")
    import pansat.download.accounts as accs

    monkeypatch.setattr("pansat.download.accounts._IDENTITY_FILE", test_data)
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)

    accs.parse_identity_file()
    accs.authenticate()
    login = accs.get_identity("Icare")
    assert login
