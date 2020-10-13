"""
Contains fixtures that are automatically available in all test files.
"""

import pytest
from pathlib import PurePath, Path
import pansat.download.accounts as accs


@pytest.fixture()
def test_identities(monkeypatch):
    """
    Fixture that makes all tests use the test identities file that contains the
    test login data for data providers.
    """
    test_identity_file = Path(
        PurePath(__file__).parent / "test_data" / "identities.json"
    )
    monkeypatch.setattr("pansat.download.accounts._IDENTITY_FILE", test_identity_file)
    monkeypatch.setattr("pansat.download.accounts._PANSAT_SECRET", None)
    accs.parse_identity_file()
