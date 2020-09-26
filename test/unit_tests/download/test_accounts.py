"""
Tests for management of user accounts.
"""


def test_add_parse_get_identity(monkeypatch, tmpdir):
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
