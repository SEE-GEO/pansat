"""
pansat.download.accounts
========================

The ``accounts`` sub-module parses and provides the account information for
different data sources. ``pansat`` uses a configuration file with the name
``identitides.json`` to store identities for different data providers. The
path of the ``identities.ini`` file is determined in a platform-independent
way using the ``appdirs`` module.

.. warning::

   pansat currently does not encrypt passwords and stores them in the user
   directory. This is a security issue and fixed as soon as possible.

"""
from appdirs import user_config_dir
import getpass
import json
import keyring
import os
from pathlib import Path

identities = {}
"""
Dictionaries containing provider names and corresponding user names.
"""

# The directory containing the configuration file.
_app_dir = Path(user_config_dir("pansat", "pansat"))
_app_dir.mkdir(parents=True, exist_ok=True)

# The path to the configuration file.
_identity_file = Path(_app_dir)
_identity_file /= Path("identities.json")

###############################################################################
# Handling identities
###############################################################################


def parse_identity_file():
    """
    If available, parses identity config file and adds entries to known
    identities.
    """
    if _identity_file.exists():
        global identities
        identities = json.loads(open(_identity_file).read())
    else:
        with open(_identity_file, "w") as file:
            file.write(json.dumps(identities))


def add_identity(provider, user_name):
    """
    Add identity to known identities.

    Args:
        provider(``str``): Name of the data provider class for which the user
        name is valid.
        user(``str``): User name for the data provider.
    """
    identities[provider] = (user_name, getpass.getpass())
    with open(_identity_file, "w") as file:
        file.write(json.dumps(identities))


def get_identity(provider):
    """
    Retrieve identity for given provider.

    Args:
       provider(``str``): Name of provider.

    Returns:
       Tuple ``(user_name, password)`` containing the user name and password
       for the given domain.

    Raises:
       Exception, if no identity for the given domain could be found.
    """
    if provider in identities:
        return identities[provider]
    else:
        raise Exception(
            f"Could not find identity for {provider}. Add section to "
            " to configuration file {_identity_file} or add an identity"
            " manually using the 'add_identity' method."
        )


parse_identity_file()
