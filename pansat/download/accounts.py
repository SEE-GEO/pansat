"""
pansat.download.accounts
========================

The ``accounts`` sub-module handles login data for different data portals.
The login data is stored in encrypted format in a configuration
file ``identities.json`` in the user's home directory tree.

Upon first usage the ``identities.json`` file is setup with a custom user
password. This password is used to encrypt all data passwords that are
subsequently added by the user. The Fernet method is used to en- and decrypt
the passwords. All password hashing is performed using random salt.
"""
import base64
import getpass
import json
import logging
import os
from pathlib import Path

from appdirs import user_config_dir
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Dictionary containing provider names and corresponding user names.
_IDENTITIES = None

# The directory containing the configuration file.
_APP_DIR = Path(user_config_dir("pansat", "pansat"))
_APP_DIR.mkdir(parents=True, exist_ok=True)

# The path to the configuration file.
_IDENTITY_FILE = os.environ.get("PANSAT_IDENTITIES_FILE")
if _IDENTITY_FILE:
    _IDENTITY_FILE = Path(_IDENTITY_FILE)
else:
    _IDENTITY_FILE = Path(_APP_DIR) / Path("identities.json")

_PANSAT_SECRET = None

LOGGER = logging.getLogger(__name__)

###############################################################################
# Handling identities
###############################################################################


class WrongPasswordError(Exception):
    """
    Exception that is thrown when an entered password in incorrect.
    """


class MissingProviderError(Exception):
    """
    Exception that is thrown when no login data can be found for a given
    data provider name.
    """


def get_password(check=False):
    """
    Check if password is provided as ``PANSAT_PASSWORD`` environment variable.
    If this is not the case query user to input password.

    Params:
        check(``bool``): Whether user should insert the password twice
            to avoid spelling errors.

    Return:
        ``str`` containing the password
    """
    try:
        password = os.environ["PANSAT_PASSWORD"]
    except KeyError:
        print("Please enter your pansat user password:")
        password = getpass.getpass()

        if check:
            print("Please repeat the password:")
            password_re = getpass.getpass()
            if password != password_re:
                LOGGER.warning("The two passwords don't match!")
                get_password(check=check)

    return password


def hash_password(password, salt):
    """
    Computes the hash of a password.

    Args:
        password(``str``): The password
        salt(``bytes``): The salt to use to compute the hash.

    Return:
        The base64 encoded hash of the password.
    """
    password = password.encode()
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000)
    return base64.urlsafe_b64encode(kdf.derive(password))


def decrypt(secret):
    """
    Decrypt password.

    Args:
        secret(``str``): The encrypted password

    Return:
        The decrypted password.

    Raises:

        cryptography.fernet.InvalidToken: Raised if the encrypted password
            is invalid.
    """
    fernet = Fernet(_PANSAT_SECRET)
    return fernet.decrypt(secret).decode("utf8")


def encrypt(password):
    """
    Encript password.

    Args:
        password(``str``): The password to encrypt.

    Return:
        The encrypted password as ``bytes``.
    """
    fernet = Fernet(_PANSAT_SECRET)
    return fernet.encrypt(password.encode())


def authenticate():
    """
    Verify pansat password.

    Reads pansat password from environment variable ``PANSAT_PASSWORD``.
    If this is not available, queries user to input password from standard
    in. Verifies password using the hash stored in the identity list.

    Raises:

        WrongPasswordError if the provided password is incorrect.
    """
    global _PANSAT_SECRET

    if _PANSAT_SECRET:
        return

    secret_hashed, salt = get_identities()["pansat"]
    secret_hashed = secret_hashed.encode()
    salt = salt.encode()

    password = get_password()

    entered_secret = hash_password(password, salt)
    entered_secret_hashed = hash_password(entered_secret.decode(), salt)

    if secret_hashed != entered_secret_hashed:
        LOGGER.error("Wrong password")
        raise WrongPasswordError("The password you entered is incorrect.")
    LOGGER.info("Authentification successful.")

    _PANSAT_SECRET = entered_secret


def initialize_identity_file():
    """
    Initializes a new identity file.

    If no existing identity file is found in the user directory, this
    function sets up a new one by generating a secret key using random
    salt and storing a hash of the key and the salt in the identity
    file.

    The salt and the hashed key can then be used to verify the user
    during subsequent logins.
    """
    global _IDENTITIES
    global _PANSAT_SECRET

    LOGGER.info("Initializing new identity file: %s", _IDENTITY_FILE)

    password = get_password(check=True)
    salt = base64.urlsafe_b64encode(os.urandom(16))
    _PANSAT_SECRET = hash_password(password, salt)

    secret_hashed = hash_password(_PANSAT_SECRET.decode(), salt)
    _IDENTITIES = {"pansat": (secret_hashed.decode(), salt.decode())}

    with open(_IDENTITY_FILE, "w") as file:
        file.write(json.dumps(_IDENTITIES))


def parse_identity_file():
    """
    If available, parses identity config file and adds entries to known
    identities.
    """
    if _IDENTITY_FILE.exists():
        global _IDENTITIES
        with open(_IDENTITY_FILE) as file:
            _IDENTITIES = json.loads(file.read())

        LOGGER.info("Parsed identity file: %s", _IDENTITY_FILE)

    else:
        initialize_identity_file()


def get_identities():
    """
    Access identity dictionary.

    Return:
        Dictionary holding identities for all known providers.
    """
    global _IDENTITIES
    if not _IDENTITIES:
        parse_identity_file()
    return _IDENTITIES


def add_identity(provider, user_name):
    """
    Add identity to known identities.

    Args:
        provider(``str``): Name of the data provider class for which the user
            name is valid.
        user(``str``): User name for the data provider.
                       ! Note that for Copernicus,you have to use the url from your $HOME/.cdsapirc
                         For more info: https://cds.climate.copernicus.eu/api-how-to
    """
    if not _PANSAT_SECRET:
        authenticate()

    if provider == "Copernicus":
        print(
            f"Please enter the key for provider '{provider}' with url"
            f" '{user_name}':"
        )
    else:
        print(
            f"Please enter password for provider '{provider}' and username"
            f" '{user_name}':"
        )
    password = getpass.getpass()
    password_encrypted = encrypt(password)
    user_name_encrypted = encrypt(user_name)

    identities = get_identities()
    identities[provider] = (user_name_encrypted.decode(), password_encrypted.decode())
    with open(_IDENTITY_FILE, "w") as file:
        file.write(json.dumps(identities))

    LOGGER.info("Added identity: %s, %s", provider, user_name)


def delete_identity(provider):
    """
    Remove identity for provider.

    Args:
        provider(``str``): Name of the data provider whose identity to
            delete.
    """
    if not _PANSAT_SECRET:
        authenticate()
    identities = get_identities()
    del identities[provider]
    with open(_IDENTITY_FILE, "w") as file:
        file.write(json.dumps(identities))

    LOGGER.info("Removed identity for provider %s", provider)


def get_identity(provider):
    """
    Retrieve identity for given provider.

    Args:
        provider(``str``): Name of provider.

    Returns:
        Tuple ``(user_name, password)`` containing the user name and password
            for the given domain.

    Raises:
        MissingProviderError: if no identity for the given domain could be found.
    """
    LOGGER.info("Retrieving identity for provider %s", provider)

    if not _PANSAT_SECRET:
        authenticate()
    identities = get_identities()
    if provider in identities:
        user_name, password = identities[provider]
        password = password.encode()
        password = decrypt(password)
        user_name = user_name.encode()
        user_name = decrypt(user_name)
        return user_name, password

    raise MissingProviderError(
        f"Could not find identity for {provider}. Use the `add_identity`"
        " function to add an identity for the requested provider."
    )
