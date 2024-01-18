"""
pansat.config
=============

This module handles the configuration of pansat.
"""
from configparser import ConfigParser, SectionProxy
from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import List, Optional


from appdirs import user_config_dir
import tomlkit

from pansat.environment import Registry, DataDir


LOGGER = logging.getLogger(__name__)


_CURRENT_CONFIG = None

# The directory containing the configuration file.
USER_CONFIG_DIR = Path(user_config_dir("pansat", "pansat"))


@dataclass
class PansatConfig:
    """
    Dataclass representing pansat configurations.
    """

    identity_file: str
    registries: List[Registry]

    def __init__(
        self,
        identity_file: Optional[str] = None,
        registries: Optional[List[Registry]] = None,
    ):
        """
        Create a new pansat configuration. If called without arguments
        creates the default configuration.

        Args:
            identity_file: Path pointing to the pansat identity file.
            registries: A list of registries.
        """
        if identity_file is None:
            identity_file = Path(USER_CONFIG_DIR) / Path("identities.json")
        self.identity_file = identity_file

        if registries is None:
            registries = [get_user_registry()]
        self.registries = registries

    def parse(self, path: Path):
        """
        Load settings from a pansat config file.

        Reads the file and setts the attributes of this config object
        to the parameters read from the config file.

        Args:
            path: Path object pointing to the config file to read.
        """

        doc = tomlkit.parse(open(path).read())

        general = doc.get("general", None)
        if general is not None:
            identity_file = general.get("identity_file", self.identity_file)

        parent = None
        if len(self.registries) > 0:
            parent = self.registries[-1]

        reg_tables = doc.get("registry", {})

        parsed_regs = []

        for reg, reg_dict in reg_tables.items():
            path = reg_dict.get("path", None)
            if path is None:
                raise RuntimeError(f"Registry entry {reg} lack 'path' argument.")
            path = Path(path)
            if not path.exists() or not path.is_dir():
                LOGGER.warning(
                    f"Path argument in registry '{reg}' does not point to an "
                    "existing folder. It will therefore be ignored."
                )
            is_data_dir = reg_dict.get("is_data_dir", False)
            transparent = reg_dict.get("transparent", True)

            if is_data_dir:
                reg_class = DataDir
            else:
                reg_class = Registry

            parsed_regs.append(
                reg_class(reg, path, transparent=transparent, parent=parent)
            )

        self.registries += parsed_regs[::-1]

    def write(self, path: Path) -> None:
        """
        Write configuration to .toml file.

        Args:
            path: Path pointing to a .toml file that the configuration will
                be written to.
        """
        doc = tomlkit.document()
        doc.add(tomlkit.comment("pansat configuration file"))
        doc.add(tomlkit.nl())

        general = tomlkit.table()
        general.add("identity_file", str(self.identity_file))
        doc.add("general", general)

        registries = tomlkit.table()
        for registry in self.registries:
            reg_table = tomlkit.table()
            reg_table.add("path", str(registry.path))
            reg_table.add("transparent", registry.transparent)
            reg_table.add("data_dir", isinstance(registry, DataDir))
            registries.add(f"{registry.name}", reg_table)
        doc.add("registry", registries)

        with open(path, "w") as output:
            output.write(tomlkit.dumps(doc))


_USER_REGISTRY = None


def get_user_registry() -> Registry:
    """
    Get the user registry, which is

    """
    global _USER_REGISTRY
    if _USER_REGISTRY is None:
        registry_dir = Path(USER_CONFIG_DIR / "registry")
        registry_dir.mkdir(exist_ok=True)
        _USER_REGISTRY = Registry("user_registry", registry_dir)
    return _USER_REGISTRY


def find_config_dir() -> Path:
    """
    Recursively searches up the directory tree for a folder named
    '.pansat'. If found this is returned as config folder. If no
    such folder is found, returns the user pansat config.

    Return:
        Path in which the pansat config.toml file is expected.
    """
    curr_path = Path.cwd()
    while curr_path != curr_path.parent:
        pansat_path = curr_path / ".pansat"
        if pansat_path.exists() and pansat_path.is_dir():
            return pansat_path
        curr_path = curr_path.parent

    config_dir = Path(user_config_dir("pansat", "pansat"))
    return config_dir


def get_current_config() -> PansatConfig:
    """
    Get the current, active pansat configuration

    """
    global _CURRENT_CONFIG
    if _CURRENT_CONFIG is None:
        _CURRENT_CONFIG = PansatConfig()
        config_dir = find_config_dir()
        config_file = config_dir / "config.toml"
        if config_file.exists():
            _CURRENT_CONFIG.parse(config_file)
    return _CURRENT_CONFIG


CONFIG_DISPLAY_PATTERN = """
Active pansat configuration:
============================

Config directory: {config_dir}
Identity file:    {identity_file}

Registries:
-----------

{registries}
"""


def display_current_config() -> str:
    """
    Renders the current config for display on the command line.
    """
    config = get_current_config()

    registries = ""
    for ind, registry in enumerate(config.registries[::-1]):
        registries += f"{ind + 1}) {registry.name}: {registry.location} \n"

    config_str = CONFIG_DISPLAY_PATTERN.format(
        **{
            "config_dir": find_config_dir(),
            "identity_file": config.identity_file,
            "registries": registries,
        }
    )
    return config_str
