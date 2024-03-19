"""
pansat.cli
==========

Defines the command line interface (CLI) of pansat.
"""
import logging
import os
from pathlib import Path
from typing import List, Optional

import click
import rich
import rich.tree
import rich.panel
import rich.padding
import rich.box


import pansat.logging
import pansat.download
from pansat.config import (
    get_current_config,
    display_current_config,
    Registry,
    DataDir
)

LOGGER = logging.getLogger(__name__)



@click.group()
def pansat_cli():
    pass


@click.command()
def config():
    """
    Displays information about the current pansat configuration.
    """
    click.echo(display_current_config())


@click.group()
def account():
    """
    Inspect, add and modify accounts.
    """
    pass


@click.command(name="list")
def list_accounts():
    identities = pansat.download.accounts.get_identities()
    output = """

Known pansat accounts:
-------------------------------
provider :: username / password

"""
    for provider in identities.keys():
        if provider == "pansat":
            continue
        user_name, password = pansat.download.accounts.get_identity(provider)
        output += f"{provider} :: {user_name} / {password}\n"
    click.echo(output)


@click.command(name="add")
@click.argument("provider_name")
@click.argument("user_name")
def add_account(provider_name, user_name):
    pansat.download.accounts.add_identity(provider_name, user_name)

account.add_command(list_accounts)
account.add_command(add_account)

@click.command("index")
@click.argument("path")
@click.option("--n_processes", type=int, default=None)
@click.option(
    "--products",
    default=None,
    help="List of product names to consider."
)
def index(
        path: Path,
        products: Optional[List[str]] = None,
        n_processes: Optional[int] = None
):
    """
    Index files in a given directory and add the to the currently active
    registry.
    """
    import pansat.environment as penv
    from pansat.catalog import Catalog

    reg = penv.get_active_registry()

    catalog = Catalog.from_existing_files(
        path,
        products=products,
        n_processes=n_processes
    )

    for name, index in catalog.indices.items():
        if name in reg.indices:
            reg.indices[name] = reg.indices[name] + index
        else:
            reg.indices[name] = index
    reg.save()


@click.group()
def catalog():
    """
    Inspect, add and modify registries and data directories.
    """
    pass

@click.command("list")
def list_catalogs():
    """
    List currently active registries and data directories.
    """
    import pansat.environment as penv

    reg = penv.get_active_registry()
    root = rich.tree.Tree(
        "📂 [bold] Active catalogs: [/bold]"
    )
    reg.print_summary(root)
    rich.print(root)

@click.command("add")
@click.argument("kind")
@click.argument("name")
@click.argument("path")
@click.option("--opaque", is_flag=True, default=False)
def add_registry(
        kind: str,
        name: str,
        path: str,
        opaque: bool = True
):
    kind = kind.lower()
    if kind == "registry":
        reg_cls = Registry
    elif kind == "data_directory":
        reg_cls = DataDir
    else:
        LOGGER.error(
            "'kind' should be 'registry' or 'data_directory' not %s.",
            kind
        )
        return 1

    path = Path(path)
    if not path.exists():
        LOGGER.error(
            "The provided path '%s' does not point to an existing directory."
        )

    config = get_current_config()

    new_reg = reg_cls(name, path, transparent=~opaque)
    config.registries.insert(0, new_reg)
    config.write()



catalog.add_command(list_catalogs)
catalog.add_command(add_registry)


pansat_cli.add_command(index)
pansat_cli.add_command(config)
pansat_cli.add_command(account)
pansat_cli.add_command(catalog)
