import os
from pathlib import Path

import click

import pansat.download
from pansat.config import display_current_config

@click.group()
def pansat_cli():
    pass

@click.command()
def config():
    """
    Displays information about the current pansat configuration.
    """
    click.echo(
        display_current_config()
    )

@click.group()
def account():
    """
    Inspect, add and modify accounts.
    """
    pass

@click.command()
def list_accounts(name="list"):
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

@click.command()
@click.argument("path")
def index(
        path: Path
):
    """
    Displays information about the current pansat configuration.
    """
    import pansat.environment as penv
    from pansat.catalog import Catalog
    reg = penv.get_active_registry()

    catalog = Catalog.from_existing_files(path)

    for name, index in catalog.indices.items():
        if name in reg.indices:
            reg.indices[name] = reg.indices[name] + index
        else:
            reg.indices[name] = index
    reg.save()


account.add_command(list_accounts)

pansat_cli.add_command(index)
pansat_cli.add_command(config)
pansat_cli.add_command(account)
