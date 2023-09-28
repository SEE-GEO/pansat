import os

import click

import pansat.download

@click.group()
def pansat_cli():
    pass

@click.command()
def config():
    """
    Displays information about the current pansat configuration.
    """
    pansat_passwd = os.environ.get("PANSAT_PASSWORD", "<not set>")

    click.echo(f"""
    pansat configuration:
    ---------------------

    Identity file:      {pansat.download.accounts._IDENTITY_FILE}
    'PANSAT_PASSWORD':  {pansat_passwd}

    """)

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


account.add_command(list_accounts)



pansat_cli.add_command(config)
pansat_cli.add_command(account)
