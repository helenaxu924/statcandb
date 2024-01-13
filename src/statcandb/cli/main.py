import click
from dotenv import load_dotenv

from statcandb.config import set_config_from_env

from .db import db_group
from .delta import delta_group
from .full import full_group

load_dotenv()
set_config_from_env()


@click.group()
def cli():
    """Basic CLI wrapper"""


cli.add_command(delta_group)
cli.add_command(full_group)
cli.add_command(db_group)

if __name__ == "__main__":
    cli()
