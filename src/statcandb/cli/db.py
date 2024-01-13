import click
from sqlalchemy import create_engine

from ..config import get_config


@click.group("db")
def db_group() -> None:
    """Functions for interacting with the database"""


@db_group.command("create")
def db_create_command() -> None:
    """
    Create the database schema. Should run once before running the uploader
    """
    from ..models import Base  # pylint: disable=import-outside-toplevel

    config = get_config()

    engine = create_engine(config.sqlite_db_url)
    Base.metadata.create_all(engine)
