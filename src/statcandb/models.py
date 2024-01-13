"""
SQLAlchemy models used throughout these scripts
"""
from datetime import datetime

from sqlalchemy import DateTime
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import (DeclarativeBase, Mapped, MappedAsDataclass,
                            mapped_column)
from sqlalchemy.sql import expression


class Base(DeclarativeBase, MappedAsDataclass):
    pass


class UtcNow(expression.FunctionElement):
    inherit_cache = True
    type = DateTime()


@compiles(UtcNow, "postgresql")
def pg_utcnow(element, compiler, **kwargs) -> str:
    """Postgres utcnow function"""
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"


@compiles(UtcNow, "mssql")
def ms_utcnow(element, compiler, **kwargs) -> str:
    """T-SQL utcnow function"""
    return "GETUTCDATE()"


@compiles(UtcNow, "sqlite")
def sqlite_utcnow(element, compiler, **kwargs) -> str:
    """sqlite utcnow function"""
    return "DATETIME('now')"


class Product(Base):
    """
    A model that represents an audio file attached to a case
    """

    __tablename__ = "products"

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    number: Mapped[int]
    release_time: Mapped[datetime]
    is_uploaded: Mapped[bool] = mapped_column(default=False)

    created_at: Mapped[datetime] = mapped_column(default=UtcNow())
    updated_at: Mapped[datetime] = mapped_column(default=UtcNow(), onupdate=UtcNow())
