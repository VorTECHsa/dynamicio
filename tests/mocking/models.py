"""A module for defining sql_alchemy models."""
# pylint: disable=too-few-public-methods, R0801, C0104
__all__ = ["ERModel"]

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class ERModel(Base):
    """
    Sql_alchemy model for example table

    """

    __tablename__ = "example"

    id = Column(String, primary_key=True)
    foo = Column(String)
    bar = Column(Integer)
    baz = Column(String)
