"""A package for wrapping your I/O operations."""

from dynamicio.io import *
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())
