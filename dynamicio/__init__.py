"""A package for wrapping your I/O operations."""

import logging

from dynamicio.io import LocalFileResource, S3Resource, PostgresResource, KafkaResource

logging.getLogger(__name__).addHandler(logging.NullHandler())
