"""A package for wrapping your I/O operations."""

import logging

from dynamicio.io import FileResource, KafkaResource, PostgresResource, S3Resource

logging.getLogger(__name__).addHandler(logging.NullHandler())
