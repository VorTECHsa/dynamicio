# flake8: noqa: I101

"""Functional io pydantic models for supported I/O targets."""

from dynamicio.io.file import FileResource
from dynamicio.io.kafka import KafkaResource
from dynamicio.io.postgres import PostgresResource
from dynamicio.io.s3 import S3Resource
