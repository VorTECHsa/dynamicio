"""A package for wrapping your I/O operations."""
import os
from contextlib import suppress

import pkg_resources
from magic_logger import logger

with suppress(Exception):
    __version__ = pkg_resources.get_distribution("dynamicio").version

from dynamicio.core import DynamicDataIO
from dynamicio.mixins import WithKafka, WithLocal, WithLocalBatch, WithPostgres, WithS3File, WithS3PathPrefix

os.environ["LC_CTYPE"] = "en_US.UTF"  # Set your locale to a unicode-compatible one


class UnifiedIO(WithS3File, WithS3PathPrefix, WithLocalBatch, WithLocal, WithKafka, WithPostgres, DynamicDataIO):  # type: ignore
    """A unified io composed of dynamicio.mixins."""


logging_config = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
        "generic-metrics": {"format": "%(message)s"},
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
        "metrics": {
            "level": "INFO",
            "formatter": "generic-metrics",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",  # Default is stderr
        },
    },
    "loggers": {
        "": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "dynamicio.metrics": {"handlers": ["metrics"], "level": "INFO", "propagate": False},
        "awscli": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

logger.dict_config(logging_config)
