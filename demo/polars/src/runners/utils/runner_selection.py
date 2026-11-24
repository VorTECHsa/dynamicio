"""Pipeline utilities for all tasks."""
import argparse
import logging
from signal import signal
from types import FrameType
from typing import Callable

logger = logging.getLogger(__name__)


def custom_signal_handler(signal_received: int, frame: FrameType, default_func: Callable[[int, FrameType], None]):
    """A custom signal handler for managing SIGINT and SIGTERM signals.

    Args:
        signal_received: The signal received
        frame: The frame from which the signal was sent.
        default_func: The handler to delegate to.
    """
    logger.info(f"Termination signal detected: {signal_received}.  Exiting gracefully...")
    default_func(signal, frame)


def create_parser(airflow_task_modules: dict) -> argparse.ArgumentParser:
    """Generates an argument parser for the docker container's entry point, namely for `pipeline.src.main`.

    - Parameter name: `--with_module`
    - Options:  staging, transform or sink
    Args:
        airflow_task_modules: The names of the airflow tasks to be chosen
    Returns:
        argparse.ArgumentParser
    """
    _parser = argparse.ArgumentParser()
    _parser.add_argument("--with_module", type=str, choices=list(airflow_task_modules.keys()))
    return _parser


def choose_module(module_arg: str, airflow_task_modules: dict):
    """A function for choosing the a module to serve as the entry point for an Airflow task.

    A choice can be provided from the below list:

    - "staging"
    - "transform"
    - "sink"

    Args:
        module_arg: str: The parsed argument provided as a parameter in an airflow task in the respective airflow dag.
        airflow_task_modules:

    Returns:
        func: Return the respective module, based on the provided input.
    """
    try:
        return airflow_task_modules[module_arg]
    except KeyError as error_key:
        raise ValueError("Invalid input argument...") from error_key
