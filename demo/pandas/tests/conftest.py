# pylint: disable=missing-module-docstring


def pytest_ignore_collect(collection_path, config):  # pylint: disable=unused-argument
    """The pandas demo runs on the pandas engine. ``--dynamicio-engine=polars`` runs the polars demo of ``demo/polars`` instead."""
    return True if config.getoption("dynamicio_engine", default=None) == "polars" else None
