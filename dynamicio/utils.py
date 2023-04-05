"""Utilities for dynamicio."""

from contextlib import contextmanager


@contextmanager
def pickle_protocol(protocol: int):
    """Downgrade to the provided pickle protocol within the context manager.

    Args:
        protocol: The number of the protocol HIGHEST_PROTOCOL to downgrade to.
    """
    import pickle  # pylint: disable=import-outside-toplevel

    previous = pickle.HIGHEST_PROTOCOL
    try:
        pickle.HIGHEST_PROTOCOL = protocol
        yield
    finally:
        pickle.HIGHEST_PROTOCOL = previous
