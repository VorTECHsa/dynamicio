import typing
from threading import Lock
from contextlib import contextmanager

import pandas as pd

from . import base

hdf_lock = Lock()


@contextmanager
def pickle_protocol(protocol: typing.Optional[int]):
    """Downgrade to the provided pickle protocol within the context manager.

    Args:
        protocol: The number of the protocol HIGHEST_PROTOCOL to downgrade to. Defaults to 4, which covers python 3.4 and higher.
    """
    import pickle  # pylint: disable=import-outside-toplevel

    previous = pickle.HIGHEST_PROTOCOL
    try:
        pickle.HIGHEST_PROTOCOL = 4
        if protocol:
            pickle.HIGHEST_PROTOCOL = protocol
        yield
    finally:
        pickle.HIGHEST_PROTOCOL = previous


class HDF(base.Base):

    use_pickle_protocol: int

    def __init__(self, pickle_protocol: typing.Optional[int] = None) -> None:
        super().__init__()
        self.use_pickle_protocol = pickle_protocol

    def read_local_file(self, file_path: typing.AnyStr, options: dict) -> pd.DataFrame:
        with hdf_lock:
            return pd.read_hdf(file_path, **options)

    def write_local_file(self, df: pd.DataFrame, file_path: typing.AnyStr, options: dict):
        with pickle_protocol(protocol=self.use_pickle_protocol), hdf_lock:
            df.to_hdf(file_path, **options)
