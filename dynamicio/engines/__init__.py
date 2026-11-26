"""The engines of dynamicio: the dataframe library that reads, writes, casts, validates and logs metrics for all resources of a process.

Set the engine once, at the start of the process, before the first read or write::

    import dynamicio
    dynamicio.configure(engine="polars")
"""

from dynamicio.engines.engine import Engine
from dynamicio.engines.engine_setting import EngineSetting
from dynamicio.engines.pandas_engine import PandasEngine
from dynamicio.engines.polars_engine import PolarsEngine
from dynamicio.engines.registry import Registry

configure = EngineSetting.configure

__all__ = ["Engine", "EngineSetting", "PandasEngine", "PolarsEngine", "Registry", "configure"]
