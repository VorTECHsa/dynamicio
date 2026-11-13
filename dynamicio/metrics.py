"""Backwards-compatible alias: `dynamicio.metrics` now lives in `dynamicio.schema.metrics`; the old path resolves to the same module object."""

import importlib
import sys

# `dynamicio.schema.metrics` is also the name of a function there (the typed `metrics(...)` helper), so the module is fetched by its path.
sys.modules[__name__] = importlib.import_module("dynamicio.schema.metrics")
