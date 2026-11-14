"""Backwards-compatible alias: `dynamicio.metrics` now lives in `dynamicio.schema.metrics`; the old path resolves to the same module object."""

import sys

from dynamicio.schema import metrics

sys.modules[__name__] = metrics
