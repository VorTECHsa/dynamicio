"""Backwards-compatible alias: `dynamicio.validations` now lives in `dynamicio.schema.validations`; the old path resolves to the same module object."""

import sys

from dynamicio.schema import validations

sys.modules[__name__] = validations
