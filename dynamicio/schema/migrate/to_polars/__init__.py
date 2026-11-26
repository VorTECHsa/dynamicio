"""Migrate the pandas models of a repo to polars models, and list what else the polars engine needs (``dynamicio migrate --engine polars``).

The command writes one ``dynamicio.schema.polars.DataModel`` for each pandas ``DataModel``, checks that each one compiles to the same dtypes,
validations, metrics and names, and writes a report. The report lists the checks to port by hand and the resource definitions that the polars
engine refuses or reads differently. The command does not change the resource definitions or the transformation code of the repo."""
