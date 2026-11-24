"""The file formats that the two engines read and write the same way.

- ``parquet``: the footer read of ``DynamicDataIO.footer()``, and the ``index_columns`` option of a parquet write.
- ``json``: the ``index_column`` option of a JSON resource.
"""
