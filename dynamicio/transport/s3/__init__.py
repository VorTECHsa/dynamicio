"""The S3 transfers that the formats and the polars engine share.

- ``tail.S3Tail``: a read-only file of an S3 object, with ranged ``GetObject`` calls, for the footer read of ``formats.parquet``.
- ``upload.S3Upload``: an upload to one key that S3 keeps only when it is complete. ``kept_open.KeptOpen`` lets a failed attempt read its buffer again.
"""
