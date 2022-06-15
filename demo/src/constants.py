"""A module for configuring all constants:"""

# Parquet
TO_PARQUET_KWARGS = {
    "use_deprecated_int96_timestamps": False,
    "coerce_timestamps": "ms",
    "allow_truncated_timestamps": True,
}
