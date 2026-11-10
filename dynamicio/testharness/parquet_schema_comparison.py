"""Compares the physical layout of two parquet files, which a comparison of the loaded frames can not see."""

from typing import List


class ParquetSchemaComparison:
    """The arrow schema (names, types, nullability, without the key-value metadata) and the pandas index columns of two parquet files."""

    @staticmethod
    def differences(actual_path: str, expected_path: str) -> List[str]:
        """Describe how the layout of the file at ``actual_path`` differs from the one at ``expected_path`` (empty when they agree)."""
        import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel

        actual, expected = pq.read_schema(actual_path), pq.read_schema(expected_path)
        found = []
        if not actual.remove_metadata().equals(expected.remove_metadata()):
            found.append(f"parquet schema {actual.remove_metadata()} != expected {expected.remove_metadata()}")
        actual_index, expected_index = ParquetSchemaComparison._index_columns(actual), ParquetSchemaComparison._index_columns(expected)
        if actual_index != expected_index:
            found.append(f"pandas index {actual_index} != expected {expected_index}")
        return found

    @staticmethod
    def _index_columns(schema) -> list:
        return (schema.pandas_metadata or {}).get("index_columns", [])

    @classmethod
    def compare(cls, actual_path: str, expected_path: str) -> None:
        """Raise ``AssertionError`` listing the differences, if any."""
        found = cls.differences(actual_path, expected_path)
        if found:
            raise AssertionError("\n".join(found))
