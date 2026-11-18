# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import pandas as pd
import pytest

# Application Imports
from tests.mocking.io import ReadPostgresIO, WritePostgresIO

pytestmark = [pytest.mark.integration, pytest.mark.floci]


class TestPostgresCopyWrite:
    """The pandas engine writes with `COPY ... FROM STDIN WITH (FORMAT text, NULL '')`, which SQLite cannot run."""

    @staticmethod
    def read(config):
        return ReadPostgresIO(source_config=config).read().sort_values("bar").reset_index(drop=True)

    def test_a_written_table_reads_back_the_same_rows(self, pg_config):
        # Given
        df = pd.DataFrame({"id": ["1", "2", None], "foo": ["a", None, "c"], "bar": [1, 2, 3], "baz": ["x", "y", "é"]})

        # When
        WritePostgresIO(source_config=pg_config, create_table=True).write(df)

        # Then
        pd.testing.assert_frame_equal(self.read(pg_config), df)

    def test_an_empty_string_is_written_as_null(self, pg_config):
        # Given the NULL rule of the COPY: an empty field is NULL
        df = pd.DataFrame({"id": ["1"], "foo": [""], "bar": [1], "baz": ["x"]})

        # When
        WritePostgresIO(source_config=pg_config, create_table=True).write(df)

        # Then
        assert self.read(pg_config).loc[0, "foo"] is None

    @pytest.mark.parametrize("truncate_and_append", [False, True])
    def test_a_second_write_replaces_the_rows(self, pg_config, truncate_and_append):
        # Given
        WritePostgresIO(source_config=pg_config, create_table=True).write(pd.DataFrame({"id": ["1"], "foo": ["a"], "bar": [1], "baz": ["x"]}))
        df = pd.DataFrame({"id": ["2", "3"], "foo": ["b", "c"], "bar": [2, 3], "baz": ["y", "z"]})

        # When
        WritePostgresIO(source_config=pg_config, truncate_and_append=truncate_and_append).write(df)

        # Then
        pd.testing.assert_frame_equal(self.read(pg_config), df)

    def test_a_large_write_keeps_all_rows_and_nulls(self, pg_config):
        # Given
        n = 250_000
        df = pd.DataFrame({"id": [str(i) for i in range(n)], "foo": [None if i % 7 == 0 else f"f{i}" for i in range(n)], "bar": range(n), "baz": "z"})

        # When
        WritePostgresIO(source_config=pg_config, create_table=True).write(df)

        # Then
        result = self.read(pg_config)
        assert len(result) == n
        assert result["foo"].isna().sum() == len(range(0, n, 7))
