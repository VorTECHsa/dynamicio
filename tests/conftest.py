# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring,
import os
import pickle
import pickletools
import tempfile
from unittest.mock import patch, Mock

import numpy as np
import pandas as pd
import pytest

from dynamicio import WithS3PathPrefix
from tests import constants
from tests.mocking.models import ERModel

TEST_SQL_DIR = os.path.dirname(os.path.abspath(__file__)) + "/test_sql/"
__pickle_loads = pickle.loads


def mock_pickle_loads(data):
    global MAX_PROTO_FOUND  # pylint: disable=global-variable-undefined
    op, fst, _ = next(pickletools.genops(data))  # pylint: disable=invalid-name)
    if op.name == "PROTO":
        proto = fst
        MAX_PROTO_FOUND = max(MAX_PROTO_FOUND, proto)
    return __pickle_loads(data)


def max_pklproto_hdf(hdf_filename):
    global MAX_PROTO_FOUND  # pylint: disable=global-variable-undefined
    MAX_PROTO_FOUND = -1
    with pytest.MonkeyPatch().context() as mocked_context:
        mocked_context.setattr(pickle, "loads", mock_pickle_loads)
        try:
            pd.read_hdf(hdf_filename)
        except ValueError:
            pass
    return MAX_PROTO_FOUND


class DummyYaml:
    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return f"DummyYaml({self.path!r})"

    def __enter__(self):
        return Mock(), None

    def __exit__(self, *args):
        return None


@pytest.fixture(scope="class")
def expected_input_yaml_dict():
    return {
        "READ_FROM_KAFKA": {
            "CLOUD": {
                "kafka": {"kafka_server": "mock-kafka-server", "kafka_topic": "mock-kafka-topic"},
                "type": "kafka",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/some_parquet_to_read.parquet",
                    "file_type": "parquet",
                },
                "type": "local",
            },
        },
        "READ_FROM_PARQUET_TEMPLATED": {
            "CLOUD": {
                "s3": {
                    "bucket": "mock-bucket",
                    "file_path": "path/to/{file_name_to_replace}.parquet",
                    "file_type": "parquet",
                },
                "type": "s3",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/{{file_name_to_replace}}.parquet",
                    "file_type": "parquet",
                },
                "type": "local",
            },
        },
        "READ_FROM_POSTGRES": {
            "CLOUD": {
                "postgres": {
                    "db_host": "127.0.0.1",
                    "db_name": "backend",
                    "db_password": "pass",
                    "db_port": "17039",
                    "db_user": "user",
                },
                "type": "postgres",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/some_pg_parquet_to_read.parquet",
                    "file_type": "parquet",
                },
                "type": "local",
            },
        },
        "READ_FROM_S3_CSV": {
            "CLOUD": {
                "s3": {"bucket": "mock-bucket", "file_path": "mock-key", "file_type": "csv"},
                "type": "s3",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/some_csv_to_read.csv",
                    "file_type": "csv",
                },
                "type": "local",
            },
            "schema": {"file_path": f"{constants.TEST_RESOURCES}/schemas/read_from_s3_csv.yaml"},
        },
        "READ_FROM_S3_CSV_ALT": {
            "CLOUD": {
                "s3": {"bucket": "mock-bucket", "file_path": "mock-key", "file_type": "csv"},
                "type": "s3",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/some_csv_to_read.csv",
                    "file_type": "csv",
                },
                "type": "local",
            },
        },
        "READ_FROM_S3_HDF": {
            "CLOUD": {
                "s3": {"bucket": "mock-bucket", "file_path": "mock-key", "file_type": "hdf"},
                "type": "s3",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/some_hdf_to_read.h5",
                    "file_type": "hdf",
                },
                "type": "local",
            },
        },
        "READ_FROM_S3_JSON": {
            "CLOUD": {
                "s3": {"bucket": "mock-bucket", "file_path": "mock-key", "file_type": "json"},
                "type": "s3",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/some_json_to_read.json",
                    "file_type": "json",
                },
                "type": "local",
            },
        },
        "READ_FROM_S3_PARQUET": {
            "CLOUD": {
                "s3": {
                    "bucket": "mock-bucket",
                    "file_path": "s3:sample-prefix/mock-key",
                    "file_type": "parquet",
                },
                "type": "s3",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/some_parquet_to_read.parquet",
                    "file_type": "parquet",
                },
                "type": "local",
            },
        },
        "TEMPLATED_FILE_PATH": {
            "CLOUD": {
                "s3": {
                    "bucket": "mock-bucket",
                    "file_path": "path/to/{file_name_to_replace}.csv",
                    "file_type": "csv",
                },
                "type": "s3",
            },
            "LOCAL": {
                "local": {
                    "file_path": f"{constants.TEST_RESOURCES}/data/input/{{file_name_to_replace}}.csv",
                    "file_type": "csv",
                },
                "type": "local",
            },
        },
    }


@pytest.fixture(scope="class")
def expected_input_sources():
    return [
        "READ_FROM_S3_CSV_ALT",
        "READ_FROM_S3_CSV",
        "READ_FROM_S3_JSON",
        "READ_FROM_S3_HDF",
        "READ_FROM_S3_PARQUET",
        "READ_FROM_POSTGRES",
        "READ_FROM_KAFKA",
        "TEMPLATED_FILE_PATH",
        "READ_FROM_PARQUET_TEMPLATED",
    ]


@pytest.fixture(scope="class")
def expected_s3_csv_local_mapping():
    return {
        "local": {
            "file_path": f"{constants.TEST_RESOURCES}/data/input/some_csv_to_read.csv",
            "file_type": "csv",
        },
        "metrics": {
            "id": ["UniqueCounts", "Counts"],
            "bar": ["Min", "Max", "Mean", "Std", "Variance"],
            "foo_name": ["CountsPerLabel"],
        },
        "schema": {"id": "int64", "bar": "int64", "foo_name": "object"},
        "type": "local",
        "validations": {
            "id": {
                "has_no_null_values": {"apply": True, "options": {}},
                "has_unique_values": {"apply": True, "options": {}},
            },
            "bar": {
                "has_no_null_values": {"apply": True, "options": {}},
                "is_greater_than": {"apply": True, "options": {"threshold": 1000}},
                "is_lower_than": {"apply": True, "options": {"threshold": 2000}},
            },
            "foo_name": {
                "has_acceptable_categorical_values": {
                    "apply": True,
                    "options": {"categorical_values": ["class_a", "class_b", "class_c"]},
                },
                "has_no_null_values": {"apply": True, "options": {}},
            },
        },
    }


@pytest.fixture(scope="class")
def expected_s3_csv_cloud_mapping():
    return {
        "metrics": {
            "id": ["UniqueCounts", "Counts"],
            "bar": ["Min", "Max", "Mean", "Std", "Variance"],
            "foo_name": ["CountsPerLabel"],
        },
        "s3": {"bucket": "mock-bucket", "file_path": "mock-key", "file_type": "csv"},
        "schema": {"id": "int64", "bar": "int64", "foo_name": "object"},
        "type": "s3",
        "validations": {
            "id": {
                "has_no_null_values": {"apply": True, "options": {}},
                "has_unique_values": {"apply": True, "options": {}},
            },
            "bar": {
                "has_no_null_values": {"apply": True, "options": {}},
                "is_greater_than": {"apply": True, "options": {"threshold": 1000}},
                "is_lower_than": {"apply": True, "options": {"threshold": 2000}},
            },
            "foo_name": {
                "has_acceptable_categorical_values": {
                    "apply": True,
                    "options": {"categorical_values": ["class_a", "class_b", "class_c"]},
                },
                "has_no_null_values": {"apply": True, "options": {}},
            },
        },
    }


@pytest.fixture(scope="class")
def expected_postgres_cloud_mapping():
    return {
        "postgres": {
            "db_host": "127.0.0.1",
            "db_name": "backend",
            "db_password": "pass",
            "db_port": "17039",
            "db_user": "user",
        },
        "type": "postgres",
    }


@pytest.fixture(scope="class")
def expected_s3_parquet_df():
    return pd.read_parquet(f"{constants.TEST_RESOURCES}/data/input/some_parquet_to_read.parquet")


@pytest.fixture(scope="class")
def expected_s3_hdf_df():
    return pd.read_hdf(f"{constants.TEST_RESOURCES}/data/input/some_hdf_to_read.h5")


@pytest.fixture(scope="class")
def expected_s3_json_df():
    return pd.read_json(f"{constants.TEST_RESOURCES}/data/input/some_json_to_read.json", orient="columns")


@pytest.fixture(scope="class")
def expected_s3_csv_df():
    return pd.read_csv(f"{constants.TEST_RESOURCES}/data/input/some_csv_to_read.csv")


@pytest.fixture(scope="class")
def expected_df_with_less_columns():
    df = pd.DataFrame.from_records(
        [
            [1, "name_a"],
            [2, "name_b"],
            [3, "name_a"],
            [4, "name_b"],
            [5, "name_a"],
            [6, "name_b"],
            [7, "name_a"],
            [8, "name_b"],
            [9, "name_a"],
            [10, "name_b"],
            [11, "name_a"],
            [12, "name_b"],
            [13, "name_a"],
            [14, "name_b"],
            [15, "name_a"],
        ],
        columns=["id", "foo_name"],
    )
    return df


@pytest.fixture(scope="class")
def dataset_with_more_columns_than_dictated_in_schema():
    df = pd.DataFrame.from_records(
        [
            [1, "foo_a", 1, 1500, 1600, "pass_through"],
            [2, "foo_b", 2, 1500, 1600, "pass_through"],
            [3, "foo_a", 3, 1500, 1600, "pass_through"],
            [4, "foo_b", 4, 1500, 1600, "pass_through"],
            [5, "foo_a", 5, 1500, 1600, "pass_through"],
            [6, "foo_b", 6, 1500, 1600, "pass_through"],
            [7, "foo_a", 7, 1500, 1600, "pass_through"],
            [8, "foo_b", 8, 1500, 1600, "pass_through"],
            [9, "foo_a", 9, 1500, 1600, "pass_through"],
            [10, "foo_b", 10, 1500, 1600, "pass_through"],
            [11, "foo_a", 11, 1500, 1600, "pass_through"],
            [12, "foo_b", 12, 1500, 1600, "pass_through"],
            [13, "foo_a", 13, 1500, 1600, "pass_through"],
            [14, "foo_b", 14, 1500, 1600, "pass_through"],
            [15, "foo_a", 15, 1500, 1600, "pass_through"],
        ],
        columns=["id", "foo_name", "bar", "start_odometer", "end_odometer", "event_type"],
    )
    return df


@pytest.fixture(scope="class")
def test_df():
    df = pd.DataFrame.from_records(
        [
            ["cm_1", "id_1", 1000, "ABC"],
            ["cm_2", "id_2", 1000, "ABC"],
            ["cm_3", "id_3", 1000, "ABC"],
        ],
        columns=["id", "foo", "bar", "baz"],
    )
    return df


@pytest.fixture(scope="class")
def expected_columns():
    return [ERModel.id, ERModel.foo, ERModel.bar, ERModel.baz]


@pytest.fixture(scope="class")
def expected_kwargs_for_read_parquet():
    return {"engine", "columns", "kwargs", "path", "use_nullable_dtypes"}


@pytest.fixture(scope="class")
def expected_value_serializer():
    return {'value_serializer': 'WithKafka._default_value_serializer'}


@pytest.fixture(scope="class")
def input_messages_df():
    return pd.DataFrame.from_dict(
        [
            {"id": "message01", "foo": "xxxxxxxx", "bar": 0, "baz": ["a", "b", "c"]},
            {"id": "message02", "foo": "yyyyyyyy", "bar": 1, "baz": ["d", "e", "f"]},
        ]
    )


@pytest.fixture(scope="class")
def input_schema_definition():
    return {
        "columns": {
            "id": {
                "metrics": ["UniqueCounts", "Counts"],
                "type": "int64",
                "validations": {
                    "has_no_null_values": {"apply": True, "options": {}},
                    "has_unique_values": {"apply": True, "options": {}},
                },
            },
            "bar": {
                "metrics": ["Min", "Max", "Mean", "Std", "Var"],
                "type": "int64",
                "validations": {
                    "has_no_null_values": {"apply": True, "options": {}},
                    "is_greater_than": {"apply": True, "options": {"threshold": 1000}},
                    "is_lower_than": {"apply": True, "options": {"threshold": 2000}},
                },
            },
            "foo_name": {
                "metrics": None,
                "type": "object",
                "validations": {
                    "has_acceptable_categorical_values": {
                        "apply": True,
                        "options": {"categorical_values": ["class_a", "class_b", "class_c"]},
                    },
                    "has_no_null_values": {"apply": True, "options": {}},
                },
            },
        },
        "name": "read_from_s3_csv",
    }


@pytest.fixture(scope="class")
def expected_schema():
    return {"id": "int64", "foo_name": "object", "bar": "int64"}


@pytest.fixture(scope="class")
def expected_schema_definition():
    return {
        "columns": {
            "id": {
                "metrics": ["UniqueCounts", "Counts"],
                "type": "int64",
                "validations": {
                    "has_no_null_values": {"apply": True, "options": {}},
                    "has_unique_values": {"apply": True, "options": {}},
                },
            },
            "bar": {
                "metrics": ["Min", "Max", "Mean", "Std", "Variance"],
                "type": "int64",
                "validations": {
                    "has_no_null_values": {"apply": True, "options": {}},
                    "is_greater_than": {"apply": True, "options": {"threshold": 1000}},
                    "is_lower_than": {"apply": True, "options": {"threshold": 2000}},
                },
            },
            "foo_name": {
                "metrics": ["CountsPerLabel"],
                "type": "object",
                "validations": {
                    "has_acceptable_categorical_values": {
                        "apply": True,
                        "options": {"categorical_values": ["class_a", "class_b", "class_c"]},
                    },
                    "has_no_null_values": {"apply": True, "options": {}},
                },
            },
        },
        "name": "read_from_s3_csv",
    }


@pytest.fixture(scope="class")
def expected_validations():
    return {
        "id": {
            "has_no_null_values": {"apply": True, "options": {}},
            "has_unique_values": {"apply": True, "options": {}},
        },
        "bar": {
            "has_no_null_values": {"apply": True, "options": {}},
            "is_greater_than": {"apply": True, "options": {"threshold": 1000}},
            "is_lower_than": {"apply": True, "options": {"threshold": 2000}},
        },
        "foo_name": {
            "has_acceptable_categorical_values": {
                "apply": True,
                "options": {"categorical_values": ["class_a", "class_b", "class_c"]},
            },
            "has_no_null_values": {"apply": True, "options": {}},
        },
    }


@pytest.fixture(scope="class")
def expected_metrics():
    return {
        "id": ["UniqueCounts", "Counts"],
        "bar": ["Min", "Max", "Mean", "Std", "Var"],
        "foo_name": None,
    }


@pytest.fixture(scope="class")
def valid_dataframe():
    return pd.DataFrame.from_dict(
        {
            "id": [3, 2, 1, 0],
            "foo_name": ["class_a", "class_b", "class_c", "class_a"],
            "bar": [1500, 1500, 1500, 1500],
        }
    )


@pytest.fixture(scope="class")
def invalid_dataframe():
    return pd.DataFrame.from_dict(
        {
            "id": [3, 2, 0, 0],
            "foo_name": ["class_a", "class_b", "class_d", "class_a"],
            "bar": [999, 1500, 2500, 1500],
        }
    )


@pytest.fixture(scope="class")
def expected_messages():
    return {
        "has_unique_values",
        "has_acceptable_categorical_values",
        "is_greater_than",
        "is_lower_than",
    }


@pytest.fixture(scope="class")
def input_df():
    return pd.DataFrame.from_records(
        [
            ["event_0", "A", "A", "discharge", 10.01234, pd.NA, pd.Timestamp("2021-03-30"), 100.01234, 5, 5, ],
            ["event_1", "B", "B", "pass_through", 10.01234, None, pd.Timestamp("2021-03-30"), 100.01234, 6, 6, ],
            ["event_2", "A", "A", "load", None, None, pd.NaT, pd.NA, 7, 7],
            ["event_3", "B", "B", "pass_through", 10.01234, 10.01234, pd.Timestamp("2021-03-30"), 100.01234, 8, 8, ],
            ["event_4", "C", pd.NA, "load", 10.01234, 10.01234, pd.Timestamp("2021-03-30"), 100.01234, 9, 9, ],
            ["event_5", "A", "A", "pass_through", 10.01234, 10.01234, pd.Timestamp("2021-03-30"), 100.01234, 8, 8, ],
            ["event_6", "C", "C", "discharge", 10.01234, 10.01234, pd.Timestamp("2021-03-30"), 100.01234, 7, 7, ],
            ["event_7", "A", None, "discharge", 10.01234, 10.01234, pd.Timestamp("2021-03-30"), 100.01234, 6, 6, ],
            ["event_8", None, np.nan, "discharge", 10.01234, 10.01234, pd.Timestamp("2021-03-30"), 100.01234, 5, 5, ],
            ["event_9", "A", "A", "discharge", 10.01234, 10.01234, pd.Timestamp("2021-03-30"), 100.01234, 5, None, ],
        ],
        columns=["id", "category_a", "category_b", "activity", "duration_a", "duration_b", "start_time", "load", "weight_a", "weight_b", ],
    )


@pytest.fixture(scope="class")
def empty_df():
    return pd.DataFrame.from_records(
        [],
        columns=["id", "category_a", "category_b", "activity", "duration_a", "duration_b", "start_time", "load", "weight_a", "weight_b", ],
    )


# Mocks
s3_obj_file_names = ["s3://path/to/obj_1.h5", "s3://path/to/obj_2.h5", "s3://path/to/obj_3.h5"]
invalid_s3_obj_file_names = ["s3://path/to/.gitkeep", "s3://path/to/obj_2.h5", "s3://path/to/obj_3.h5"]
local_obj_file_names = ["obj_1.h5", "obj_2.h5", "obj_3.h5"]
invalid_local_obj_file_names = ["obj_2.h5", "obj_3.h5"]


@pytest.fixture
def mock__read_hdf_file():
    def return_mock_df(path, _schema, **_options):
        path_id_map = {"temp/" + f: i + 1 for i, f in enumerate(local_obj_file_names)}

        return pd.DataFrame({"id": [path_id_map[path]], "foo_name": ["class_a"], "bar": [1001]})

    with patch.object(WithS3PathPrefix, "_read_hdf_file", side_effect=return_mock_df) as mock:
        yield mock


@pytest.fixture
def mock__read_parquet_file():
    def return_mock_df(path, _schema, **_options):
        path_id_map = {"temp/" + f: i + 1 for i, f in enumerate(local_obj_file_names)}

        return pd.DataFrame({"id": [path_id_map[path]], "foo_name": ["class_a"], "bar": [1001]})

    with patch.object(WithS3PathPrefix, "_read_parquet_file", side_effect=return_mock_df) as mock:
        yield mock


@pytest.fixture
def mock__read_csv_file():
    def return_mock_df(path, _schema, **_options):
        path_id_map = {"temp/" + f: i + 1 for i, f in enumerate(local_obj_file_names)}

        return pd.DataFrame({"id": [path_id_map[path]], "foo_name": ["class_a"], "bar": [1001]})

    with patch.object(WithS3PathPrefix, "_read_csv_file", side_effect=return_mock_df) as mock:
        yield mock


@pytest.fixture
def mock__read_json_file():
    def return_mock_df(path, _schema, **_options):
        path_id_map = {"temp/" + f: i + 1 for i, f in enumerate(local_obj_file_names)}

        return pd.DataFrame({"id": [path_id_map[path]], "foo_name": ["class_a"], "bar": [1001]})

    with patch.object(WithS3PathPrefix, "_read_json_file", side_effect=return_mock_df) as mock:
        yield mock


@pytest.fixture
# pylint: disable=invalid-name
def mock_temporary_directory():
    with patch.object(tempfile, "TemporaryDirectory") as mock:
        mock.return_value.__enter__.return_value = "temp"
        yield mock


@pytest.fixture
def mock_listdir():
    with patch.object(os, "listdir", return_value=local_obj_file_names) as mock:
        yield mock


@pytest.fixture
def mock_invalid_listdir():
    with patch.object(os, "listdir", return_value=invalid_local_obj_file_names) as mock:
        yield mock


@pytest.fixture
# pylint: disable=invalid-name
def mock_parquet_temporary_directory():
    with patch.object(tempfile, "TemporaryDirectory") as mock:
        mock.return_value.__enter__.return_value = os.path.join(constants.TEST_RESOURCES, "data/input/batch/parquet")
        yield mock


@pytest.fixture
# pylint: disable=invalid-name
def mock_parquet_temporary_directory_w_empty_files():
    with patch.object(tempfile, "TemporaryDirectory") as mock:
        mock.return_value.__enter__.return_value = os.path.join(constants.TEST_RESOURCES, "data/input/batch/parquet_w_empty_files")
        yield mock
