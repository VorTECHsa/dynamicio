# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302
import argparse
import os
from unittest.mock import patch

import pandas as pd
import pytest

import dynamicio
from dynamicio import cli
from dynamicio.cli import parse_args
from dynamicio.errors import InvalidDatasetTypeError
from tests.conftest import DummyYaml
from tests.constants import TEST_RESOURCES


class TestCli:
    @pytest.mark.unit
    def test_entrypoint(self):
        print()  # Just makes the output more readable in the terminal

        # When
        exit_status = os.system("python -m dynamicio --help")

        # Then
        assert exit_status == 0

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["args_pattern", "expected_args"],
        [
            (
                ["-b", "-p", "path/to/datasets_dir", "-o", "output_dir"],
                argparse.Namespace(batch=True, output="output_dir", path="path/to/datasets_dir", single=False),
            ),
            (
                ["-s", "-p", "path/to/datasets_dir/the_one.parquet", "-o", "output_dir"],
                argparse.Namespace(
                    batch=False,
                    output="output_dir",
                    path="path/to/datasets_dir/the_one.parquet",
                    single=True,
                ),
            ),
        ],
    )
    def test_parser_can_take_one_out_of_two_valid_argument_patters(self, args_pattern, expected_args):
        # When/Then
        assert parse_args(args_pattern) == expected_args

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "args_pattern",
        [
            ["-p", "path/to/datasets_dir", "-o", "output_dir"],
            ["-p", "path/to/datasets_dir/the_one.parquet", "-o", "output_dir"],
        ],
    )
    def test_parse_args_raises_system_exit_if_batch_or_single_flags_not_provided(self, args_pattern):
        # When/Then
        with pytest.raises(SystemExit):
            parse_args(args_pattern)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "args_pattern",
        [
            ["-b"],
            ["-b", "-o", "output_dir"],
            ["-b", "-p", "path/to/datasets_dir"],
            ["-s"],
            ["-s", "-o", "output_dir"],
            ["-s", "-p", "path/to/datasets_dir"],
        ],
    )
    def test_parse_args_raises_system_exit_with_approved_flag_without_path_and_output(self, args_pattern):
        # When/Then
        with pytest.raises(SystemExit):
            parse_args(args_pattern)

    @pytest.mark.unit
    def test_when_single_flag_is_used__generate_schema_for__is_called_once(self):
        with patch.object(cli.argparse.ArgumentParser, "parse_args") as mocked__parse_args, patch.object(cli, "generate_schema_for") as mocked__generate_schema_for, patch.object(
            cli, "open"
        ) as mocked__open, patch.object(cli.yaml, "safe_dump") as mocked__dump:
            # Given
            mocked__parse_args.return_value = argparse.Namespace(batch=False, single=True, path="the_one.parquet", output=".")
            mocked__generate_schema_for.return_value = {"name": "the_one", "columns": {}}
            mocked__open.return_value = DummyYaml(path="path/to/the_one.yaml")
            mocked__dump.return_value = "The-Matrix"
            # When
            dynamicio.cli.run()

        # Then
        assert mocked__generate_schema_for.called_once_with("the_one.parquet", ".")

    @pytest.mark.unit
    def test_when_batch_flag_is_used__generate_schema_for__is_called_multiple_times_as_per_the_no_of_files_under_the_datasets_dir(
        self,
    ):

        with patch.object(cli.argparse.ArgumentParser, "parse_args") as mocked__parse_args:
            with patch.object(cli, "generate_schema_for") as mocked__generate_schema_for:
                with patch.object(cli.glob, "glob") as mocked__glob:
                    with patch.object(cli, "open") as mocked__open:
                        with patch.object(cli.yaml, "safe_dump") as mocked__dump:
                            # Given
                            mocked__parse_args.return_value = argparse.Namespace(batch=True, single=False, path="path/to/datasets_dir", output=".")
                            mocked__generate_schema_for.return_value = {
                                "name": "random",
                                "columns": {},
                            }
                            mocked__glob.return_value = [
                                "path/to/datasets_dir/agent_1.parquet",
                                "path/to/datasets_dir/agent_2.parquet",
                            ]
                            mocked__open.return_value = DummyYaml(path="path/to/the_oracle.yaml")
                            mocked__dump.return_value = "file_content"
                            # When
                            dynamicio.cli.run()

        # Then
        assert mocked__generate_schema_for.call_count == 2

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["dataset", "expected_reader"],
        [
            ("path/to/dataset.parquet", "read_parquet"),
            ("path/to/dataset.json", "read_json"),
            ("path/to/dataset.csv", "read_csv"),
            ("path/to/dataset.h5", "read_hdf"),
        ],
    )
    def test_generate_schema_for__uses_the_appropriate_pandas_reader_to_read_a_file(self, dataset, expected_reader):
        #  When
        with patch.object(cli.pd, expected_reader) as mocked_reader:
            mocked_reader.return_value = pd.DataFrame()
            cli.generate_schema_for(dataset)

        # Then
        mocked_reader.assert_called()

    @pytest.mark.unit
    def test_generate_schema_for__throws_exception_InvalidDatasetTypeError(self):
        # Given
        dataset = "path/to/trinity.txt"

        #  When/Then
        with pytest.raises(InvalidDatasetTypeError):
            cli.generate_schema_for(dataset)

    @pytest.mark.unit
    def test_generate_schema_for__returns_a_json_schema_with_a_name_key_populated_with_the_dataset_name(
        self,
    ):
        # Given
        dataset = "path/to/the_matrix.parquet"

        # When
        with patch.object(cli.pd, "read_parquet") as mocked_reader:
            mocked_reader.return_value = pd.DataFrame.from_dict({"agents": [1, 2, 3], "zioners": [4, 5, 6]})
            json_schema = cli.generate_schema_for(dataset)

        # Then
        assert json_schema["name"] == "the_matrix"

    @pytest.mark.unit
    def test_generate_schema_for__returns_a_json_schema_with_all_columns_in_the_provided_dataset(
        self,
    ):
        # Given
        dataset = "path/to/the_matrix.parquet"

        #  When
        with patch.object(cli.pd, "read_parquet") as mocked_reader:
            mocked_reader.return_value = pd.DataFrame.from_dict({"agents": [1, 2, 3], "zioners": [4, 5, 6]})
            json_schema = cli.generate_schema_for(dataset)

        # Then
        assert list(json_schema["columns"].keys()) == ["agents", "zioners"]

    @pytest.mark.unit
    def test_generate_schema_for__returns_a_json_schema_with_all_columns_in_the_provided_dataset_with_the_correct_data_types(
        self,
    ):
        # Given
        dataset = "path/to/the_matrix.parquet"

        #  When
        with patch.object(cli.pd, "read_parquet") as mocked_reader:
            mocked_reader.return_value = pd.DataFrame.from_dict(
                {
                    "agents": [1, 2, 3],
                    "zioners": ["4", "5", "6"],
                    "red_pill": [True, False, True],
                    "value": [1.0, 2.0, 3.0],
                }
            )
            json_schema = cli.generate_schema_for(dataset)

        # Then
        assert {column["type"] for column in json_schema["columns"].values()} == {
            "bool",
            "object",
            "int64",
            "float64",
        }

    @pytest.mark.unit
    def test_generate_schema_for__returns_a_valid_json_schema_for_a_given_dataset(self):
        # Given
        dataset = "path/to/the_matrix.parquet"

        #  When
        with patch.object(cli.pd, "read_parquet") as mocked_reader:
            mocked_reader.return_value = pd.DataFrame.from_dict(
                {
                    "agents": [1, 2, 3],
                    "zioners": ["4", "5", "6"],
                    "red_pill": [True, False, True],
                    "value": [1.0, 2.0, 3.0],
                }
            )
            json_schema = cli.generate_schema_for(dataset)

        # Then
        assert json_schema == {
            "columns": {
                "agents": {"metrics": [], "type": "int64", "validations": {}},
                "red_pill": {"metrics": [], "type": "bool", "validations": {}},
                "value": {"metrics": [], "type": "float64", "validations": {}},
                "zioners": {"metrics": [], "type": "object", "validations": {}},
            },
            "name": "the_matrix",
        }

    @pytest.mark.unit
    def test_cli_runner_raises_invalid_dataset_type_error_exception_message_when_invoked_with_single_flag_and_invalid_path(
        self,
    ):
        # Given
        dataset = "path/to/trinity.txt"

        #  When/Then
        with patch.object(cli.argparse.ArgumentParser, "parse_args") as mocked__parse_args:
            mocked__parse_args.return_value = argparse.Namespace(
                batch=False,
                single=True,
                path=dataset,
                output=os.path.join(TEST_RESOURCES, "data/temp/"),
            )
            with pytest.raises(InvalidDatasetTypeError):
                cli.run()

    @pytest.mark.unit
    def test_when_single_flag_is_used__the_cli_generates_a_schema_yaml_for_the_provided_dataset(
        self,
    ):
        with patch.object(cli.argparse.ArgumentParser, "parse_args") as mocked__parse_args:
            with patch.object(cli.pd, "read_parquet") as mocked_reader:
                mocked__parse_args.return_value = argparse.Namespace(
                    batch=False,
                    single=True,
                    path="the_one.parquet",
                    output=os.path.join(TEST_RESOURCES, "data/temp/"),
                )
                # Given
                mocked_reader.return_value = pd.DataFrame.from_dict({"agents": [1, 2, 3], "zioners": [4, 5, 6]})

                # When
                dynamicio.cli.run()

        # Then
        output_yaml = os.path.join(TEST_RESOURCES, "data/temp", "the_one.yaml")
        try:
            assert os.path.isfile(output_yaml)
        finally:
            os.remove(output_yaml)

    @pytest.mark.unit
    def test_when_batch_flag_is_used__the_cli_generates_a_schema_yaml_for_each_dataset_in_the_provided_dir(
        self,
    ):
        with patch.object(cli.argparse.ArgumentParser, "parse_args") as mocked__parse_args:
            with patch.object(cli.pd, "read_parquet") as mocked_reader:
                with patch.object(cli.glob, "glob") as mocked__glob:
                    # Given
                    mocked__parse_args.return_value = argparse.Namespace(
                        batch=True,
                        single=False,
                        path="path/to/datasets_dir",
                        output=os.path.join(TEST_RESOURCES, "data/temp/"),
                    )
                    mocked__glob.return_value = [
                        "path/to/datasets_dir/agent_1.parquet",
                        "path/to/datasets_dir/agent_2.parquet",
                    ]
                    mocked_reader.return_value = pd.DataFrame.from_dict({"skills": [1, 2, 3], "levels": [4, 5, 6]})

                    # When
                    dynamicio.cli.run()

        # Then
        output_yaml_1 = os.path.join(TEST_RESOURCES, "data/temp", "agent_1.yaml")
        output_yaml_2 = os.path.join(TEST_RESOURCES, "data/temp", "agent_2.yaml")
        try:
            assert os.path.isfile(output_yaml_1) & os.path.isfile(output_yaml_2)
        finally:
            os.remove(output_yaml_1)
            os.remove(output_yaml_2)

    @pytest.mark.unit
    def test_cli_runner_prints_an_invalid_dataset_type_warning_when_invoked_with_batch_flag_and_a_dir_with_an_invalid_path_but_is_not_interrupted(self, capsys):

        with patch.object(cli.argparse.ArgumentParser, "parse_args") as mocked__parse_args:
            with patch.object(cli.glob, "glob") as mocked__glob:
                with patch.object(cli.pd, "read_parquet") as mocked_reader:
                    with patch.object(cli, "open") as mocked__open:
                        with patch.object(cli.yaml, "safe_dump") as mocked__dump:
                            # Given
                            mocked__parse_args.return_value = argparse.Namespace(
                                batch=True,
                                single=False,
                                path="a/dummy/path/",
                                output=os.path.join(TEST_RESOURCES, "data/temp/"),
                            )
                            mocked__glob.return_value = [
                                "path/to/neo.parquet",
                                "path/to/trinity.txt",
                                "path/to/morpheus.parquet",
                            ]
                            mocked_reader.return_value = pd.DataFrame.from_dict({"column_1": [1, 2, 3], "column_2": [4, 5, 6]})
                            mocked__open.return_value = DummyYaml(path="path/to/the_oracle.yaml")
                            mocked__dump.return_value = "file_content"
                            # When
                            cli.run()
                            captured = capsys.readouterr()

        # Then
        std_out = captured.out.split("\n")
        assert (
            (std_out[0] == "Generating schema for: path/to/neo.parquet")
            and (std_out[1] == "Skipping path/to/trinity.txt! You may want to remove this file from the datasets directory")
            and (std_out[2] == "Generating schema for: path/to/morpheus.parquet")
        )
