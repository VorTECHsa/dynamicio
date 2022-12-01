# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import os
from typing import Any

import pytest

from dynamicio.config import IOConfig
from dynamicio.mixins.utils import allow_options, args_of, get_string_template_field_names, resolve_template
from tests import constants
from tests.mocking.io import (
    ReadS3CsvIO,
)


class TestGetStringTemplateFieldNames:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["s", "expected_result"],
        [
            ("", []),
            ("abc", []),
            ("{abc}", ["abc"]),
            ("a{abc}d{def}", ["abc", "def"]),
            ("a{0}b{1}", ["0", "1"]),
            ("{abc:.2f}", ["abc"]),
        ],
    )
    def test_returns_correct_result(self, s, expected_result):
        result = get_string_template_field_names(s)
        assert result == expected_result


class TestResolveTemplate:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["s", "options", "expected_result"],
        [
            ("{abc}d{def}", {"abc": "100", "def": "hello"}, "100dhello"),
            ("{hello}", {"world": "100", "hello": "world"}, "world"),
        ],
    )
    def test_returns_correct_result(self, s, options, expected_result):
        result = resolve_template(s, options)
        assert result == expected_result

    @pytest.mark.unit
    @pytest.mark.parametrize(["s"], [("abc{0}",), ("{1def}def",)])
    def test_raises_value_error_if_s_has_fields_which_are_not_valid_identifiers(self, s):
        with pytest.raises(ValueError):
            resolve_template(s, None)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["s", "options"],
        [("{abc}", {}), ("{abc}", {"def": "something"}), ("{abc}{def}", {"def": "700"})],
    )
    def test_raises_value_error_if_template_field_cannot_be_resolved_to_options(self, s, options):
        with pytest.raises(ValueError):
            resolve_template(s, options)


class TestAllowedOptions:
    @pytest.fixture(autouse=True)
    def _pass_fixtures(self, capsys):
        self.capsys = capsys  # pylint: disable=attribute-defined-outside-init

    @pytest.mark.unit
    def test_args_of_returns_valid_set_of_allowed_kwargs_for_a_given_function(self):
        # Given
        def magic_function(arg_a: str, arg_b: int, arg_c: bool) -> bool:
            print(f"{arg_a}: {arg_b}")
            return arg_c

        func = magic_function

        # When
        options = args_of(func)

        # Then
        assert options == {"arg_a", "arg_b", "arg_c"}

    @pytest.mark.integration
    def test_allow_options_can_use_iterable_returned_from_args_of_to_filter_out_invalid_options(
        self,
    ):
        # Given
        def magic_function(arg_a: str, arg_b: int, arg_c: bool) -> bool:
            print(f"{arg_a}: {arg_b}")
            return arg_c

        func = magic_function

        @allow_options(args_of(func))
        def mock_method(**options: Any):
            return [*options]

        # When
        options = mock_method(arg_a="A", arg_b=1, arg_c=True, invalid_option="I SHOULDN'T BE HERE")

        # Then
        assert options == ["arg_a", "arg_b", "arg_c"]

    @pytest.mark.integration
    def test_allow_options_does_not_filter_out_valid_args_when_they_are_passed_as_args_and_not_as_kwargs(
        self,
    ):
        # Given
        def magic_function(arg_a: str, arg_b: int, arg_c: bool) -> bool:
            return [arg_a, arg_b, arg_c]

        func = magic_function

        @allow_options(args_of(func))
        def mock_method(schema: "str", **options: Any):
            print(schema)
            return magic_function(**options)

        # When
        # options = mock_method(schema="schema", **{"arg_a": "A", "arg_b": 1, "arg_c": True, "invalid_option": "I SHOULDN'T BE HERE"})  # THIS WOULD FAIL!
        options = mock_method(
            "schema",
            **{"arg_a": "A", "arg_b": 1, "arg_c": True, "invalid_option": "I SHOULDN'T BE HERE"},
        )

        # Then
        captured = self.capsys.readouterr()
        assert (captured.out == "schema\n") and (options == ["A", 1, True])

    @pytest.mark.integration  # This is an integration test as it uses `allow_options()` after `args_of()`
    def test_when_reading_locally_or_from_s3_invalid_options_are_ignored(self, expected_s3_csv_df):
        # Given
        invalid_option = "INVALID_OPTION"
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        s3_csv_df = ReadS3CsvIO(source_config=s3_csv_local_config, foo=invalid_option).read()

        # Then
        assert expected_s3_csv_df.equals(s3_csv_df)

    @pytest.mark.integration
    def test_when_reading_locally_or_from_s3_valid_options_are_considered(self, expected_s3_csv_df):
        # Given
        # VALID OPTION: dtype=None
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        s3_csv_df = ReadS3CsvIO(source_config=s3_csv_local_config, dtype=None).read()

        # Then
        assert expected_s3_csv_df.equals(s3_csv_df)
