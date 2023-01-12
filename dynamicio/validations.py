"""Implements the Validator class responsible for various generic data validations and metrics generation."""
import operator
from typing import Callable, NamedTuple, Set

import pandas as pd  # type: ignore

ALL_VALIDATORS = {}  # name -> function


def validator(func: Callable):
    """A decorator to add the function to the ALL_VALIDATORS dict"""
    name = func.__name__
    assert name not in ALL_VALIDATORS
    ALL_VALIDATORS[name] = func
    return func


class ValidationResult(NamedTuple):
    """A NamedTuple for capturing different outputs after a validation."""

    valid: bool
    message: str
    value: float


@validator
def has_unique_values(dataset: str, df: pd.DataFrame, column: str) -> ValidationResult:
    """Checks if values in column are unique.

    Args:
        dataset: Name fo the dataset_name
        df: A pandas DataFrame
        column: The column to be validated

    Returns:
        An instance of  ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and  `Validation.Result.value` is no_of_duplicated_elements
    """
    counts = df[column].value_counts()
    if not (counts > 1).any():
        return ValidationResult(valid=True, message=f"{dataset}[{column}] has unique values", value=0)

    duplicates = counts[counts > 1].index.to_list()
    return ValidationResult(valid=False, message=f"Values {duplicates} for {dataset}[{column}] are duplicated!", value=len(duplicates))


@validator
def has_no_null_values(dataset: str, df: pd.DataFrame, column: str) -> ValidationResult:
    """Checks if column has any null values (including NaN and NaT values).

    Args:
        dataset: Name fo the dataset_name
        df: A pandas DataFrame
        column: The column to be validated

    Returns:
        An instance of  ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and  `Validation.Result.value` is no_of_nulls
    """
    mask = df[column].isnull()
    no_of_nulls = mask.sum()
    return ValidationResult(valid=not mask.any(), message=f"{dataset}[{column}] has {no_of_nulls} nulls", value=no_of_nulls)


@validator
def has_acceptable_percentage_of_nulls(
    dataset: str,
    df: pd.DataFrame,
    column: str,
    threshold: float,
) -> ValidationResult:
    """Checks if a provided threshold of max nulls has been exceeded.

    Note: For an empty df the validation will always be successful

    Args:
        dataset: Name fo the dataset_name
        df: A pandas DataFrame
        column: The column to be validated
        threshold: Maximum allowed threshold

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and  `Validation.Result.value` is percentage_of_nulls
    """
    if threshold <= 0 or threshold >= 1:
        raise ValueError(f"Threshold value: {threshold} must be a value between 0 and 1.")

    no_of_nulls = df[column].isnull().sum()
    if len(df) == 0:
        percentage_of_nulls = 0
    else:
        percentage_of_nulls = no_of_nulls / len(df)

    if percentage_of_nulls < threshold:
        return ValidationResult(
            valid=True,
            message=f"Percentage of nulls of for {dataset}[{column}] is {percentage_of_nulls}",
            value=percentage_of_nulls,
        )
    return ValidationResult(
        valid=False,
        message=f"Percentage of nulls of for {dataset}[{column}] is {percentage_of_nulls} which exceeds threshold: {threshold}",
        value=percentage_of_nulls,
    )


@validator
def is_in(dataset: str, df: pd.DataFrame, column: str, categorical_values: Set[str], match_all: bool = True) -> ValidationResult:
    """Checks if the column only has allowed categorical values as per the set provided.

    Note:
        Ignores nulls

    Args:
        dataset: Name fo the dataset_name
        df: A DataFrame
        column: The DataFrame column to be validated
        categorical_values: The allowed set of categorical values
        match_all: If True, the categorical values must be a subset of the allowed set, otherwise they must be equal

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and `Validation.Result.value` is no_of_not_acceptable
    """
    unique_values = set(df[column][df[column].notna()].unique())

    if match_all:
        return _validate_categoricals_are_a_subset_of_the_acceptable(categorical_values, unique_values, column, dataset, df)
    return _validate_all_acceptable_categoricals_are_present(categorical_values, unique_values, column, dataset, df)


@validator
def _validate_all_acceptable_categoricals_are_present(acceptable_categoricals: Set[str], unique_values: Set[str], column: str, dataset: str, df: pd.DataFrame) -> ValidationResult:
    if unique_values == acceptable_categoricals:
        validation_result = ValidationResult(valid=True, message=f"All acceptable categorical values for {dataset}[{column}] are present", value=0)
    elif unique_values < acceptable_categoricals:
        validation_result = ValidationResult(
            valid=False,
            message=f"Missing categorical values for {dataset}[{column}]: {acceptable_categoricals - unique_values}",
            value=len(acceptable_categoricals - unique_values),
        )
    else:
        count_invalid = (~df[column].isin(acceptable_categoricals)).sum()
        validation_result = ValidationResult(
            valid=False,
            message=f"Values {unique_values - set(acceptable_categoricals)} for {dataset}[{column}] are not acceptable for {count_invalid} cells",
            value=count_invalid,
        )
    return validation_result


@validator
def _validate_categoricals_are_a_subset_of_the_acceptable(acceptable_categoricals: Set[str], unique_values: Set[str], column: str, dataset: str, df: pd.DataFrame) -> ValidationResult:
    if unique_values.issubset(acceptable_categoricals):
        return ValidationResult(valid=True, message=f"Categorical values for {dataset}[{column}] are acceptable", value=0)
    count_invalid = (~df[column].isin(acceptable_categoricals)).sum()
    return ValidationResult(
        valid=False,
        message=f"Values {unique_values - set(acceptable_categoricals)} for {dataset}[{column}] are not acceptable for {count_invalid} cells",
        value=count_invalid,
    )


@validator
def is_greater_than(
    dataset: str,
    df: pd.DataFrame,
    column: str,
    threshold: float,
) -> ValidationResult:
    """Confirms column values are above a given threshold.

    Args:
        dataset: Name fo the dataset_name
        df: A DataFrame
        column: The DataFrame column to be validated
        threshold: A lower bound threshold not to be exceeded

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and `Validation.Result.value` is the
        percentage of invalid values
    """
    no_nulls_for_column_df = df[~df[column].isnull()][column]
    valid = no_nulls_for_column_df > threshold

    if valid.all():
        return ValidationResult(valid=True, message=f"All values of {dataset}[{column}] are above {threshold}", value=0)

    no_of_invalid = (~valid).sum()
    return ValidationResult(
        valid=False,
        message=f"{no_of_invalid} cell values for {dataset}[{column}] are below {threshold}",
        value=no_of_invalid / len(no_nulls_for_column_df),
    )


@validator
def is_greater_than_or_equal(
    dataset: str,
    df: pd.DataFrame,
    column: str,
    threshold: float,
) -> ValidationResult:
    """Confirms column values are above a given threshold.

    Args:
        dataset: Name fo the dataset_name
        df: A DataFrame
        column: The DataFrame column to be validated
        threshold: A lower bound threshold not to be exceeded

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and `Validation.Result.value` is the
        percentage of invalid values
    """
    no_nulls_for_column_df = df[~df[column].isnull()][column]
    valid = no_nulls_for_column_df >= threshold

    if valid.all():
        return ValidationResult(valid=True, message=f"All values of {dataset}[{column}] are above {threshold}", value=0)

    no_of_invalid = (~valid).sum()
    return ValidationResult(
        valid=False,
        message=f"{no_of_invalid} cell values for {dataset}[{column}] are below {threshold}",
        value=no_of_invalid / len(no_nulls_for_column_df),
    )


@validator
def is_lower_than(
    dataset: str,
    df: pd.DataFrame,
    column: str,
    threshold: float,
) -> ValidationResult:
    """Confirms column values are below a given threshold.

    IMPORTANT NOTE: Ignores nulls!

    Args:
        dataset: Name fo the dataset_name
        df: A DataFrame
        column: The DataFrame column to be validated
        threshold: A lower bound threshold not to be exceeded

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and `Validation.Result.value` is the percentage of
        invalid values
    """
    no_nulls_for_column_df = df[~df[column].isnull()][column]
    valid = no_nulls_for_column_df < threshold  # pd.DataFrame

    if valid.all():
        return ValidationResult(valid=True, message=f"All values of {dataset}[{column}] are below {threshold}", value=0)

    no_of_invalid = (~valid).sum()
    return ValidationResult(
        valid=False,
        message=f"{no_of_invalid} cell values for {dataset}[{column}] are above {threshold}",
        value=no_of_invalid / len(no_nulls_for_column_df),
    )


@validator
def is_lower_than_or_equal(
    dataset: str,
    df: pd.DataFrame,
    column: str,
    threshold: float,
) -> ValidationResult:
    """Confirms column values are below a given threshold.

    IMPORTANT NOTE: Ignores nulls!

    Args:
        dataset: Name fo the dataset_name
        df: A DataFrame
        column: The DataFrame column to be validated
        threshold: A lower bound threshold not to be exceeded

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and `Validation.Result.value` is the percentage of
        invalid values
    """
    no_nulls_for_column_df = df[~df[column].isnull()][column]
    valid = no_nulls_for_column_df <= threshold

    if valid.all():
        return ValidationResult(valid=True, message=f"All values of {dataset}[{column}] are below {threshold}", value=0)

    no_of_invalid = (~valid).sum()
    return ValidationResult(
        valid=False,
        message=f"{no_of_invalid} cell values for {dataset}[{column}] are above {threshold}",
        value=no_of_invalid / len(no_nulls_for_column_df),
    )


@validator
def is_between(
    dataset: str,
    df: pd.DataFrame,
    column: str,
    lower: float,
    upper: float,
    include_left: bool = False,
    include_right: bool = False,
) -> ValidationResult:
    """Confirms column values are between a lower bound and an upper bound thresholds.

    IMPORTANT NOTE: Ignores nulls!

    Args:
        dataset: Name fo the dataset_name
        df: A DataFrame
        column: The DataFrame column to be validated
        lower: The lower bound (left)
        upper: The upper bound (right)
        include_left: `left <= df[column]`
        include_right: `df[column] <=right`

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and `Validation.Result.value` is the percentage of
        invalid values
    """
    no_nulls_for_column_df = df[~df[column].isnull()][column]
    lower_bound_operator = operator.ge if include_left else operator.gt
    upper_bound_operator = operator.le if include_right else operator.lt

    valid = lower_bound_operator(no_nulls_for_column_df, lower) & upper_bound_operator(no_nulls_for_column_df, upper)

    if valid.all():
        return ValidationResult(valid=True, message=f"All values of {dataset}[{column}] is between {lower} and {upper} thresholds", value=0)

    no_of_invalid = (~valid).sum()
    return ValidationResult(
        valid=False,
        message=f"{no_of_invalid} cell values for {dataset}[{column}] are either below {lower} or above {upper}",
        value=no_of_invalid / len(no_nulls_for_column_df),
    )
