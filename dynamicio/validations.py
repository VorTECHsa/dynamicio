"""Implements the Validator class responsible for various generic data validations and metrics generation."""
__all__ = [
    "has_unique_values",
    "has_no_null_values",
    "has_acceptable_percentage_of_nulls",
    "has_acceptable_categorical_values",
    "is_greater_than",
    "is_greater_than_or_equal",
    "is_lower_than",
    "is_lower_than_or_equal",
    "is_between",
]

import operator
from typing import NamedTuple, Set

import pandas as pd  # type: ignore


class ValidationResult(NamedTuple):
    """A NamedTuple for capturing different outputs after a validation."""

    valid: bool
    message: str
    value: float


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


def has_acceptable_categorical_values(dataset: str, df: pd.DataFrame, column: str, categorical_values: Set[str]) -> ValidationResult:
    """Checks if the column only has allowed categorical values as per the set provided.

    Note:
        Ignores nulls

    Args:
        dataset: Name fo the dataset_name
        df: A DataFrame
        column: The DataFrame column to be validated
        categorical_values: The allowed set of categorical values

    Returns:
        An instance of ValidationResult where `Validation.Result.valid` is a bool indicate the success of the validation,
        `Validation.Result.message` is a message (usually used in exceptions), and `Validation.Result.value` is no_of_not_acceptable
    """
    unique_values = set(df[column][df[column].notna()].unique())
    if unique_values.issubset(categorical_values):
        return ValidationResult(valid=True, message=f"Categorical values for {dataset}[{column}] are acceptable", value=0)

    count_invalid = (~df[column].isin(categorical_values)).sum()
    return ValidationResult(
        valid=False,
        message=f"Values {unique_values - set(categorical_values)} for {dataset}[{column}] are not acceptable for {count_invalid} cells",
        value=count_invalid,
    )


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
