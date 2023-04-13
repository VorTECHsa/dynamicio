"""Custom validators for the dynamicio, to be used with pandera schemas."""
from typing import List

import pandas as pd
from pandera import extensions

# TODO: add these on package installation


@extensions.register_check_method(statistics=["threshold"], check_type="vectorized", supported_types=pd.Series)
def has_acceptable_percentage_of_nulls(series, *, threshold: float):
    """Check that the percentage of nulls in a column is below a threshold."""
    return series.isna().sum() < threshold * len(series)


@extensions.register_check_method(
    statistics=["values", "ignore_na"], check_type="vectorized", supported_types=pd.Series
)
def has_all_values(series, *, values: List, ignore_na: bool = False):
    """Check that all values in a column are in the list of values and that there are no other values.

    v4: is_in (WITHOUT match_all) << this seems to have been the wrong way round in v4.
    """
    if ignore_na:
        _series = series.dropna()
    else:
        _series = series
    return set(_series.unique()) == set(values)


# pandera isin: v4 equivalent: is_in + match_all
# difference: ignore_na was practically always True in v4
