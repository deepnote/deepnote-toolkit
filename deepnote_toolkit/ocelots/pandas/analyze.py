import math
from collections import Counter
from typing import List, Optional

import numpy as np
import pandas as pd

from deepnote_toolkit.ocelots.constants import DEEPNOTE_INDEX_COLUMN
from deepnote_toolkit.ocelots.pandas.utils import (
    is_numeric_or_temporal,
    is_type_datetime_or_timedelta,
    safe_convert_to_string,
)
from deepnote_toolkit.ocelots.types import ColumnsStatsRecord, ColumnStats


def _count_unique(column):
    try:
        return column.dropna().nunique()
    except TypeError:
        # This happens when the column contains e.g. dictionaries, lists or sets
        # In that case, we fall back to each value being unique
        return len(column)


def _get_categories(np_array):
    pandas_series = pd.Series(np_array)

    # special treatment for empty values
    num_nans = pandas_series.isna().sum().item()

    try:
        counter = Counter(pandas_series.dropna().astype(str))
    except (TypeError, UnicodeDecodeError, AttributeError):
        counter = Counter(pandas_series.dropna().apply(safe_convert_to_string))

    max_items = 3
    if num_nans > 0:
        max_items -= 1  # We need to save space for "missing" category

    if len(counter) > max_items:
        most_common = counter.most_common(max_items - 1)
        counter -= dict(most_common)
        sum_others = sum(counter.values())
        num_others = len(counter)
        most_common.append((f"{num_others} others", sum_others))
        categories = most_common
    else:
        categories = counter.most_common(max_items)

    if num_nans > 0:
        categories.append(("Missing", num_nans))

    return [{"name": name, "count": count} for name, count in categories]


def _get_histogram(pd_series):
    try:
        if is_type_datetime_or_timedelta(pd_series):
            np_array = np.array(pd_series.dropna().astype(int))
        else:
            # let's drop infinite values because they break histograms
            np_array = np.array(pd_series.replace([np.inf, -np.inf], np.nan).dropna())

        # Check if array is empty after dropping NaN/NaT values
        if len(np_array) == 0:
            return None

        y, bins = np.histogram(np_array, bins=10)
        return [
            {"bin_start": bins[i], "bin_end": bins[i + 1], "count": count.item()}
            for i, count in enumerate(y)
        ]
    except (ValueError, IndexError) as e:
        # NumPy 2.2+ raises "Too many bins for data range" when:
        # - Data range is zero (all values identical), or
        # - For integer data, bin width would be < 1.0, or
        # - Floating point precision prevents creating finite-sized bins at large scales
        # Numpy implementation: https://github.com/numpy/numpy/blob/e7a123b2d3eca9897843791dd698c1803d9a39c2/numpy/lib/_histograms_impl.py#L454
        # IndexError can occur in NumPy 2.x with edge cases involving large integers or datetime conversions
        if isinstance(e, ValueError) and "Too many bins for data range" in str(e):
            return None
        # For IndexError or other ValueError cases, return None to gracefully handle edge cases
        return None


def _calculate_min_max(column):
    """
    Calculate min and max values for a given column.
    """
    if not is_numeric_or_temporal(column.dtype):
        return None, None

    try:
        min_value = str(min(column.dropna())) if len(column.dropna()) > 0 else None
        max_value = str(max(column.dropna())) if len(column.dropna()) > 0 else None
        return min_value, max_value
    except (TypeError, ValueError):
        return None, None


def analyze_columns(
    df: pd.DataFrame, color_scale_column_names: Optional[List[str]] = None
) -> List[ColumnsStatsRecord]:
    """
    Analyze columns in a Pandas DataFrame, but only within a certain computational limit.

    This function is used to analyze the columns of a Pandas DataFrame for display in a
    Deepnote data table. It only analyzes a certain number of columns to keep things fast.
    The number of columns it analyzes is determined by the `max_cells_to_analyze` variable,
    which is calculated so that the analysis takes no more than 100ms.

    If the user has applied color scale format rules in a data table, this function will
    calculate additional statistics for the columns that are required for display of the
    color scales.

    Args:
        df: A Pandas DataFrame to analyze.
        color_scale_column_names: A set of column names that have a color scale formatting rule applied.

    Returns:
        A list of ColumnsStatsRecord
    """

    # Analyze only certain number of columns to keep things fast
    max_cells_to_analyze = (
        100000  # calculated so that the analysis takes no more than 100ms
    )
    if len(df) == 0:
        max_columns_to_analyze = len(df.columns)
    else:
        max_columns_to_analyze = min(
            math.floor(max_cells_to_analyze / len(df)),
            len(df.columns),
        )

    # Analyze columns
    columns = [
        ColumnsStatsRecord(
            name=str(name),
            dtype=str(dtype),
        )
        for name, dtype in zip(df.columns, df.dtypes)
    ]

    # Add stats to columns, but only within computational limit
    for i in range(max_columns_to_analyze):
        column = df.iloc[
            :, i
        ]  # We need to use iloc because it works if column names have duplicates
        if columns[i].name == DEEPNOTE_INDEX_COLUMN:
            continue  # Do not analyze DEEPNOTE_INDEX_COLUMN column

        columns[i].stats = ColumnStats(
            unique_count=_count_unique(column), nan_count=column.isnull().sum().item()
        )

        if is_numeric_or_temporal(column.dtype):
            min_value, max_value = _calculate_min_max(column)
            columns[i].stats.min = min_value
            columns[i].stats.max = max_value
            columns[i].stats.histogram = _get_histogram(column)
        else:
            columns[i].stats.categories = _get_categories(np.array(column))

    if not color_scale_column_names:
        return columns

    # Calculate stats for additional columns if user has applied color scale format rules in a data table
    # That’s because showing color scales in columns requires min and max values
    # Use additional cell limit calculated to keep analysis under ~3s
    remaining_cells_to_analyze_for_color_scales = 10000000

    # Process remaining columns for color scale rules
    for i in range(max_columns_to_analyze, len(df.columns)):
        # Ignore columns that are not numeric
        column = df.iloc[:, i]
        if not is_numeric_or_temporal(column.dtype):
            continue

        column_name = columns[i].name

        if column_name in color_scale_column_names:
            # Check if we still have budget to analyze all of the DataFrame rows for this column
            if remaining_cells_to_analyze_for_color_scales <= len(df):
                break  # Exceeded budget, stop processing

            columns[i].stats = ColumnStats(
                unique_count=_count_unique(column),
                nan_count=column.isnull().sum().item(),
            )

            min_value, max_value = _calculate_min_max(column)
            columns[i].stats.min = min_value
            columns[i].stats.max = max_value
            columns[i].stats.histogram = _get_histogram(column)

            remaining_cells_to_analyze_for_color_scales -= len(column)

    return columns
