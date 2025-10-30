import numpy as np
import pandas as pd
from packaging.requirements import Requirement

from deepnote_toolkit.ocelots.constants import MAX_STRING_CELL_LENGTH


# like fillna, but only fills NaT (not a time) values in datetime columns with the specified value
def fill_nat(df, value):
    df_datetime_columns = df.select_dtypes(
        include=["datetime", "datetimetz", "datetime64"]
    )
    df[df_datetime_columns.columns] = df_datetime_columns.fillna(value)


def flatten_column_name(item):
    if isinstance(item, list) or isinstance(item, tuple):
        return " ".join(map(lambda x: str(x or ""), item))
    else:
        return item


def fix_nan_category(df):
    for i in range(len(df.columns)):
        column = df.iloc[
            :, i
        ]  # We need to use iloc because it works if column names have duplicates

        # If the column is categorical, we need to create a category for nan
        if column.dtype.name == "category":
            df.iloc[:, i] = column.cat.add_categories("nan")

    return df


def deduplicate_columns(df):
    """Make sure the column names are unique since they don't have to be"""
    try:
        if Requirement("pandas<1.3").specifier.contains(pd.__version__):
            # pandas < 1.3
            parser = pd.io.parsers.ParserBase(
                {"names": df.columns}
            )  # pylint: disable=no-member
            df.columns = parser._maybe_dedup_names(
                df.columns
            )  # pylint: disable=protected-access
        elif Requirement("pandas>=1.3,<2").specifier.contains(pd.__version__):
            # pandas >= 1.3, < 2
            parser = pd.io.parsers.base_parser.ParserBase(
                {"names": df.columns, "usecols": None}
            )
            df.columns = parser._maybe_dedup_names(
                df.columns
            )  # pylint: disable=protected-access,no-member
        elif Requirement("pandas>=2").specifier.contains(pd.__version__):
            # pandas >=2
            df.columns = pd.io.common.dedup_names(
                df.columns, pd.io.common.is_potential_multi_index(df.columns)
            )
    except (ImportError, AttributeError):
        # Fallback method for all pandas versions in case the internal API changes
        # This ensures we still have a functioning method even if pandas internals change
        column_names = list(df.columns)
        seen = set()
        for i, name in enumerate(column_names):
            original_name = name
            counter = 0
            while name in seen:
                counter += 1
                name = f"{original_name}.{counter}"
            seen.add(name)
            column_names[i] = name
        df.columns = column_names


# Cast dataframe contents to strings and trim them to avoid sending too much data
def cast_objects_to_string(df):
    def to_string_truncated(elem):
        elem_string = str(elem)
        return (
            (elem_string[: MAX_STRING_CELL_LENGTH - 1] + "…")
            if len(elem_string) > MAX_STRING_CELL_LENGTH
            else elem_string
        )

    for column in df:
        if not _is_type_number(df[column].dtype):
            # if the dtype is not a number, we want to convert it to string and truncate
            df[column] = df[column].apply(to_string_truncated)

    return df


def _is_type_number(dtype):
    """
    Returns True if dtype is a number, False otherwise. Datetime and timedelta will return False.

    The primary intent of this is to recognize a value that will converted to a JSON number during serialization.
    """

    if pd.api.types.is_datetime64_any_dtype(dtype) or pd.api.types.is_timedelta64_dtype(
        dtype
    ):
        # np.issubdtype(dtype, np.number) returns True for timedelta, which we don't want
        return False

    try:
        return np.issubdtype(dtype, np.number)
    except TypeError:
        # np.issubdtype crashes on categorical column dtype, and also on others, e.g. geopandas types
        return False
