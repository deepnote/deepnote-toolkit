from typing import Any, List, Optional

import pandas as pd

import deepnote_toolkit.ocelots as oc


def sanitize_dataframe_for_chart(pd_df: pd.DataFrame):
    sanitized_dataframe = pd_df.copy()

    oc.pandas.utils.deduplicate_columns(sanitized_dataframe)
    _convert_timedelta_columns_to_seconds(sanitized_dataframe)
    _convert_column_names_to_string(sanitized_dataframe)

    return sanitized_dataframe


def _convert_column_names_to_string(pd_df: pd.DataFrame):
    """
    Converts dataframe column names to strings.

    WARNING: This function modifies the DataFrame in-place.
    """
    pd_df.columns = pd_df.columns.astype(str)


def _convert_timedelta_columns_to_seconds(pd_sanitized_df: pd.DataFrame):
    """
    Converts timedelta columns to seconds.

    WARNING: This function modifies the DataFrame in-place.
    """
    timedelta_columns = list(
        filter(
            lambda column: pd.api.types.is_timedelta64_dtype(
                pd_sanitized_df[column].dtype
            ),
            pd_sanitized_df.columns,
        )
    )
    pd_sanitized_df[timedelta_columns] = pd_sanitized_df[timedelta_columns].apply(
        lambda column: column.dt.total_seconds()
    )


def _is_jsonable(value: Any):
    return isinstance(value, (str, int, float, bool))


def _safe_str(value: Any) -> Optional[str]:
    try:
        return str(value)
    except Exception:
        return None


def serialize_values_list_for_json(values: List[Any]):
    result = []
    for val in values:
        if _is_jsonable(val):
            result.append(val)
        else:
            stringified = _safe_str(val)
            if stringified is not None:
                result.append(stringified)
    return result
