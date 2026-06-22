from typing import Any, List, Optional, Set

import pandas as pd

import deepnote_toolkit.ocelots as oc


def sanitize_dataframe_for_chart(
    pd_df: pd.DataFrame, temporal_fields: Optional[Set[str]] = None
) -> pd.DataFrame:
    sanitized_dataframe = pd_df.copy()

    oc.pandas.utils.deduplicate_columns(sanitized_dataframe)
    _convert_timedelta_columns_to_seconds(sanitized_dataframe)
    _convert_column_names_to_string(sanitized_dataframe)
    _convert_datetime_string_columns(sanitized_dataframe, temporal_fields)

    return sanitized_dataframe


def _convert_datetime_string_columns(
    pd_df: pd.DataFrame, temporal_fields: Optional[Set[str]] = None
) -> None:
    """
    Converts object columns that contain ISO datetime strings to datetime64.

    VegaFusion treats datetime64 columns as temporal natively. When columns are
    left as object (string), VegaFusion attempts to parse them using the axis
    display format from the Vega-Lite spec (e.g. ``'%B %d, %Y %H:%M'``), which
    fails for ISO 8601 strings and raises a DataFusion ValueError.

    Only columns named in ``temporal_fields`` (fields the spec encodes as
    ``"type": "temporal"``) are converted. This avoids turning nominal string
    axes such as years (``"2020"``), months (``"2024-01"``) or numeric codes
    into time scales just because they happen to parse as ISO 8601. When
    ``temporal_fields`` is ``None`` or empty, no columns are converted.

    WARNING: This function modifies the DataFrame in-place.
    """
    if not temporal_fields:
        return

    for col in pd_df.columns:
        if col not in temporal_fields:
            continue
        if pd_df[col].dtype != object:
            continue
        non_null = pd_df[col].dropna()
        if len(non_null) == 0:
            continue
        try:
            converted = pd.to_datetime(non_null, format="ISO8601", utc=True, errors="coerce")
            if converted.notna().all():
                pd_df[col] = pd.to_datetime(pd_df[col], format="ISO8601", utc=True, errors="coerce")
        except Exception:
            pass


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
