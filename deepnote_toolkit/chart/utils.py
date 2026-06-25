import uuid
from typing import TYPE_CHECKING, Any, List, Optional

import pandas as pd

import deepnote_toolkit.ocelots as oc

if TYPE_CHECKING:
    import polars as pl


def sanitize_dataframe_for_chart(pd_df: pd.DataFrame):
    sanitized_dataframe = pd_df.copy()

    oc.pandas.utils.deduplicate_columns(sanitized_dataframe)
    _convert_timedelta_columns_to_seconds(sanitized_dataframe)
    _convert_uuid_columns_to_string(sanitized_dataframe)
    _convert_column_names_to_string(sanitized_dataframe)

    return sanitized_dataframe


def sanitize_polars_dataframe_for_chart(pl_df: "pl.DataFrame") -> "pl.DataFrame":
    """
    Coerce polars columns that VegaFusion cannot serialize into chart-friendly
    types, returning a new DataFrame.

    polars stores values it has no native type for (e.g. ``uuid.UUID`` objects)
    in an ``Object`` column, which converts to an opaque Arrow ``FixedSizeBinary``
    that VegaFusion cannot serialize to JSON. Such columns are not meaningfully
    chartable as-is, so we stringify them -- the polars analogue of the UUID
    handling in :func:`sanitize_dataframe_for_chart` for the pandas path.
    """
    import polars as pl

    object_columns = [
        name for name, dtype in zip(pl_df.columns, pl_df.dtypes) if dtype == pl.Object
    ]
    if not object_columns:
        return pl_df

    return pl_df.with_columns(
        pl.col(name).map_elements(str, return_dtype=pl.String)
        for name in object_columns
    )


def _convert_column_names_to_string(pd_df: pd.DataFrame):
    """
    Converts dataframe column names to strings.

    WARNING: This function modifies the DataFrame in-place.
    """
    pd_df.columns = pd_df.columns.astype(str)


def _convert_uuid_columns_to_string(pd_df: pd.DataFrame):
    """
    Converts columns of ``uuid.UUID`` objects to strings.

    Starting with pyarrow 24.0.0, Arrow conversion infers the canonical
    ``arrow.uuid`` extension type (backed by ``FixedSizeBinary(16)``) for object
    columns holding ``uuid.UUID`` values; pyarrow <= 23 produced a serializable
    result for the same data. VegaFusion's Arrow runtime cannot serialize
    ``FixedSizeBinary(16)`` to JSON (``Unsupported datatype for JSON
    serialization: FixedSizeBinary(16)``), so we stringify such columns to keep
    charting working across pyarrow versions.

    WARNING: This function modifies the DataFrame in-place.
    """
    for column in pd_df.columns:
        col = pd_df[column]
        if not pd.api.types.is_object_dtype(col.dtype):
            continue
        non_null = col.dropna()
        if non_null.empty or not isinstance(non_null.iloc[0], uuid.UUID):
            continue
        pd_df[column] = col.map(
            lambda value: str(value) if isinstance(value, uuid.UUID) else value
        )


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
