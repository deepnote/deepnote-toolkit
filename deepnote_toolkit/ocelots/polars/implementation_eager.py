import functools
import operator
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, TextIO, Tuple, Union

from typing_extensions import Self

from deepnote_toolkit.logging import LoggerManager
from deepnote_toolkit.ocelots.constants import (
    DEEPNOTE_INDEX_COLUMN,
    MAX_COLUMNS_TO_DISPLAY,
    MAX_STRING_CELL_LENGTH,
)
from deepnote_toolkit.ocelots.filters import Filter, FilterOperator
from deepnote_toolkit.ocelots.types import Column, ColumnsStatsRecord, PolarsEagerDF

logger = LoggerManager().get_logger()


def _stringify_element(v) -> str:
    """Convert a Polars element to a truncated string.

    map_elements on List columns yields polars.Series objects rather than
    Python lists, so we convert to a native Python object first.
    """
    import polars as pl

    if isinstance(v, pl.Series):
        v = v.to_list()
    return str(v)[:MAX_STRING_CELL_LENGTH]


class PolarsEagerImplementation:
    """Implementation of DataFrame methods for Polars dataframes."""

    name: Literal["polars-eager"] = "polars-eager"
    lazy: bool = False

    def __init__(self, df: PolarsEagerDF):
        self._df = df

    @property
    def columns(self) -> Tuple[Column, ...]:
        """Get the list of columns in the dataframe."""
        return tuple(
            Column(name=name, native_type=str(dtype))
            for name, dtype in zip(self._df.columns, self._df.dtypes)
        )

    def paginate(self, page_index: int, page_size: int) -> Self:
        """Paginate the dataframe and return a new instance with the specified page.

        If the requested page index is out of bounds, returns the last page instead.
        """
        total_pages = (self._df.height + page_size - 1) // page_size
        normalized_page_index = (
            min(page_index, total_pages - 1) if total_pages > 0 else 0
        )
        start_idx = normalized_page_index * page_size
        return self.__class__(self._df.slice(start_idx, page_size))

    def size(self) -> int:
        """Get the number of rows in the dataframe."""
        return self._df.height

    def sample(self, n: int, seed: Optional[int] = None) -> Self:
        """Randomly select n records from the dataframe."""
        if n < 1:
            raise ValueError("n must be positive")
        normalized_n = min(n, self.size())
        return self.__class__(self._df.sample(n=normalized_n, seed=seed))

    def sort(self, columns: List[Tuple[str, bool]]) -> Self:
        """Sort the dataframe by multiple columns."""
        by = [col for col, _ in columns]
        descending = [not asc for _, asc in columns]
        return self.__class__(self._df.sort(by=by, descending=descending))

    def _resolve_temporal_col(self, col_name: str):
        """Return a Polars expression that is guaranteed to be temporal.

        If the column is already temporal, returns `pl.col(col_name)` unchanged.
        Otherwise attempts `str.to_datetime()` conversion.
        """
        import polars as pl

        dtype = self._df.schema[col_name]
        expr = pl.col(col_name)
        if dtype.is_temporal():
            return expr
        return expr.str.to_datetime()

    def filter(self, *filters: Filter) -> Self:
        """Filter the dataframe using the provided filters."""
        import polars as pl

        if not filters:
            return self.__class__(self._df.clone())

        conditions = []
        for filter_obj in filters:
            try:
                col = pl.col(filter_obj.column)

                if filter_obj.operator == FilterOperator.TEXT_CONTAINS:
                    if not filter_obj.comparative_values:
                        continue
                    sub_conditions = [
                        col.cast(pl.String)
                        .str.to_lowercase()
                        .str.contains(str(v).lower(), literal=True)
                        for v in filter_obj.comparative_values
                    ]
                    condition = functools.reduce(operator.or_, sub_conditions)
                elif filter_obj.operator == FilterOperator.TEXT_DOES_NOT_CONTAIN:
                    if not filter_obj.comparative_values:
                        continue
                    sub_conditions = [
                        ~col.cast(pl.String)
                        .str.to_lowercase()
                        .str.contains(str(v).lower(), literal=True)
                        for v in filter_obj.comparative_values
                    ]
                    condition = functools.reduce(operator.and_, sub_conditions)
                elif filter_obj.operator in {
                    FilterOperator.IS_EQUAL,
                    FilterOperator.IS_NOT_EQUAL,
                    FilterOperator.GREATER_THAN,
                    FilterOperator.GREATER_THAN_OR_EQUAL,
                    FilterOperator.LESS_THAN,
                    FilterOperator.LESS_THAN_OR_EQUAL,
                }:
                    if not filter_obj.comparative_values:
                        continue
                    comp_value = float(filter_obj.comparative_values[0])
                    num_col = col.cast(pl.Float64)
                    if filter_obj.operator == FilterOperator.IS_EQUAL:
                        condition = num_col == comp_value
                    elif filter_obj.operator == FilterOperator.IS_NOT_EQUAL:
                        condition = num_col != comp_value
                    elif filter_obj.operator == FilterOperator.GREATER_THAN:
                        condition = num_col > comp_value
                    elif filter_obj.operator == FilterOperator.GREATER_THAN_OR_EQUAL:
                        condition = num_col >= comp_value
                    elif filter_obj.operator == FilterOperator.LESS_THAN:
                        condition = num_col < comp_value
                    elif filter_obj.operator == FilterOperator.LESS_THAN_OR_EQUAL:
                        condition = num_col <= comp_value
                elif filter_obj.operator == FilterOperator.OUTSIDE_OF:
                    if len(filter_obj.comparative_values) < 2:
                        continue
                    min_val = float(filter_obj.comparative_values[0])
                    max_val = float(filter_obj.comparative_values[1])
                    num_col = col.cast(pl.Float64)
                    condition = (num_col < min_val) | (num_col > max_val)
                elif filter_obj.operator == FilterOperator.IS_ONE_OF:
                    if not filter_obj.comparative_values:
                        continue
                    dtype = self._df.schema[filter_obj.column]
                    if dtype == pl.Boolean:
                        values = [
                            v.lower() == "true" for v in filter_obj.comparative_values
                        ]
                        condition = col.is_in(values)
                    elif dtype.is_numeric():
                        condition = col.cast(pl.Float64).is_in(
                            [float(v) for v in filter_obj.comparative_values]
                        )
                    else:
                        condition = (
                            col.cast(pl.String)
                            .str.to_lowercase()
                            .is_in(
                                [str(v).lower() for v in filter_obj.comparative_values]
                            )
                        )
                elif filter_obj.operator == FilterOperator.IS_NOT_ONE_OF:
                    if not filter_obj.comparative_values:
                        continue
                    dtype = self._df.schema[filter_obj.column]
                    if dtype.is_numeric():
                        condition = ~col.cast(pl.Float64).is_in(
                            [float(v) for v in filter_obj.comparative_values]
                        )
                    else:
                        condition = ~(
                            col.cast(pl.String)
                            .str.to_lowercase()
                            .is_in(
                                [str(v).lower() for v in filter_obj.comparative_values]
                            )
                        )
                elif filter_obj.operator == FilterOperator.IS_NULL:
                    condition = col.is_null()
                elif filter_obj.operator == FilterOperator.IS_NOT_NULL:
                    condition = col.is_not_null()
                elif filter_obj.operator == FilterOperator.BETWEEN:
                    if len(filter_obj.comparative_values) < 2:
                        continue
                    dtype = self._df.schema[filter_obj.column]
                    if dtype.is_numeric():
                        min_val = float(filter_obj.comparative_values[0])
                        max_val = float(filter_obj.comparative_values[1])
                        condition = col.cast(pl.Float64).is_between(min_val, max_val)
                    else:
                        dt_col = self._resolve_temporal_col(filter_obj.column)
                        lit_start = pl.lit(
                            filter_obj.comparative_values[0]
                        ).str.to_datetime()
                        lit_end = pl.lit(
                            filter_obj.comparative_values[1]
                        ).str.to_datetime()
                        condition = dt_col.is_between(lit_start, lit_end)
                elif filter_obj.operator == FilterOperator.IS_AFTER:
                    if not filter_obj.comparative_values:
                        continue
                    dt_col = self._resolve_temporal_col(filter_obj.column)
                    value = pl.lit(filter_obj.comparative_values[0]).str.to_datetime()
                    condition = dt_col >= value
                elif filter_obj.operator == FilterOperator.IS_BEFORE:
                    if not filter_obj.comparative_values:
                        continue
                    dt_col = self._resolve_temporal_col(filter_obj.column)
                    value = pl.lit(filter_obj.comparative_values[0]).str.to_datetime()
                    condition = dt_col <= value
                elif filter_obj.operator == FilterOperator.IS_ON:
                    if not filter_obj.comparative_values:
                        continue
                    dt_col = self._resolve_temporal_col(filter_obj.column)
                    value = pl.lit(filter_obj.comparative_values[0]).str.to_datetime()
                    condition = dt_col.dt.date() == value.dt.date()
                elif filter_obj.operator == FilterOperator.IS_RELATIVE_TODAY:
                    if not filter_obj.comparative_values:
                        continue
                    relative = filter_obj.comparative_values[0]
                    dt_col = self._resolve_temporal_col(filter_obj.column)

                    col_dtype = self._df.schema[filter_obj.column]
                    col_tz = getattr(col_dtype, "time_zone", None)
                    if col_tz:
                        now_lit = pl.lit(
                            datetime.now(timezone.utc)
                        ).dt.convert_time_zone(col_tz)
                    else:
                        now_lit = pl.lit(datetime.now())

                    if relative == "today":
                        condition = dt_col.dt.date() == now_lit.dt.date()
                    elif relative == "yesterday":
                        condition = (
                            dt_col.dt.date() == now_lit.dt.offset_by("-1d").dt.date()
                        )
                    elif relative == "week-ago":
                        condition = dt_col >= now_lit.dt.offset_by("-1w")
                    elif relative == "month-ago":
                        condition = dt_col >= now_lit.dt.offset_by("-1mo")
                    elif relative == "quarter-ago":
                        condition = dt_col >= now_lit.dt.offset_by("-3mo")
                    elif relative == "half-year-ago":
                        condition = dt_col >= now_lit.dt.offset_by("-6mo")
                    elif relative == "year-ago":
                        condition = dt_col >= now_lit.dt.offset_by("-1y")
                    else:
                        continue
                else:
                    continue

                conditions.append(condition)

            except (ValueError, TypeError, pl.exceptions.PolarsError) as e:
                logger.warning("Skipping filter on column %r: %s", filter_obj.column, e)
                continue

        if conditions:
            combined = functools.reduce(operator.and_, conditions)
            try:
                return self.__class__(self._df.filter(combined))
            except pl.exceptions.PolarsError as e:
                logger.warning(
                    "Filter evaluation failed, returning unfiltered data: %s", e
                )
                return self.__class__(self._df.clone())

        return self.__class__(self._df.clone())

    def to_native(self) -> PolarsEagerDF:
        """Get the underlying native dataframe."""
        return self._df

    def to_records(self, mode: Literal["json", "python"]) -> List[Dict[str, Any]]:
        """Convert the dataframe to a list of dictionaries."""
        import polars as pl

        if mode == "python":
            return self._df.to_dicts()

        select_exprs = []
        for name, dtype in zip(self._df.columns, self._df.dtypes):
            col = pl.col(name)
            if dtype.is_integer() or dtype.is_float() or dtype == pl.Boolean:
                select_exprs.append(col)
            elif dtype == pl.String:
                select_exprs.append(
                    col.str.slice(0, MAX_STRING_CELL_LENGTH).alias(name)
                )
            elif dtype.is_nested() or dtype == pl.Binary or dtype.is_temporal():
                select_exprs.append(
                    col.map_elements(
                        _stringify_element,
                        return_dtype=pl.String,
                    ).alias(name)
                )
            else:
                select_exprs.append(
                    col.cast(pl.String).str.slice(0, MAX_STRING_CELL_LENGTH).alias(name)
                )

        return self._df.select(select_exprs).to_dicts()

    def to_csv(self, path_or_buf: Union[str, TextIO]) -> None:
        """Write the dataframe to a CSV file."""
        df = self._df
        if DEEPNOTE_INDEX_COLUMN in df.columns:
            df = df.drop(DEEPNOTE_INDEX_COLUMN)

        if isinstance(path_or_buf, str):
            df.write_csv(path_or_buf)
        else:
            path_or_buf.write(df.write_csv())

    def analyze_columns(
        self, color_scale_column_names: Optional[List[str]] = None
    ) -> List[ColumnsStatsRecord]:
        """Analyze columns in the dataframe and return statistics.

        Converts to pandas and delegates to the existing pandas analyze_columns
        implementation, since Polars DataFrames are already in memory.
        """
        from deepnote_toolkit.ocelots.pandas.analyze import analyze_columns

        pandas_df = self._df.to_pandas()
        return analyze_columns(pandas_df, color_scale_column_names)

    def get_columns_distinct_values(
        self, column_names: List[str], limit: int = 1000
    ) -> Dict[str, List[Any]]:
        """Get distinct values from multiple columns. Results are limited to 1000 values per column."""
        result = {}
        capped_limit = min(limit, 1000)
        for column_name in column_names:
            try:
                unique_values = (
                    self._df.get_column(column_name)
                    .drop_nulls()
                    .unique()
                    .head(capped_limit)
                    .to_list()
                )
                result[column_name] = unique_values
            except Exception:
                result[column_name] = []
        return result

    def prepare_for_serialization(self) -> Self:
        """Prepare the dataframe for serialization."""
        import polars as pl

        df = self._df.select(self._df.columns[:MAX_COLUMNS_TO_DISPLAY])

        df = df.with_row_index(DEEPNOTE_INDEX_COLUMN)
        df = df.with_columns(pl.col(DEEPNOTE_INDEX_COLUMN).cast(pl.Int64))

        return self.__class__(df)
