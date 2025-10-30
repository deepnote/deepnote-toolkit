from typing import Any, Dict, List, Literal, Optional, TextIO, Tuple, Union

import numpy as np
import pandas as pd
from typing_extensions import Self

from deepnote_toolkit.ocelots.constants import (
    DEEPNOTE_INDEX_COLUMN,
    MAX_COLUMNS_TO_DISPLAY,
)
from deepnote_toolkit.ocelots.filters import Filter, FilterOperator
from deepnote_toolkit.ocelots.types import Column, ColumnsStatsRecord

from .analyze import analyze_columns
from .utils import (
    cast_objects_to_string,
    deduplicate_columns,
    fill_nat,
    fix_nan_category,
    flatten_column_name,
)


class PandasImplementation:
    """Implementation of DataFrame methods for pandas dataframes."""

    name: Literal["pandas"] = "pandas"

    def __init__(self, df: pd.DataFrame):
        self._df = df

    @property
    def columns(self) -> Tuple[Column, ...]:
        """Get the list of columns in the dataframe."""
        return tuple(
            [
                Column(name=col_name, native_type=str(dtype))
                for col_name, dtype in self._df.dtypes.items()
            ]
        )

    def paginate(self, page_index: int, page_size: int) -> Self:
        """Paginate the dataframe and return a new instance with the specified page.

        If the requested page index is out of bounds, returns the last page instead.
        """
        total_pages = (len(self._df) + page_size - 1) // page_size
        normalized_page_index = (
            min(page_index, total_pages - 1) if total_pages > 0 else 0
        )
        start_idx = normalized_page_index * page_size
        end_idx = start_idx + page_size
        return self.__class__(self._df.iloc[start_idx:end_idx])

    def size(self) -> int:
        """Get the number of rows in the dataframe."""
        return len(self._df)

    def sample(self, n: int, seed: Optional[int] = None) -> Self:
        """Randomly select n records from the dataframe."""
        if n < 1:
            raise ValueError("n must be positive")
        normalized_n = min(n, self.size())
        return self.__class__(self._df.sample(n=normalized_n, random_state=seed))

    def sort(self, columns: List[Tuple[str, bool]]) -> Self:
        """Sort the dataframe by multiple columns."""
        by = [col for col, _ in columns]
        ascending = [asc for _, asc in columns]
        return self.__class__(self._df.sort_values(by=by, ascending=ascending))

    def filter(self, *filters: Filter) -> Self:
        """Filter the dataframe using the provided filters."""
        if not filters:
            return self.__class__(self._df.copy())

        masks = []
        for filter_obj in filters:
            try:
                if filter_obj.operator == FilterOperator.TEXT_CONTAINS:
                    mask = (
                        self._df[filter_obj.column]
                        .astype(str)
                        .apply(
                            lambda x: any(
                                str(val).lower() in x.lower()
                                for val in filter_obj.comparative_values
                            )
                        )
                    )
                elif filter_obj.operator == FilterOperator.TEXT_DOES_NOT_CONTAIN:
                    mask = (
                        self._df[filter_obj.column]
                        .astype(str)
                        .apply(
                            lambda x: all(
                                str(val).lower() not in x.lower()
                                for val in filter_obj.comparative_values
                            )
                        )
                    )
                elif filter_obj.operator in {
                    FilterOperator.IS_EQUAL,
                    FilterOperator.IS_NOT_EQUAL,
                    FilterOperator.GREATER_THAN,
                    FilterOperator.GREATER_THAN_OR_EQUAL,
                    FilterOperator.LESS_THAN,
                    FilterOperator.LESS_THAN_OR_EQUAL,
                    FilterOperator.OUTSIDE_OF,
                }:
                    if not filter_obj.comparative_values:
                        continue
                    comp_value = float(filter_obj.comparative_values[0])
                    if filter_obj.operator == FilterOperator.IS_EQUAL:
                        mask = self._df[filter_obj.column].astype(float) == comp_value
                    elif filter_obj.operator == FilterOperator.IS_NOT_EQUAL:
                        mask = self._df[filter_obj.column].astype(float) != comp_value
                    elif filter_obj.operator == FilterOperator.GREATER_THAN:
                        mask = self._df[filter_obj.column].astype(float) > comp_value
                    elif filter_obj.operator == FilterOperator.GREATER_THAN_OR_EQUAL:
                        mask = self._df[filter_obj.column].astype(float) >= comp_value
                    elif filter_obj.operator == FilterOperator.LESS_THAN:
                        mask = self._df[filter_obj.column].astype(float) < comp_value
                    elif filter_obj.operator == FilterOperator.LESS_THAN_OR_EQUAL:
                        mask = self._df[filter_obj.column].astype(float) <= comp_value
                    elif filter_obj.operator == FilterOperator.OUTSIDE_OF:
                        if len(filter_obj.comparative_values) < 2:
                            continue
                        mask = ~self._df[filter_obj.column].between(
                            float(filter_obj.comparative_values[0]),
                            float(filter_obj.comparative_values[1]),
                        )
                elif filter_obj.operator == FilterOperator.IS_ONE_OF:
                    if not filter_obj.comparative_values:
                        continue
                    if pd.api.types.is_bool_dtype(self._df[filter_obj.column]):
                        values = [
                            val.lower() == "true"
                            for val in filter_obj.comparative_values
                        ]
                        mask = self._df[filter_obj.column].isin(values)
                    elif pd.api.types.is_numeric_dtype(self._df[filter_obj.column]):
                        mask = (
                            self._df[filter_obj.column]
                            .astype(float)
                            .isin([float(val) for val in filter_obj.comparative_values])
                        )
                    else:
                        mask = (
                            self._df[filter_obj.column]
                            .astype(str)
                            .str.lower()
                            .isin(
                                [
                                    str(val).lower()
                                    for val in filter_obj.comparative_values
                                ]
                            )
                        )
                elif filter_obj.operator == FilterOperator.IS_NOT_ONE_OF:
                    if not filter_obj.comparative_values:
                        continue
                    if pd.api.types.is_numeric_dtype(self._df[filter_obj.column]):
                        mask = (
                            ~self._df[filter_obj.column]
                            .astype(float)
                            .isin([float(val) for val in filter_obj.comparative_values])
                        )
                    else:
                        mask = (
                            ~self._df[filter_obj.column]
                            .astype(str)
                            .str.lower()
                            .isin(
                                [
                                    str(val).lower()
                                    for val in filter_obj.comparative_values
                                ]
                            )
                        )
                elif filter_obj.operator == FilterOperator.IS_NULL:
                    mask = self._df[filter_obj.column].isna()
                elif filter_obj.operator == FilterOperator.IS_NOT_NULL:
                    mask = self._df[filter_obj.column].notna()
                elif filter_obj.operator == FilterOperator.BETWEEN:
                    if len(filter_obj.comparative_values) < 2:
                        continue
                    if pd.api.types.is_numeric_dtype(self._df[filter_obj.column]):
                        mask = self._df[filter_obj.column].between(
                            float(filter_obj.comparative_values[0]),
                            float(filter_obj.comparative_values[1]),
                        )
                    else:
                        col = pd.to_datetime(self._df[filter_obj.column])
                        if col.dt.tz is None:
                            col = col.dt.tz_localize("UTC", ambiguous="NaT")
                        else:
                            col = col.dt.tz_convert("UTC")

                        values = [
                            (
                                pd.to_datetime(val).tz_localize("UTC")
                                if pd.to_datetime(val).tzinfo is None
                                else pd.to_datetime(val).tz_convert("UTC")
                            )
                            for val in filter_obj.comparative_values
                        ]
                        mask = col.between(values[0], values[1])
                elif filter_obj.operator == FilterOperator.IS_AFTER:
                    if not filter_obj.comparative_values:
                        continue
                    col = pd.to_datetime(self._df[filter_obj.column])
                    if col.dt.tz is None:
                        col = col.dt.tz_localize("UTC", ambiguous="NaT")
                    else:
                        col = col.dt.tz_convert("UTC")

                    value = pd.to_datetime(filter_obj.comparative_values[0])
                    if value.tzinfo is None:
                        value = value.tz_localize("UTC")
                    else:
                        value = value.tz_convert("UTC")
                    mask = col >= value
                elif filter_obj.operator == FilterOperator.IS_BEFORE:
                    if not filter_obj.comparative_values:
                        continue
                    col = pd.to_datetime(self._df[filter_obj.column])
                    if col.dt.tz is None:
                        col = col.dt.tz_localize("UTC", ambiguous="NaT")
                    else:
                        col = col.dt.tz_convert("UTC")

                    value = pd.to_datetime(filter_obj.comparative_values[0])
                    if value.tzinfo is None:
                        value = value.tz_localize("UTC")
                    else:
                        value = value.tz_convert("UTC")
                    mask = col <= value
                elif filter_obj.operator == FilterOperator.IS_ON:
                    if not filter_obj.comparative_values:
                        continue
                    col = pd.to_datetime(self._df[filter_obj.column])
                    if col.dt.tz is None:
                        col = col.dt.tz_localize("UTC", ambiguous="NaT")
                    else:
                        col = col.dt.tz_convert("UTC")

                    value = pd.to_datetime(filter_obj.comparative_values[0])
                    if value.tzinfo is None:
                        value = value.tz_localize("UTC")
                    else:
                        value = value.tz_convert("UTC")
                    mask = col == value
                elif filter_obj.operator == FilterOperator.IS_RELATIVE_TODAY:
                    if not filter_obj.comparative_values:
                        continue
                    now = pd.Timestamp("now", tz="UTC")
                    value = filter_obj.comparative_values[0]
                    col = pd.to_datetime(self._df[filter_obj.column])
                    if col.dt.tz is None:
                        col = col.dt.tz_localize("UTC", ambiguous="NaT")
                    else:
                        col = col.dt.tz_convert("UTC")

                    if value == "today":
                        mask = col.dt.date == now.date()
                    elif value == "yesterday":
                        mask = col.dt.date == (now - pd.Timedelta(days=1)).date()
                    elif value == "week-ago":
                        mask = col >= (now - pd.Timedelta(weeks=1))
                    elif value == "month-ago":
                        mask = col >= (now - pd.DateOffset(months=1))
                    elif value == "quarter-ago":
                        mask = col >= (now - pd.DateOffset(months=3))
                    elif value == "half-year-ago":
                        mask = col >= (now - pd.DateOffset(months=6))
                    elif value == "year-ago":
                        mask = col >= (now - pd.DateOffset(years=1))
                    else:
                        continue
                else:
                    continue

                masks.append(mask)

            except (ValueError, TypeError):
                continue

        if masks:
            df = self._df[np.logical_and.reduce(masks)]
        else:
            df = self._df.copy()

        return self.__class__(df)

    def to_native(self) -> pd.DataFrame:
        """Get the underlying native dataframe."""
        return self._df

    def to_records(self, mode: Literal["json", "python"]) -> List[Dict[str, Any]]:
        """Convert the dataframe to a list of dictionaries."""
        df_copy = self._df.copy()
        if mode == "json":
            fill_nat(df_copy, "NaT")
            cast_objects_to_string(df_copy)
        return df_copy.to_dict("records")

    def to_csv(self, path_or_buf: Union[str, TextIO]) -> None:
        """Write the dataframe to a CSV file."""
        if DEEPNOTE_INDEX_COLUMN in self._df.columns:
            self._df.drop(columns=[DEEPNOTE_INDEX_COLUMN]).to_csv(
                path_or_buf, index=False
            )
        else:
            self._df.to_csv(path_or_buf, index=False)

    def analyze_columns(
        self, color_scale_column_names: Optional[List[str]] = None
    ) -> List[ColumnsStatsRecord]:
        """Analyze columns in a Pandas DataFrame, but only within a certain computational limit."""
        return analyze_columns(self._df, color_scale_column_names)

    def get_columns_distinct_values(
        self, column_names: List[str], limit: int = 1000
    ) -> Dict[str, List[Any]]:
        """Get distinct values from multiple columns. Results are limited to 1000 values per column."""
        result = {}
        for column_name in column_names:
            try:
                result[column_name] = (
                    self._df[column_name].dropna().unique().tolist()[: min(limit, 1000)]
                )
            except TypeError:
                # This happens when the column contains e.g. dictionaries, lists or sets
                result[column_name] = []
        return result

    def prepare_for_serialization(self) -> Self:
        """Prepare the dataframe for serialization."""
        df_analyzed = self._df.copy()
        df_analyzed.columns = map(flatten_column_name, df_analyzed.columns)
        deduplicate_columns(df_analyzed)
        df_analyzed = fix_nan_category(df_analyzed)

        df_analyzed = df_analyzed.iloc[:, :MAX_COLUMNS_TO_DISPLAY]

        # We need to send data to frontend in a form of a list, otherwise we lose information
        # on sorting/ordering of the dataframe. However, when we are sending data as list (orient='records')
        # we are losing the information on index so we need to send it to frontend using a custom column name
        df_analyzed[DEEPNOTE_INDEX_COLUMN] = df_analyzed.index
        df_analyzed.columns = df_analyzed.columns.astype(str)

        return self.__class__(df_analyzed)
