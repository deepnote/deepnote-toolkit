import csv
import functools
import operator
from typing import Any, Dict, List, Literal, Optional, TextIO, Tuple, Union

from typing_extensions import Self

from deepnote_toolkit.ocelots.constants import (
    MAX_COLUMNS_TO_DISPLAY,
    MAX_STRING_CELL_LENGTH,
)
from deepnote_toolkit.ocelots.filters import Filter, FilterOperator
from deepnote_toolkit.ocelots.types import (
    Column,
    ColumnsStatsRecord,
    ColumnStats,
    PysparkDF,
)


class PysparkImplementation:
    """Implementation of DataFrame methods for PySpark dataframes."""

    name: Literal["pyspark"] = "pyspark"

    def __init__(self, df: PysparkDF):
        self._df = df

    @property
    def columns(self) -> Tuple[Column, ...]:
        """Get the list of columns in the dataframe."""
        return tuple(
            [
                Column(name=field.name, native_type=field.dataType.simpleString())
                for field in self._df.schema.fields
            ]
        )

    def paginate(self, page_index: int, page_size: int) -> Self:
        """Paginate the dataframe and return a new instance with the specified page.

        If the requested page index is out of bounds, returns the last page instead.

        NOTE: Due to way Spark works, we can't do this performantly without knowing the
        shape of dataframe in advance. So this might be quite slow for pages with
        big offset, as it requires pulling all preceding rows into driver machine.
        """
        total_rows = self.size()
        total_pages = (total_rows + page_size - 1) // page_size
        normalized_page_index = (
            min(page_index, total_pages - 1) if total_pages > 0 else 0
        )

        start = normalized_page_index * page_size

        if hasattr(self._df, "offset") and callable(self._df.offset):
            # Spark 3.4+
            paged = self._df.offset(start).limit(page_size)
        else:
            rows = self._df.limit(start + page_size).tail(page_size)
            paged = self._df.sparkSession.createDataFrame(rows, self._df.schema)

        return self.__class__(paged)

    def size(self) -> int:
        """Get the number of rows in the dataframe."""
        return self._df.count()

    def sample(self, n: int, seed: Optional[int] = None) -> Self:
        """Randomly select n records from the dataframe."""
        if n < 1:
            raise ValueError("n must be positive")

        # Spark doesn't guarantee that we get exactly self.size() * fraction records. It can be slightly more or
        # slightly less. So instead we dial up fraction a bit to get more rows and then limit resulting dataframe
        fraction = min(1.2 * n / self.size(), 1.0)
        return self.__class__(self._df.sample(fraction=fraction, seed=seed).limit(n))

    def sort(self, columns: List[Tuple[str, bool]]) -> Self:
        """Sort the dataframe by multiple columns."""
        by = [col for col, _ in columns]
        ascending = [asc for _, asc in columns]
        return self.__class__(self._df.sort(*by, ascending=ascending))

    def filter(self, *filters: Filter) -> Self:
        """Filter the dataframe using the provided filters."""
        if not filters:
            return self.__class__(self._df)

        # We don't import this on top of the file because it's not guaranteed to be installed.
        # However, at the time this code is called, we can be sure there is PySpark installed
        from pyspark.sql import functions as F

        conditions = []
        for filter_obj in filters:
            try:
                col = F.col(filter_obj.column)

                if filter_obj.operator == FilterOperator.TEXT_CONTAINS:
                    if not filter_obj.comparative_values:
                        continue
                    conditions_list = [
                        F.lower(col.cast("string")).like(f"%{value.lower()}%")
                        for value in filter_obj.comparative_values
                    ]
                    condition = functools.reduce(operator.or_, conditions_list)
                elif filter_obj.operator == FilterOperator.TEXT_DOES_NOT_CONTAIN:
                    if not filter_obj.comparative_values:
                        continue
                    conditions_list = [
                        ~F.lower(col.cast("string")).like(f"%{value.lower()}%")
                        for value in filter_obj.comparative_values
                    ]
                    condition = functools.reduce(operator.and_, conditions_list)
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
                    value = float(filter_obj.comparative_values[0])
                    if filter_obj.operator == FilterOperator.IS_EQUAL:
                        condition = col == value
                    elif filter_obj.operator == FilterOperator.IS_NOT_EQUAL:
                        condition = col != value
                    elif filter_obj.operator == FilterOperator.GREATER_THAN:
                        condition = col > value
                    elif filter_obj.operator == FilterOperator.GREATER_THAN_OR_EQUAL:
                        condition = col >= value
                    elif filter_obj.operator == FilterOperator.LESS_THAN:
                        condition = col < value
                    elif filter_obj.operator == FilterOperator.LESS_THAN_OR_EQUAL:
                        condition = col <= value
                elif filter_obj.operator == FilterOperator.OUTSIDE_OF:
                    if len(filter_obj.comparative_values) < 2:
                        continue
                    min_val, max_val = float(filter_obj.comparative_values[0]), float(
                        filter_obj.comparative_values[1]
                    )
                    condition = (col < min_val) | (col > max_val)
                elif filter_obj.operator == FilterOperator.IS_ONE_OF:
                    if not filter_obj.comparative_values:
                        continue
                    condition = col.isin(filter_obj.comparative_values)
                elif filter_obj.operator == FilterOperator.IS_NOT_ONE_OF:
                    if not filter_obj.comparative_values:
                        continue
                    condition = ~col.isin(filter_obj.comparative_values)
                elif filter_obj.operator == FilterOperator.IS_NULL:
                    condition = col.isNull()
                elif filter_obj.operator == FilterOperator.IS_NOT_NULL:
                    condition = col.isNotNull()
                elif filter_obj.operator == FilterOperator.BETWEEN:
                    if len(filter_obj.comparative_values) < 2:
                        continue
                    min_val, max_val = float(filter_obj.comparative_values[0]), float(
                        filter_obj.comparative_values[1]
                    )
                    condition = (col >= min_val) & (col <= max_val)
                elif filter_obj.operator == FilterOperator.IS_AFTER:
                    if not filter_obj.comparative_values:
                        continue
                    condition = col >= F.to_timestamp(
                        F.lit(filter_obj.comparative_values[0])
                    )
                elif filter_obj.operator == FilterOperator.IS_BEFORE:
                    if not filter_obj.comparative_values:
                        continue
                    condition = col <= F.to_timestamp(
                        F.lit(filter_obj.comparative_values[0])
                    )
                elif filter_obj.operator == FilterOperator.IS_ON:
                    if not filter_obj.comparative_values:
                        continue
                    date = F.to_date(F.lit(filter_obj.comparative_values[0]))
                    condition = F.to_date(col) == date
                elif filter_obj.operator == FilterOperator.IS_RELATIVE_TODAY:
                    if not filter_obj.comparative_values:
                        continue
                    value = filter_obj.comparative_values[0]
                    today = F.current_date()

                    if value == "today":
                        condition = F.to_date(col) == today
                    elif value == "yesterday":
                        condition = F.to_date(col) == F.date_sub(today, 1)
                    elif value == "week-ago":
                        condition = col >= F.date_sub(today, 7)
                    elif value == "month-ago":
                        condition = col >= F.add_months(today, -1)
                    elif value == "quarter-ago":
                        condition = col >= F.add_months(today, -3)
                    elif value == "half-year-ago":
                        condition = col >= F.add_months(today, -6)
                    elif value == "year-ago":
                        condition = col >= F.add_months(today, -12)
                    else:
                        continue
                else:
                    continue

                conditions.append(condition)

            except (ValueError, TypeError):
                continue

        if conditions:
            compiled_condition = functools.reduce(operator.and_, conditions)
            df = self._df.filter(compiled_condition)
        else:
            df = self._df

        return self.__class__(df)

    def to_native(self) -> PysparkDF:
        """Get the underlying native dataframe."""
        return self._df

    def to_records(self, mode: Literal["json", "python"]) -> List[Dict[str, Any]]:
        """Convert the dataframe to a list of dictionaries."""
        from pyspark.sql import Column
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            BinaryType,
            DecimalType,
            NumericType,
            StringType,
            StructField,
        )

        def binary_to_string_repr(
            binary_data: Optional[Union[bytes, bytearray]]
        ) -> Optional[str]:
            """Convert binary data to Python string representation (e.g., b'hello').

            Args:
                binary_data: Binary data as bytes or bytearray. PySpark passes BinaryType
                    as bytearray by default, but Spark 4.1+ with
                    spark.sql.execution.pyspark.binaryAsBytes=true passes bytes instead.
            """
            if binary_data is None:
                return None
            return str(bytes(binary_data))

        binary_udf = F.udf(binary_to_string_repr, StringType())

        def select_column(field: StructField) -> Column:
            col = F.col(field.name)
            # Numbers are already JSON-serialise, except Decimal
            if isinstance(field.dataType, NumericType) and not isinstance(
                field.dataType, DecimalType
            ):
                return col

            # We slice binary field before converting to string representation
            if isinstance(field.dataType, BinaryType):
                # Each byte becomes up to 4 chars (\xNN) in string repr, plus b'' overhead
                max_binary_bytes = (MAX_STRING_CELL_LENGTH - 3) // 4
                sliced = F.substring(F.col(field.name), 1, max_binary_bytes)
                return binary_udf(sliced)

            # String just needs to be trimmed
            if isinstance(field.dataType, StringType):
                return F.substring(col, 1, MAX_STRING_CELL_LENGTH)

            # Everything else gets stringified (Decimal, Date, Timestamp, Struct, …)
            return F.substring(col.cast("string"), 1, MAX_STRING_CELL_LENGTH)

        if mode == "python":
            return [row.asDict() for row in self._df.collect()]
        elif mode == "json":
            query = (
                select_column(field).alias(field.name)
                for field in self._df.schema.fields
            )
            converted_df = self._df.select(*query)

            return [row.asDict(True) for row in converted_df.collect()]

    def to_csv(self, path_or_buf: Union[str, TextIO]) -> None:
        """Write the dataframe to a CSV file."""

        def write_file(file, fieldnames, iterator):
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for row in iterator:
                writer.writerow(row.asDict(True))

        fieldnames = [field.name for field in self._df.schema.fields]
        iterator = self._df.toLocalIterator()

        if isinstance(path_or_buf, str):
            with open(path_or_buf, "w") as csvfile:
                write_file(csvfile, fieldnames, iterator)
        else:
            write_file(path_or_buf, fieldnames, iterator)

    def analyze_columns(
        self, color_scale_column_names: Optional[List[str]] = None
    ) -> List[ColumnsStatsRecord]:
        """Analyze columns in the dataframe and return statistics."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import NumericType

        if color_scale_column_names is None:
            color_scale_column_names = []

        numeric_cols = [
            field.name
            for field in self._df.schema.fields
            if field.name in color_scale_column_names
            and isinstance(field.dataType, NumericType)
        ]

        agg_exprs = [
            F.min(col_name).alias(f"{col_name}_min") for col_name in numeric_cols
        ] + [F.max(col_name).alias(f"{col_name}_max") for col_name in numeric_cols]

        if agg_exprs:
            min_max_df = self._df.agg(*agg_exprs)
            min_max_row = min_max_df.collect()[0]
        else:
            min_max_row = {}

        records = []
        for col in self.columns:
            if col.name in numeric_cols:
                stats = ColumnStats(
                    min=str(min_max_row[f"{col.name}_min"]),
                    max=str(min_max_row[f"{col.name}_max"]),
                )
            else:
                stats = None
            records.append(
                ColumnsStatsRecord(name=col.name, dtype=col.native_type, stats=stats)
            )

        return records

    def get_columns_distinct_values(
        self, column_names: List[str], limit: int = 100
    ) -> Dict[str, List[Any]]:
        """Get distinct values from multiple columns. Results are limited to 100 values per column."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import MapType

        # Spark can get distinct values for every column type EXCEPT maps
        map_columns = [
            field.name
            for field in self._df.schema.fields
            if isinstance(field.dataType, MapType)
        ]

        agg_exprs = [
            F.slice(F.collect_set(c), 1, min(limit, 100)).alias(c)
            for c in column_names
            if c not in map_columns
        ]
        result = self._df.agg(*agg_exprs).first().asDict()
        for col in column_names:
            if col in map_columns:
                result[col] = []

        return result

    def prepare_for_serialization(self) -> Self:
        """Prepare the dataframe for serialization."""
        # NOTE: We do not add the DEEPNOTE_INDEX_COLUMN column here because it's not possible to get a consecutive
        # sequence of numbers used as a row ID in PySpark in a performant manner. We use DataPreview to handle PySpark
        # dataframes in the app, and it adds its own index column when materializing the records.
        columns = self._df.columns[:MAX_COLUMNS_TO_DISPLAY]
        return self.__class__(self._df.select(*columns))
