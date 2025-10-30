from __future__ import annotations

import io
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    TextIO,
    Tuple,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import Self, TypeGuard

if TYPE_CHECKING:
    from deepnote_toolkit.ocelots.data_preview import DataPreview

from deepnote_toolkit.ocelots.filters import Filter
from deepnote_toolkit.ocelots.pandas.implementation import PandasImplementation
from deepnote_toolkit.ocelots.pyspark.implementation import PysparkImplementation
from deepnote_toolkit.ocelots.types import (
    Column,
    ColumnsStatsRecord,
    NativeInputDF,
    NativeOutputDF,
    NativeOutputType,
    PandasDF,
    PandasOnSparkDF,
    PysparkDF,
    UnsupportedDataFrameException,
)
from deepnote_toolkit.ocelots.utils import (
    is_pandas_dataframe,
    is_pandas_on_spark_dataframe,
    is_pyspark_dataframe,
)

Implementation = Union[PandasImplementation, PysparkImplementation]

T = TypeVar("T", bound=NativeOutputDF)
FromNativeT = TypeVar("FromNativeT", bound=NativeOutputDF)


class DataFrame(Generic[T]):
    """A generic wrapper for supported DataFrame libraries.

    This class provides a unified interface for working with different DataFrame implementations,
    abstracting away their differences while maintaining native functionality.

    To create an instance, use the `from_native` class method with a supported DataFrame.
    The class will automatically detect the DataFrame type and create an appropriate wrapper.
    """

    _implementation: Implementation

    def __init__(self, implementation: Implementation):
        self._implementation = implementation

    def __repr__(self) -> str:
        return f"DataFrame(native_type='{self.native_type}')"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DataFrame):
            return False
        if self.native_type != other.native_type:
            return False
        return self._implementation.to_native() == other._implementation.to_native()

    @classmethod
    def is_supported(cls, df: Any) -> bool:
        """Check if the given dataframe is supported by the ocelots.

        Args:
            df: Any object to check for dataframe support

        Returns:
            bool: True if the object is a supported dataframe type, False otherwise
        """
        return (
            is_pandas_dataframe(df)
            or is_pyspark_dataframe(df)
            or is_pandas_on_spark_dataframe(df)
        )

    # Special case for Pandas-on-Spark DFs, as they aren't wrapped directly, but converted
    # to Spark DataFrame first, see comment in .from_native method
    @overload
    @classmethod
    def from_native(cls, df: PandasOnSparkDF) -> "DataFrame[PysparkDF]":
        pass

    @overload
    @classmethod
    def from_native(cls, df: FromNativeT) -> "DataFrame[FromNativeT]":
        pass

    @classmethod
    def from_native(cls, df: NativeInputDF):
        """Create a DataFrame instance from a native dataframe.

        Args:
            df: Native dataframe to wrap

        Returns:
            DataFrame: New instance wrapping the native dataframe

        Raises:
            UnsupportedDataFrameException: If the input is not a supported dataframe type
        """
        if is_pandas_dataframe(df):
            return cls(PandasImplementation(df))
        if is_pyspark_dataframe(df):
            return cls(PysparkImplementation(df))
        if is_pandas_on_spark_dataframe(df):
            # NOTE: we accept Pandas-on-Spark dataframes, but we convert them into Spark and
            # work like with it same as with normal Spark DF from that.
            # Main problem with Pandas-on-Spark is that they have Pandas API (so we can't
            # reuse existing Spark implementation), but they also have network delay of
            # Spark (so we can't reuse Pandas implementation as it simply does too much -> is too slow).
            # So in the end, if we want to fully support Pandas-on-Spark (i.e. both accept and *output*
            # Pandas-on-Spark DFs), we need a separate implementation for it. We don't yet have a
            # need to transparently output DFs of the same type, so we reuse Spark implementation
            # for Pandas-on-Spark to save time and efforts.

            # NOTE: we construct new instance here, so we need to transfer data preview (if there is any) manually
            # Local import to avoid cyclic import at top level
            from deepnote_toolkit.ocelots.data_preview import (
                DeepnoteDataFrameWithDataPreview,
            )

            if isinstance(df, DeepnoteDataFrameWithDataPreview):
                data_preview = df.deepnote_data_preview
                native_spark_df = df.to_spark()
                wrapped_spark_df = DeepnoteDataFrameWithDataPreview(native_spark_df)
                wrapped_spark_df.deepnote_data_preview = data_preview
                return cls(PysparkImplementation(wrapped_spark_df))

            return cls(PysparkImplementation(df.to_spark()))

        raise UnsupportedDataFrameException(
            f"expected Pandas or PySpark dataframe, got {type(df)}"
        )

    @property
    def native_type(self) -> NativeOutputType:
        """Get the native type of the dataframe.

        Returns:
            NativeType: Either 'pandas' or 'pyspark'
        """
        return self._implementation.name

    @property
    def columns(self) -> Tuple[Column, ...]:
        """Get the list of columns in the dataframe.

        Returns:
            Tuple[Column, ...]: Tuple of Column objects containing name and **native** type
        """
        return self._implementation.columns

    @property
    def column_names(self) -> Tuple[str, ...]:
        """Get the list of column names in the dataframe.

        Returns:
            Tuple[str, ...]: Tuple of column names as strings
        """
        return tuple([col.name for col in self.columns])

    @property
    def data_preview(self) -> Optional["DataPreview"]:
        # Local import to avoid cyclic import at top level
        from deepnote_toolkit.ocelots.data_preview import (
            DeepnoteDataFrameWithDataPreview,
        )

        native_df = self.to_native()
        if isinstance(native_df, DeepnoteDataFrameWithDataPreview):
            return native_df.deepnote_data_preview

        return None

    def paginate(self, page_index: int, page_size: int) -> Self:
        """Paginate the dataframe and return a new instance with the specified page. If the requested
        page index is out of bounds, returns the last page instead.

        Args:
            page_index: 0-based page index
            page_size: Number of rows per page

        Returns:
            New DataFrame instance containing the requested page

        Raises:
            ValueError: If page_index is negative or page_size is less than 1
        """
        if page_index < 0 or page_size < 1:
            raise ValueError(
                "page_index must be non-negative and page_size must be positive"
            )
        return self.__class__(self._implementation.paginate(page_index, page_size))

    def size(self) -> int:
        """Get the number of rows in the dataframe.

        Returns:
            int: Number of rows
        """
        return self._implementation.size()

    def estimate_export_byte_size(self, format: Literal["csv"]) -> int:
        """Get an estimate of the dataframe's size in bytes when exported to the specified format.
        Please, note this is rather rough estimate, as it's done by sampling the dataframe
        and then scaling size of sample to the actual dataframe size.

        Args:
            format: The export format to estimate size for. Currently only 'csv' is supported.

        Returns:
            int: Estimated size of the dataframe in bytes when exported
        """

        sampled_df = self.sample(500)
        sampled_count = sampled_df.size()

        if sampled_count == 0:
            return 0

        if format == "csv":
            buffer = io.StringIO()
            sampled_df.to_csv(buffer)
            buffer.seek(0, io.SEEK_END)  # Seek to end so we can use .tell() to get size
            sample_size = buffer.tell()
        else:
            raise ValueError(
                'invalid value passed for "format" parameter, only "csv" is supported'
            )

        total_rows = self.size()
        estimated_size = round((sample_size / sampled_count) * total_rows)

        return estimated_size

    def sample(self, n: int, seed: Optional[int] = None) -> Self:
        """Randomly select ~n records from the dataframe. Please note that different backends don't guarantee
        you get exactly n records, it can be slightly less (but never more than n).

        Args:
            n: Number of records to sample
            seed: Optional random seed for reproducibility. If None, a seed will be generated by underlying native library.

        Returns:
            Self: New DataFrame instance containing the sampled records

        Raises:
            ValueError: If n is less than 1
        """
        return self.__class__(self._implementation.sample(n, seed))

    def sort(self, columns: List[Tuple[str, bool]]) -> Self:
        """Sort the dataframe by multiple columns.

        Args:
            columns: List of (column_name, ascending) tuples specifying sort order.
                   Columns are sorted in the order they appear in the list.
                   Secondary columns are used to break ties in primary columns.

        Returns:
            Self: New DataFrame instance containing the sorted data
        """
        valid_columns = [(col, asc) for col, asc in columns if col in self.column_names]
        if not valid_columns:
            return self
        return self.__class__(self._implementation.sort(valid_columns))

    def filter(self, *filters: Filter) -> Self:
        """Filter the dataframe using the provided filters.

        Args:
            *filters: One or more Filter objects specifying the filtering conditions.
                     If no filters are provided, returns a copy of the dataframe.

        Returns:
            Self: New DataFrame instance containing only the rows that match all filters,
                  or a copy of the original dataframe if no filters are provided
        """
        valid_filters = [f for f in filters if f.column in self.column_names]
        if not valid_filters:
            return self
        return self.__class__(self._implementation.filter(*valid_filters))

    def to_native(self) -> T:
        """Get the underlying native dataframe.

        Returns:
            NativeDF: The native dataframe instance
        """
        return self._implementation.to_native()

    def to_records(self, mode: Literal["json", "python"]) -> List[Dict[str, Any]]:
        """Convert the dataframe to a list of dictionaries.

        Args:
            mode: Conversion mode
                - 'python': Returns raw Python objects (may contain non-JSON-serializable values)
                - 'json': Returns JSON-serializable objects (e.g. casts objects to strings)

        Returns:
            List[Dict[str, Any]]: List of dictionaries, where each dictionary represents a row
            with column names used as keys
        """
        return self._implementation.to_records(mode)

    def to_csv(self, path_or_buf: Union[str, TextIO]) -> None:
        """Write the dataframe to a CSV file.

        Args:
            path_or_buf: File path or file-like object to write to.
        """
        self._implementation.to_csv(path_or_buf)

    def analyze_columns(
        self, color_scale_column_names: Optional[List[str]] = None
    ) -> List[ColumnsStatsRecord]:
        """Analyze columns in the dataframe and return statistics.

        Returns:
            List[ColumnsStatsRecord]: List of column statistics for each column in the dataframe
        """
        return self._implementation.analyze_columns(color_scale_column_names)

    def prepare_for_serialization(self) -> Self:
        """Prepare the dataframe for serialization.

        This method ensures the dataframe is in a format that can be safely serialized.
        It handles type conversions and other necessary transformations.

        Returns:
            Self: New DataFrame instance ready for serialization
        """
        return self.__class__(self._implementation.prepare_for_serialization())

    def get_columns_distinct_values(
        self, columns: List[Union[str, Column]], limit: Optional[int] = None
    ) -> Dict[str, List[Any]]:
        """Get distinct values from multiple columns.

        Note: The number of values returned depends on the underlying implementation.
        It is not guaranteed that you will get all distinct values from the columns. For large dataframes,
        implementations may limit the number of values returned.

        Args:
            columns: list of column names as strings or Column objects to get distinct values from
            limit: optional limit (per column). Note, this will be ignored if supplied value is bigger than implementation-specific upper limit

        Returns:
            Dict[str, List[Any]]: Dictionary mapping column names to lists of distinct values from each column
                (can be partial for big dataframes)

        Raises:
            KeyError: If any column name is not in the dataframe
        """
        column_names = []
        for column in columns:
            column_name = column.name if isinstance(column, Column) else column
            if column_name not in self.column_names:
                raise KeyError(f"Column '{column_name}' not found in dataframe")
            column_names.append(column_name)

        if limit is not None:
            result = self._implementation.get_columns_distinct_values(
                column_names, limit
            )
        else:
            result = self._implementation.get_columns_distinct_values(column_names)

        # Try sorting values for each column so they will be in deterministic order in the UI
        for column_name, values in result.items():
            try:
                values.sort()
            except Exception:
                # If failed we'll keep values list as is
                pass

        return result

    def get_column_distinct_values(
        self, column: Union[str, Column], limit: Optional[int] = None
    ) -> List[Any]:
        """Get distinct values from a column.

        Note: The number of values returned depends on the underlying implementation.
        It is not guaranteed that you will get all distinct values from the column. For large dataframes,
        implementations may limit the number of values returned.

        Args:
            column: either column name as string or Column object to get distinct values from
            limit: optional limit. Note, this will be ignored if supplied value is bigger than
                implementation-specific upper limit

        Returns:
            List[Any]: List of distinct values from the column (can be partial for big dataframes)

        Raises:
            KeyError: If column name is not in the dataframe
        """
        result = self.get_columns_distinct_values([column], limit)
        column_name = column.name if isinstance(column, Column) else column
        return result[column_name]


# These have to be separate functions (and not methods on DataFrame itself) because
# instance method type guards in Python aren't applied to 'self', but to first argument of the method,
# and thus can't be used to type guard generic type of class instance
def is_wrapped_pandas_dataframe(df: DataFrame) -> TypeGuard[DataFrame[PandasDF]]:
    return df.native_type == "pandas"


def is_wrapped_pyspark_dataframe(df: DataFrame) -> TypeGuard[DataFrame[PysparkDF]]:
    return df.native_type == "pyspark"
