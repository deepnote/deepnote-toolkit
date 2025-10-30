from typing import Any, Dict, List, Literal, Optional, Tuple

from wrapt import ObjectProxy

from .constants import DEEPNOTE_INDEX_COLUMN, DEFAULT_DATA_PREVIEW_RECORDS
from .dataframe import DataFrame
from .filters import Filter
from .types import ColumnsStatsRecord, NativeInputDF
from .utils import is_pandas_on_spark_dataframe, is_pyspark_dataframe


def should_wrap_into_data_preview(df: NativeInputDF):
    is_suitable_df = is_pyspark_dataframe(df) or is_pandas_on_spark_dataframe(df)
    # Since already wrapped DF will always pass checks above, we need to explicitly check that
    # DF is not wrapped yet
    is_already_wrapped = isinstance(df, DeepnoteDataFrameWithDataPreview)
    return is_suitable_df and not is_already_wrapped


class DataPreview:
    """A preview of a DataFrame that stores a local copy of a small number of records along with
    filters and sorting that was applied to the DataFrame before pulling the records.

    This class is used to speed up browsing results from distributed DataFrames by maintaining
    a local cache of filtered and sorted data. It's particularly useful for PySpark DataFrames
    where pagination can be expensive.

    NOTE: this class (with exception of .source field) is mutable, meaning associated sorting/filters/data
    can change in-place
    """

    def __init__(
        self,
        source: NativeInputDF,
        max_preview_size: int = DEFAULT_DATA_PREVIEW_RECORDS,
        mode: Literal["head", "sampled"] = "head",
    ):
        """
        Args:
            source: The original DataFrame (immutable)
            max_preview_size: Maximum number of records to pull for preview (default: DEFAULT_DATA_PREVIEW_RECORDS)

        Attributes:
            source: The original DataFrame (immutable)
            initialized: Whether the preview has been initialized with data
            _filters: List of active filters (private)
            _sort: List of (column_name, is_ascending) tuples for sorting (private)
            _data: List of records pulled from the source DataFrame (private)
            _total_size: Total number of records in the filtered DataFrame (private)
            _column_stats: List of column statistics (private)
            _color_scale_column_names: List of column names that have color scale formatting (private)
            _processed_df: Processed DataFrame with filters and sort applied (private)
            _max_preview_size: Maximum number of records to pull for preview (private)
            _mode: Mode for loading records: sequential or sampled (private)
        """
        self.initialized = False
        self.source = source
        self._max_preview_size = max_preview_size
        self._mode = mode
        self._filters = []
        self._sort_by = []
        self._data = []
        self._total_size = 0
        self._column_stats = None
        self._color_scale_column_names = None
        self._processed_df = None

    def _pull_data_preview(
        self, *, filters: List[Filter], sort_by: List[Tuple[str, bool]]
    ) -> Tuple[List[Dict[str, Any]], DataFrame]:
        """Pull a preview of records from a DataFrame using ocelots.

        This function applies filters and sorting to the DataFrame, then takes the first
        N records and converts them to a list of dictionaries.

        Args:
            filters: List of filters to apply
            sort_by: List of (column_name, is_ascending) tuples for sorting

        Returns:
            Tuple containing:
            - List of dictionaries containing the pulled records
            - Processed DataFrame with filters and sort applied
        """
        serialized_df = DataFrame.from_native(self.source).prepare_for_serialization()

        filtered_df = serialized_df.filter(*filters)
        sorted_df = filtered_df.sort(sort_by)

        if self._mode == "head":
            preview_df = sorted_df.paginate(0, self._max_preview_size)
        else:
            # We always sample with same seed to make data preview reproducible
            preview_df = sorted_df.sample(self._max_preview_size, 42)
        records = [
            {DEEPNOTE_INDEX_COLUMN: index, **record}
            for index, record in enumerate(preview_df.to_records(mode="json"))
        ]
        return records, sorted_df

    @property
    def data(self) -> List[Dict[str, Any]]:
        """Get the preview data. Raises RuntimeError if preview is not initialized."""
        if not self.initialized:
            raise RuntimeError("Cannot access data: preview is not initialized")
        return self._data

    @property
    def total_size(self) -> int:
        """Get the total number of records in the filtered DataFrame. Raises RuntimeError if preview is not initialized."""
        if not self.initialized:
            raise RuntimeError("Cannot access total size: preview is not initialized")
        return self._total_size

    def update_if_needed(
        self, *, filters: List[Filter], sort_by: List[Tuple[str, bool]]
    ) -> None:
        """Update the preview with new filters and sorting.

        This method updates the filters and sorting criteria and re-pulls the data
        from the source DataFrame.

        Args:
            filters: New list of filters to apply
            sort_by: New list of (column_name, is_ascending) tuples for sorting
        """
        if self.satisfies(filters=filters, sort_by=sort_by):
            return

        new_data, processed_df = self._pull_data_preview(
            filters=filters, sort_by=sort_by
        )
        self._data = new_data
        self._total_size = processed_df.size()
        self._filters = filters
        self._sort_by = sort_by
        self._processed_df = processed_df
        # Clear cached column stats since filters/sort changed
        self._column_stats = None
        self._color_scale_column_names = None
        self.initialized = True

    def satisfies(
        self,
        *,
        filters: List[Filter],
        sort_by: List[Tuple[str, bool]],
        df: Optional[NativeInputDF] = None,
    ) -> bool:
        """Check if this preview satisfies the given filters, sorting and optionally DataFrame.

        Args:
            filters: List of filters to check against cached filters
            sort_by: List of (column_name, is_ascending) tuples to check against cached sort
            df: Optional DataFrame to check against source DataFrame

        Returns:
            True if this preview satisfies all given criteria, False otherwise
        """
        if not self.initialized:
            return False

        if df is not None and df is not self.source:
            return False

        if len(filters) != len(self._filters):
            return False

        filters_set = set(filters)
        self_filters_set = set(self._filters)
        if filters_set != self_filters_set:
            return False

        if len(sort_by) != len(self._sort_by):
            return False

        for sort1, sort2 in zip(sort_by, self._sort_by):
            if sort1 != sort2:
                return False

        return True

    def page(self, page_index: int, page_size: int):
        """Get a page of data from the preview"""
        if not self.initialized:
            raise RuntimeError("Cannot paginate data: preview is not initialized")

        if page_index < 0 or page_size < 1:
            raise ValueError(
                "page_index must be non-negative and page_size must be positive"
            )

        total_pages = (len(self._data) + page_size - 1) // page_size
        normalized_page_index = (
            min(page_index, total_pages - 1) if total_pages > 0 else 0
        )
        start_idx = normalized_page_index * page_size
        end_idx = start_idx + page_size
        return self._data[start_idx:end_idx]

    def get_columns_stats(
        self, color_scale_column_names: Optional[List[str]] = None
    ) -> List[ColumnsStatsRecord]:
        """Get column statistics for the DataFrame.

        This method works as cached proxy to oc_df.analyze_columns method. If the preview is already
        initialized and the color_scale_column_names match the cached ones, it returns the cached
        statistics to avoid expensive recomputation. Otherwise, it computes new statistics.

        Args:
            color_scale_column_names: Optional list of column names that have color scale formatting

        Returns:
            List of column statistics for each column in the DataFrame
        """
        if not self.initialized:
            raise RuntimeError("Cannot get column stats: preview is not initialized")

        if (
            self._column_stats is None
            or self._color_scale_column_names != color_scale_column_names
        ):
            self._column_stats = [
                ColumnsStatsRecord(DEEPNOTE_INDEX_COLUMN, "int")
            ] + self._processed_df.analyze_columns(color_scale_column_names)
            self._color_scale_column_names = color_scale_column_names

        return self._column_stats

    def __repr__(self) -> str:
        """Return a string representation of the preview.

        This includes information about the source DataFrame type and number of records
        in the preview.
        """
        source_type = type(self.source).__name__
        record_count = len(self._data) if self.initialized else 0
        total_size = self._total_size if self.initialized else 0
        return f"DataPreview(source={source_type}, preview_records={record_count}, total_records={total_size})"


class DeepnoteDataFrameWithDataPreview(ObjectProxy):
    """This class wraps original DataFrame and adds lazily-initiated deepnote_data_preview property which
    holds data preview for this DataFrame. All other methods and properties access is forwarded to the original
    object. ObjectProxy also taps into original object's class hierarchy so isinstance continues to work.
    See wrapt module docs for more info.
    """

    # This needs to have default value (even though we always initialize it in the constructor) because
    # otherwise Pandas-on-Spark blocks us from accessing this property in its custom __getattr__ method
    deepnote_data_preview: DataPreview = None

    def __init__(self, wrapped_object):
        super(DeepnoteDataFrameWithDataPreview, self).__init__(wrapped_object)
        data_preview = DataPreview(wrapped_object)
        self.deepnote_data_preview = data_preview
