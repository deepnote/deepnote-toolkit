import unittest
from unittest.mock import Mock, patch

import pandas as pd

from deepnote_toolkit.ocelots.data_preview import (
    DataPreview,
    DeepnoteDataFrameWithDataPreview,
)
from deepnote_toolkit.ocelots.filters import Filter, FilterOperator
from deepnote_toolkit.ocelots.types import ColumnsStatsRecord

from .helpers.testing_dataframes import testing_dataframes


class TestDeepnoteDataFrameWithDataPreview(unittest.TestCase):
    def setUp(self):
        self.df = testing_dataframes["basic"]
        self.wrapped_df = DeepnoteDataFrameWithDataPreview(self.df)

    def test_property_access(self):
        """Test that property access is forwarded to the original object."""
        self.assertEqual(self.wrapped_df.shape, (2, 2))

    def test_method_call(self):
        """Test that method calls are forwarded to the original object."""
        result = self.wrapped_df.head(1)
        expected = self.df.head(1)
        pd.testing.assert_frame_equal(result, expected)

    def test_deepnote_data_preview_property(self):
        """Test that deepnote_data_preview property returns a DataPreview instance."""
        preview = self.wrapped_df.deepnote_data_preview
        self.assertIsInstance(preview, DataPreview)
        self.assertIs(preview.source, self.df)

    def test_deepnote_data_preview_caching(self):
        """Test that deepnote_data_preview property caches the DataPreview instance."""
        preview1 = self.wrapped_df.deepnote_data_preview
        preview2 = self.wrapped_df.deepnote_data_preview
        self.assertIs(preview1, preview2)


class TestDataPreview(unittest.TestCase):
    def setUp(self):
        self.df = testing_dataframes["basic"]

    def test_uninitialized_preview_properties(self):
        """Test that accessing public properties raises RuntimeError when preview is not initialized."""
        preview = DataPreview(self.df)

        with self.assertRaises(RuntimeError):
            _ = preview.data

        with self.assertRaises(RuntimeError):
            _ = preview.total_size

        with self.assertRaises(RuntimeError):
            _ = preview.get_columns_stats()

        with self.assertRaises(RuntimeError):
            _ = preview.page(0, 10)

    def test_update_if_needed(self):
        """Test update_if_needed method with new filters and sorting."""
        preview = DataPreview(self.df)
        filters = [Filter("col1", FilterOperator.GREATER_THAN_OR_EQUAL, [5])]
        sort_by = [("col1", True)]
        mock_data = [{"col1": 5}, {"col1": 7}]
        mock_processed_df = Mock()
        mock_processed_df.size.return_value = 10

        with patch.object(preview, "_pull_data_preview") as mock_pull:
            mock_pull.return_value = (mock_data, mock_processed_df)
            preview.update_if_needed(filters=filters, sort_by=sort_by)

        self.assertTrue(preview.initialized)
        self.assertEqual(preview._filters, filters)
        self.assertEqual(preview._sort_by, sort_by)
        self.assertEqual(preview._data, mock_data)
        self.assertEqual(preview._total_size, 10)
        self.assertEqual(preview._processed_df, mock_processed_df)
        self.assertIsNone(preview._column_stats)
        self.assertIsNone(preview._color_scale_column_names)

    def test_satisfies_same_filters_and_sort(self):
        """Test satisfies method with matching filters and sorting."""
        preview = DataPreview(self.df)
        filters = [Filter("col1", FilterOperator.IS_EQUAL, [1])]
        sort_by = [("col1", True)]

        with patch.object(preview, "_pull_data_preview") as mock_pull:
            mock_pull.return_value = ([], Mock())
            preview.update_if_needed(filters=filters, sort_by=sort_by)

        self.assertTrue(preview.satisfies(filters=filters, sort_by=sort_by))

    def test_satisfies_different_filters(self):
        """Test satisfies method with different filters."""
        preview = DataPreview(self.df)
        filters1 = [Filter("col1", FilterOperator.IS_EQUAL, [1])]
        filters2 = [Filter("col1", FilterOperator.IS_EQUAL, [2])]
        sort_by = [("col1", True)]

        with patch.object(preview, "_pull_data_preview") as mock_pull:
            mock_pull.return_value = ([], Mock())
            preview.update_if_needed(filters=filters1, sort_by=sort_by)

        self.assertFalse(preview.satisfies(filters=filters2, sort_by=sort_by))

    def test_page(self):
        """Test page method for pagination."""
        preview = DataPreview(self.df)
        mock_data = [{"col1": i} for i in range(10)]
        with patch.object(preview, "_pull_data_preview") as mock_pull:
            mock_pull.return_value = (mock_data, Mock())
            preview.update_if_needed(filters=[], sort_by=[])

        self.assertEqual(preview.page(0, 3), mock_data[0:3])
        self.assertEqual(preview.page(1, 3), mock_data[3:6])
        self.assertEqual(preview.page(3, 3), mock_data[9:])
        # Out of bounds should return last page
        self.assertEqual(preview.page(42, 3), mock_data[9:])

    def test_page_invalid_inputs(self):
        """Test page method with invalid inputs."""
        preview = DataPreview(self.df)
        with patch.object(preview, "_pull_data_preview") as mock_pull:
            mock_pull.return_value = ([], Mock())
            preview.update_if_needed(filters=[], sort_by=[])

        with self.assertRaises(ValueError):
            preview.page(-1, 3)
        with self.assertRaises(ValueError):
            preview.page(0, 0)

    def test_get_columns_stats(self):
        """Test get_columns_stats method."""
        preview = DataPreview(self.df)
        mock_analyze_result = [ColumnsStatsRecord(name="col1", dtype="int", stats=None)]
        # _deepnote_index_column is not returned by oc_df.analyze_column() but in get_columns_stats
        expected_stats = [
            ColumnsStatsRecord(name="_deepnote_index_column", dtype="int", stats=None),
            *mock_analyze_result,
        ]
        mock_processed_df = Mock()
        mock_processed_df.analyze_columns.return_value = mock_analyze_result

        with patch.object(preview, "_pull_data_preview") as mock_pull:
            mock_pull.return_value = ([], mock_processed_df)
            preview.update_if_needed(filters=[], sort_by=[])

        # Test first call
        stats = preview.get_columns_stats()
        self.assertEqual(stats, expected_stats)
        mock_processed_df.analyze_columns.assert_called_once_with(None)

        # Test cached call
        stats = preview.get_columns_stats()
        self.assertEqual(stats, expected_stats)
        # Should not call analyze_columns again
        self.assertEqual(mock_processed_df.analyze_columns.call_count, 1)

        # Test with color scale columns
        color_scale_columns = ["col1"]
        stats = preview.get_columns_stats(color_scale_columns)
        self.assertEqual(stats, expected_stats)
        # Should call analyze_columns again with new color scale columns
        self.assertEqual(mock_processed_df.analyze_columns.call_count, 2)
        mock_processed_df.analyze_columns.assert_called_with(color_scale_columns)
