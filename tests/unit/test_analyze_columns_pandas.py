import unittest

import numpy as np
import pandas as pd
from trino.types import NamedRowTuple

from deepnote_toolkit.ocelots.constants import DEEPNOTE_INDEX_COLUMN
from deepnote_toolkit.ocelots.pandas.analyze import analyze_columns


class TestAnalyzeColumnsBasic(unittest.TestCase):
    def test_analyze_basic_numeric_column(self):
        df = pd.DataFrame({"col1": [1, 2, 3, 4, 5]})
        result = analyze_columns(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "col1")
        self.assertEqual(result[0].dtype, "int64")
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 5)
        self.assertEqual(result[0].stats.nan_count, 0)
        self.assertEqual(result[0].stats.min, "1")
        self.assertEqual(result[0].stats.max, "5")
        self.assertIsNotNone(result[0].stats.histogram)
        self.assertEqual(len(result[0].stats.histogram), 10)
        self.assertIsNone(result[0].stats.categories)

    def test_analyze_basic_string_column(self):
        df = pd.DataFrame({"col1": ["a", "b", "c", "a"]})
        result = analyze_columns(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "col1")
        self.assertEqual(result[0].dtype, "object")
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 3)
        self.assertEqual(result[0].stats.nan_count, 0)
        self.assertIsNone(result[0].stats.min)
        self.assertIsNone(result[0].stats.max)
        self.assertIsNone(result[0].stats.histogram)
        self.assertIsNotNone(result[0].stats.categories)
        self.assertEqual(len(result[0].stats.categories), 3)

    def test_analyze_float_column(self):
        df = pd.DataFrame({"col1": [1.5, 2.7, 3.2, 4.1, 5.9]})
        result = analyze_columns(df)

        self.assertEqual(result[0].dtype, "float64")
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.min, "1.5")
        self.assertEqual(result[0].stats.max, "5.9")

    def test_analyze_datetime_column(self):
        df = pd.DataFrame({"col1": pd.date_range("2020-01-01", periods=5)})
        result = analyze_columns(df)

        self.assertEqual(result[0].name, "col1")
        self.assertTrue("datetime64" in result[0].dtype)
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 5)
        self.assertIsNotNone(result[0].stats.min)
        self.assertIsNotNone(result[0].stats.max)
        self.assertIsNotNone(result[0].stats.histogram)

    def test_analyze_timedelta_column(self):
        df = pd.DataFrame({"col1": pd.to_timedelta([1, 2, 3, 4, 5], unit="D")})
        result = analyze_columns(df)

        self.assertTrue("timedelta64" in result[0].dtype)
        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[0].stats.histogram)

    def test_analyze_categorical_column(self):
        df = pd.DataFrame({"col1": pd.Categorical(["a", "b", "c", "a"])})
        result = analyze_columns(df)

        self.assertEqual(result[0].dtype, "category")
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 3)
        self.assertIsNotNone(result[0].stats.categories)

    def test_analyze_boolean_column(self):
        df = pd.DataFrame({"col1": [True, False, True, True]})
        result = analyze_columns(df)

        self.assertEqual(result[0].dtype, "bool")
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 2)

    def test_analyze_complex_column(self):
        df = pd.DataFrame({"col1": [1 + 2j, 3 + 4j, 5 + 6j, 1 + 2j]})
        result = analyze_columns(df)

        self.assertTrue("complex" in result[0].dtype)
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 3)
        self.assertEqual(result[0].stats.nan_count, 0)
        self.assertIsNone(result[0].stats.min)
        self.assertIsNone(result[0].stats.max)
        self.assertIsNone(result[0].stats.histogram)


class TestAnalyzeColumnsEdgeCases(unittest.TestCase):
    def test_empty_dataframe(self):
        df = pd.DataFrame()
        result = analyze_columns(df)

        self.assertEqual(len(result), 0)

    def test_dataframe_with_no_rows(self):
        df = pd.DataFrame({"col1": [], "col2": []})
        result = analyze_columns(df)

        self.assertEqual(len(result), 2)
        for col in result:
            self.assertIsNotNone(col.stats)
            self.assertEqual(col.stats.unique_count, 0)
            self.assertEqual(col.stats.nan_count, 0)

    def test_all_nan_column(self):
        df = pd.DataFrame({"col1": [np.nan, np.nan, np.nan]})
        result = analyze_columns(df)

        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 0)
        self.assertEqual(result[0].stats.nan_count, 3)
        self.assertIsNone(result[0].stats.min)
        self.assertIsNone(result[0].stats.max)

    def test_all_identical_values(self):
        df = pd.DataFrame({"col1": [5, 5, 5, 5, 5]})
        result = analyze_columns(df)

        self.assertEqual(result[0].stats.unique_count, 1)
        self.assertEqual(result[0].stats.min, "5")
        self.assertEqual(result[0].stats.max, "5")

    def test_infinite_values(self):
        df = pd.DataFrame({"col1": [0, 1, np.inf, -np.inf, 2]})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[0].stats.histogram)
        for bin in result[0].stats.histogram:
            self.assertTrue(np.isfinite(bin["bin_start"]))
            self.assertTrue(np.isfinite(bin["bin_end"]))

    def test_unhashable_types_dict(self):
        df = pd.DataFrame({"col1": [{}, {"a": 1}, {"b": 2}]})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 3)

    def test_unhashable_types_list(self):
        df = pd.DataFrame({"col1": [[], [1], [2, 3]]})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 3)

    def test_unhashable_types_set(self):
        df = pd.DataFrame({"col1": [set(), {1}, {2, 3}]})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 3)

    def test_duplicate_column_names(self):
        df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df.columns = ["col1", "col1"]
        result = analyze_columns(df)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "col1")
        self.assertEqual(result[1].name, "col1")
        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[1].stats)

    def test_deepnote_index_column_skipped(self):
        df = pd.DataFrame({DEEPNOTE_INDEX_COLUMN: [1, 2, 3], "col1": [4, 5, 6]})
        result = analyze_columns(df)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, DEEPNOTE_INDEX_COLUMN)
        self.assertIsNone(result[0].stats)
        self.assertEqual(result[1].name, "col1")
        self.assertIsNotNone(result[1].stats)

    def test_mixed_types_in_column(self):
        df = pd.DataFrame({"col1": [1, "a", 2.5, None]})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[0].stats.categories)

    def test_nan_in_numeric_column(self):
        df = pd.DataFrame({"col1": [1, 2, np.nan, 4, 5]})
        result = analyze_columns(df)

        self.assertEqual(result[0].stats.unique_count, 4)
        self.assertEqual(result[0].stats.nan_count, 1)
        self.assertIsNotNone(result[0].stats.histogram)

    def test_nat_in_datetime_column(self):
        df = pd.DataFrame(
            {"col1": [pd.Timestamp("2020-01-01"), pd.NaT, pd.Timestamp("2020-01-03")]}
        )
        result = analyze_columns(df)

        self.assertEqual(result[0].stats.unique_count, 2)
        self.assertEqual(result[0].stats.nan_count, 1)

    def test_complex_numbers_with_nan(self):
        df = pd.DataFrame({"col1": [1 + 2j, np.nan, 3 + 4j, np.nan, 1 + 2j]})
        result = analyze_columns(df)

        self.assertTrue("complex" in result[0].dtype)
        self.assertEqual(result[0].stats.unique_count, 2)
        self.assertEqual(result[0].stats.nan_count, 2)
        self.assertIsNone(result[0].stats.min)
        self.assertIsNone(result[0].stats.max)
        self.assertIsNone(result[0].stats.histogram)


class TestAnalyzeColumnsCategories(unittest.TestCase):
    def test_categories_three_or_less_unique_values(self):
        df = pd.DataFrame({"col1": ["a", "b", "c"]})
        result = analyze_columns(df)

        self.assertEqual(len(result[0].stats.categories), 3)
        category_names = [cat["name"] for cat in result[0].stats.categories]
        self.assertIn("a", category_names)
        self.assertIn("b", category_names)
        self.assertIn("c", category_names)

    def test_categories_more_than_three_unique_values(self):
        df = pd.DataFrame({"col1": ["a", "a", "b", "c", "d", "e"]})
        result = analyze_columns(df)

        self.assertEqual(len(result[0].stats.categories), 3)
        category_names = [cat["name"] for cat in result[0].stats.categories]
        self.assertIn("a", category_names)
        has_others = any("others" in cat["name"] for cat in result[0].stats.categories)
        self.assertTrue(has_others)

    def test_categories_with_missing_values(self):
        df = pd.DataFrame({"col1": ["a", "b", None, "a"]})
        result = analyze_columns(df)

        category_names = [cat["name"] for cat in result[0].stats.categories]
        self.assertIn("Missing", category_names)
        missing_cat = next(
            cat for cat in result[0].stats.categories if cat["name"] == "Missing"
        )
        self.assertEqual(missing_cat["count"], 1)

    def test_categories_counts(self):
        df = pd.DataFrame({"col1": ["a", "a", "a", "b", "b", "c"]})
        result = analyze_columns(df)

        categories = {cat["name"]: cat["count"] for cat in result[0].stats.categories}
        self.assertEqual(categories["a"], 3)
        self.assertEqual(categories["b"], 2)
        self.assertEqual(categories["c"], 1)

    def test_categories_with_many_unique_values_and_missing(self):
        df = pd.DataFrame({"col1": ["a", "a", "b", "c", "d", None]})
        result = analyze_columns(df)

        self.assertEqual(len(result[0].stats.categories), 3)
        category_names = [cat["name"] for cat in result[0].stats.categories]
        self.assertIn("Missing", category_names)
        has_others = any("others" in cat["name"] for cat in result[0].stats.categories)
        self.assertTrue(has_others)

    def test_categories_with_binary_data(self):
        df = pd.DataFrame(
            {"col1": [b"hello", b"world", b"hello", b"\x80\x81\x82", b"test"]}
        )
        result = analyze_columns(df)

        str_hello = "b'hello'"
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 4)
        self.assertIsNotNone(result[0].stats.categories)
        self.assertEqual(len(result[0].stats.categories), 3)
        category_names = [cat["name"] for cat in result[0].stats.categories]
        self.assertIn(str_hello, category_names)
        hello_count = next(
            cat["count"]
            for cat in result[0].stats.categories
            if cat["name"] == str_hello
        )
        self.assertEqual(hello_count, 2)
        has_others = any("others" in cat["name"] for cat in result[0].stats.categories)
        self.assertTrue(has_others)


class TestAnalyzeColumnsHistogram(unittest.TestCase):
    def test_histogram_ten_bins(self):
        df = pd.DataFrame({"col1": list(range(100))})
        result = analyze_columns(df)

        self.assertEqual(len(result[0].stats.histogram), 10)

    def test_histogram_bin_structure(self):
        df = pd.DataFrame({"col1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
        result = analyze_columns(df)

        for bin in result[0].stats.histogram:
            self.assertIn("bin_start", bin)
            self.assertIn("bin_end", bin)
            self.assertIn("count", bin)
            self.assertTrue(bin["bin_start"] < bin["bin_end"])

    def test_histogram_empty_after_dropna(self):
        df = pd.DataFrame({"col1": [np.nan, np.nan]})
        result = analyze_columns(df)

        self.assertIsNone(result[0].stats.histogram)

    def test_histogram_datetime_conversion(self):
        df = pd.DataFrame({"col1": pd.date_range("2020-01-01", periods=100)})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats.histogram)
        self.assertEqual(len(result[0].stats.histogram), 10)

    def test_histogram_timedelta_conversion(self):
        df = pd.DataFrame({"col1": pd.to_timedelta(range(100), unit="D")})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats.histogram)
        self.assertEqual(len(result[0].stats.histogram), 10)

    def test_histogram_large_integer_edge_case(self):
        """Test histogram with very large integers that might cause IndexError."""
        # Large integers that might cause edge cases in NumPy histogram
        large_values = [10**15, 10**15 + 1, 10**15 + 2]
        df = pd.DataFrame({"col1": large_values})
        result = analyze_columns(df)

        # Should handle gracefully without crashing
        self.assertIsNotNone(result[0].stats)

    def test_histogram_single_unique_value_int(self):
        """Test histogram with a single unique integer value (zero range)."""
        df = pd.DataFrame({"col1": [100] * 50})
        result = analyze_columns(df)

        # Should handle zero data range gracefully
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 1)

    def test_histogram_single_unique_value_float(self):
        """Test histogram with a single unique float value (zero range)."""
        df = pd.DataFrame({"col1": [3.14159] * 50})
        result = analyze_columns(df)

        # Should handle zero data range gracefully
        self.assertIsNotNone(result[0].stats)
        self.assertEqual(result[0].stats.unique_count, 1)


class TestAnalyzeColumnsPerformanceBudget(unittest.TestCase):
    def test_within_budget_all_columns_analyzed(self):
        df = pd.DataFrame({f"col{i}": list(range(100)) for i in range(10)})
        result = analyze_columns(df)

        self.assertEqual(len(result), 10)
        for col in result:
            self.assertIsNotNone(col.stats)
            self.assertIsNotNone(col.stats.histogram)

    def test_exceeds_budget_partial_analysis(self):
        df = pd.DataFrame({f"col{i}": list(range(10000)) for i in range(20)})
        result = analyze_columns(df)

        self.assertEqual(len(result), 20)
        columns_with_stats = sum(1 for col in result if col.stats is not None)
        columns_without_stats = sum(1 for col in result if col.stats is None)

        self.assertGreater(columns_with_stats, 0)
        self.assertGreater(columns_without_stats, 0)

    def test_budget_calculation_with_large_dataframe(self):
        df = pd.DataFrame({f"col{i}": list(range(200_000)) for i in range(10)})
        result = analyze_columns(df)

        self.assertEqual(len(result), 10)
        columns_with_full_stats = sum(
            1
            for col in result
            if col.stats is not None and col.stats.histogram is not None
        )
        self.assertEqual(columns_with_full_stats, 0)

    def test_budget_with_single_row(self):
        df = pd.DataFrame({f"col{i}": [i] for i in range(1000)})
        result = analyze_columns(df)

        self.assertEqual(len(result), 1000)
        for col in result:
            self.assertIsNotNone(col.stats)


class TestAnalyzeColumnsColorScale(unittest.TestCase):
    def test_color_scale_within_initial_budget(self):
        df = pd.DataFrame(
            {
                "col1": list(range(10)),
                "col2": list(range(10)),
            }
        )
        result = analyze_columns(df, color_scale_column_names=["col1"])

        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[0].stats.min)
        self.assertIsNotNone(result[0].stats.max)

    def test_color_scale_beyond_initial_budget(self):
        df = pd.DataFrame({f"col{i}": list(range(10_000)) for i in range(20)})
        result = analyze_columns(df, color_scale_column_names=["col15"])

        col15_index = next(i for i, col in enumerate(result) if col.name == "col15")
        self.assertIsNotNone(result[col15_index].stats)
        self.assertIsNotNone(result[col15_index].stats.min)
        self.assertIsNotNone(result[col15_index].stats.max)

    def test_color_scale_non_numeric_column_skipped(self):
        df = pd.DataFrame({f"col{i}": list(range(10_000)) for i in range(15)})
        df["col_string"] = ["a"] * 10_000
        result = analyze_columns(df, color_scale_column_names=["col_string"])

        col_string_index = next(
            i for i, col in enumerate(result) if col.name == "col_string"
        )
        self.assertIsNone(result[col_string_index].stats)

    def test_color_scale_multiple_columns(self):
        df = pd.DataFrame({f"col{i}": list(range(10_000)) for i in range(20)})
        result = analyze_columns(
            df, color_scale_column_names=["col15", "col16", "col17"]
        )

        for col_name in ["col15", "col16", "col17"]:
            col_index = next(i for i, col in enumerate(result) if col.name == col_name)
            self.assertIsNotNone(result[col_index].stats)

    def test_color_scale_exceeds_secondary_budget(self):
        df = pd.DataFrame({f"col{i}": list(range(100_000)) for i in range(200)})
        color_scale_columns = [f"col{i}" for i in range(10, 200)]
        result = analyze_columns(df, color_scale_column_names=color_scale_columns)

        columns_with_stats_beyond_initial = sum(
            1 for i, col in enumerate(result) if i >= 10 and col.stats is not None
        )
        self.assertGreater(columns_with_stats_beyond_initial, 0)
        self.assertLess(columns_with_stats_beyond_initial, len(color_scale_columns))

    def test_color_scale_empty_list(self):
        df = pd.DataFrame({"col1": list(range(10))})
        result_without = analyze_columns(df)
        result_with_empty = analyze_columns(df, color_scale_column_names=[])

        self.assertEqual(len(result_without), len(result_with_empty))

    def test_color_scale_none(self):
        df = pd.DataFrame({"col1": list(range(10))})
        result = analyze_columns(df, color_scale_column_names=None)

        self.assertIsNotNone(result[0].stats)


class TestAnalyzeColumnsMinMax(unittest.TestCase):
    def test_min_max_integers(self):
        df = pd.DataFrame({"col1": [1, 5, 2, 8, 3]})
        result = analyze_columns(df)

        self.assertEqual(result[0].stats.min, "1")
        self.assertEqual(result[0].stats.max, "8")

    def test_min_max_floats(self):
        df = pd.DataFrame({"col1": [1.5, 5.5, 2.2, 8.8, 3.3]})
        result = analyze_columns(df)

        self.assertEqual(result[0].stats.min, "1.5")
        self.assertEqual(result[0].stats.max, "8.8")

    def test_min_max_negative_numbers(self):
        df = pd.DataFrame({"col1": [-5, -1, 0, 1, 5]})
        result = analyze_columns(df)

        self.assertEqual(result[0].stats.min, "-5")
        self.assertEqual(result[0].stats.max, "5")

    def test_min_max_with_nan(self):
        df = pd.DataFrame({"col1": [1, np.nan, 5, np.nan, 3]})
        result = analyze_columns(df)

        self.assertEqual(result[0].stats.min, "1.0")
        self.assertEqual(result[0].stats.max, "5.0")

    def test_min_max_non_numeric(self):
        df = pd.DataFrame({"col1": ["a", "b", "c"]})
        result = analyze_columns(df)

        self.assertIsNone(result[0].stats.min)
        self.assertIsNone(result[0].stats.max)

    def test_min_max_datetime(self):
        df = pd.DataFrame({"col1": pd.date_range("2020-01-01", periods=5)})
        result = analyze_columns(df)

        self.assertIsNotNone(result[0].stats.min)
        self.assertIsNotNone(result[0].stats.max)
        self.assertTrue("2020-01-01" in result[0].stats.min)
        self.assertTrue("2020-01-05" in result[0].stats.max)

    def test_min_max_non_comparable_objects(self):
        """Test TypeError/ValueError handling."""
        # Create a column with non-comparable objects that pass numeric check
        # Using object dtype with mixed incomparable types
        df = pd.DataFrame(
            {"col1": pd.array([{"a": 1}, {"b": 2}, {"c": 3}], dtype=object)}
        )
        result = analyze_columns(df)

        # Should handle the error gracefully and return None for min/max
        self.assertIsNone(result[0].stats.min)
        self.assertIsNone(result[0].stats.max)

    def test_min_max_object_dtype_non_numeric(self):
        """Test explicit non-numeric object dtype."""
        df = pd.DataFrame({"col1": pd.array(["x", "y", "z"], dtype=object)})
        result = analyze_columns(df)

        # Non-numeric dtype should return None for min/max
        self.assertIsNone(result[0].stats.min)
        self.assertIsNone(result[0].stats.max)
        # Should have categories instead
        self.assertIsNotNone(result[0].stats.categories)


class TestAnalyzeColumnsMultipleTypes(unittest.TestCase):
    def test_multiple_numeric_and_string_columns(self):
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.5, 2.5, 3.5],
                "str_col": ["a", "b", "c"],
            }
        )
        result = analyze_columns(df)

        self.assertEqual(len(result), 3)
        self.assertIsNotNone(result[0].stats.histogram)
        self.assertIsNotNone(result[1].stats.histogram)
        self.assertIsNotNone(result[2].stats.categories)

    def test_mixed_column_types(self):
        df = pd.DataFrame(
            {
                "numeric": [1, 2, 3],
                "string": ["a", "b", "c"],
                "datetime": pd.date_range("2020-01-01", periods=3),
                "boolean": [True, False, True],
            }
        )
        result = analyze_columns(df)

        self.assertEqual(len(result), 4)
        for col in result:
            self.assertIsNotNone(col.stats)


class TestAnalyzeColumnsWithTrinoTypes(unittest.TestCase):
    def test_analyze_columns_with_named_row_tuple(self):
        row1 = NamedRowTuple(
            values=[1, "Alice"], names=["id", "name"], types=["integer", "varchar"]
        )
        row2 = NamedRowTuple(
            values=[2, "Bob"], names=["id", "name"], types=["integer", "varchar"]
        )
        row3 = NamedRowTuple(
            values=[1, "Alice"], names=["id", "name"], types=["integer", "varchar"]
        )

        np_array = np.empty(3, dtype=object)
        np_array[0] = row1
        np_array[1] = row2
        np_array[2] = row3

        df = pd.DataFrame({"col1": np_array})
        result = analyze_columns(df)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "col1")
        self.assertEqual(result[0].dtype, "object")
        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[0].stats.categories)
        self.assertIsInstance(result[0].stats.categories, list)
        self.assertGreater(len(result[0].stats.categories), 0)
        for category in result[0].stats.categories:
            self.assertIn("name", category)
            self.assertIn("count", category)

    def test_analyze_columns_with_named_row_tuple_and_missing_values(self):
        row1 = NamedRowTuple(
            values=[1, "Alice"], names=["id", "name"], types=["integer", "varchar"]
        )
        row2 = NamedRowTuple(
            values=[2, "Bob"], names=["id", "name"], types=["integer", "varchar"]
        )

        np_array = np.empty(4, dtype=object)
        np_array[0] = row1
        np_array[1] = row2
        np_array[2] = None
        np_array[3] = row1

        df = pd.DataFrame({"col1": np_array})
        result = analyze_columns(df)

        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[0].stats.categories)

        category_names = [cat["name"] for cat in result[0].stats.categories]
        self.assertIn("Missing", category_names)

        missing_cat = next(
            cat for cat in result[0].stats.categories if cat["name"] == "Missing"
        )
        self.assertEqual(missing_cat["count"], 1)

    def test_analyze_columns_with_many_named_row_tuples(self):
        np_array = np.empty(20, dtype=object)
        for i in range(10):
            row = NamedRowTuple(
                values=[i, f"User{i}"],
                names=["id", "name"],
                types=["integer", "varchar"],
            )
            np_array[i * 2] = row
            np_array[i * 2 + 1] = row

        df = pd.DataFrame({"col1": np_array})
        result = analyze_columns(df)

        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result[0].stats)
        self.assertIsNotNone(result[0].stats.categories)
        self.assertGreaterEqual(len(result[0].stats.categories), 1)
        self.assertLessEqual(len(result[0].stats.categories), 3)

        has_others = any("others" in cat["name"] for cat in result[0].stats.categories)
        self.assertTrue(has_others)


if __name__ == "__main__":
    unittest.main()
