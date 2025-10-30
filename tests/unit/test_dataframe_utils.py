import json
import unittest
from unittest.mock import MagicMock

from ipykernel.jsonutil import json_clean

from deepnote_toolkit.dataframe_utils import _describe_dataframe, add_formatters

from .helpers.testing_dataframes import testing_dataframes


def describe_and_json_clean(df, browse_spec=None):
    result = _describe_dataframe(df, browse_spec)
    # We run these 2 functions on the mimebundle to check if ipykernel will be able to serialize the result into the notebook
    # json, which is necessary for our whole mechanism to work. It raises an exception for e.g. timedelta types
    cleaned_for_json = json_clean(result)
    json.dumps(cleaned_for_json)
    return result


class TestDataframeDescribe(unittest.TestCase):
    def setUp(self) -> None:
        self.spec_with_format_rules = '{"cellFormattingRules":[{"type":"colorScale","columnNames":["col1","col2"],"columnSelectionMode":"all"}]}'
        self.spec_with_invalid_format_rules = (
            '{"cellFormattingRules":[{"type":"colorScale","columnNames":"not_a_list"}]}'
        )

    def test_dataframe(self):
        df = testing_dataframes["basic"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["column_count"], 2)
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(result["columns"][0]["name"], "col1")

    def test_dataframe_sort(self):
        df = testing_dataframes["basic2"]
        result = describe_and_json_clean(df.sort_values("col1"))
        self.assertEqual(result["rows"][0]["col1"], 1)
        self.assertEqual(result["rows"][1]["col1"], 2)
        self.assertEqual(result["rows"][2]["col1"], 3)
        # _deepnote_index_column is hidden on frontend. See variable_explorer_helpers for more info.
        self.assertEqual(result["rows"][0]["_deepnote_index_column"], 1)

    def test_categorical_columns(self):
        df = testing_dataframes["categorical_columns"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 4)
        self.assertEqual(result["column_count"], 5)
        self.assertEqual(len(result["rows"]), 4)
        self.assertDictEqual(
            result["columns"][0],
            {
                "name": "cat1",
                "dtype": "object",
                "stats": {
                    "unique_count": 4,
                    "nan_count": 0,
                    "histogram": None,
                    "max": None,
                    "min": None,
                    "categories": [
                        {"name": "a", "count": 1},
                        {"name": "b", "count": 1},
                        {"name": "2 others", "count": 2},
                    ],
                },
            },
        )
        self.assertEqual(
            result["columns"][1]["stats"]["categories"],
            [
                {"name": "a", "count": 1},
                {"name": "2 others", "count": 2},
                {"name": "Missing", "count": 1},
            ],
        )

    def test_numerical_columns(self):
        df = testing_dataframes["numerical_columns"]
        result = describe_and_json_clean(df, self.spec_with_format_rules)
        self.assertEqual(result["row_count"], 4)
        self.assertEqual(result["column_count"], 4)
        self.assertEqual(len(result["rows"]), 4)
        self.assertEqual(result["columns"][0]["name"], "col1")

        self.assertEqual(result["columns"][0]["stats"]["min"], "1")
        self.assertEqual(result["columns"][0]["stats"]["max"], "4")
        self.assertEqual(result["columns"][1]["stats"]["min"], "1.0")
        self.assertEqual(result["columns"][1]["stats"]["max"], "4.0")
        # Third column has complex number in it which makes its type to be "object" (i.e. non numerical)
        self.assertIsNone(result["columns"][2]["stats"]["min"])
        self.assertIsNone(result["columns"][2]["stats"]["max"])
        self.assertIsNotNone(result["columns"][0]["stats"]["histogram"])
        self.assertIsNotNone(result["columns"][1]["stats"]["histogram"])
        self.assertIsNone(result["columns"][2]["stats"]["histogram"])

    def test_multi_level_columns(self):
        df = testing_dataframes["multi_level_columns"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["columns"][0]["name"], "col1 col0")
        self.assertEqual(result["columns"][1]["name"], "col2 col0")
        self.assertEqual(result["columns"][2]["name"], "col3 col0")

    def test_dataframe_with_many_rows(self):
        df = testing_dataframes["many_rows_100k"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 100000)
        self.assertEqual(result["column_count"], 3)
        self.assertEqual(len(result["rows"]), 10)
        self.assertTrue("stats" in result["columns"][0])
        self.assertTrue("stats" not in result["columns"][1])

        df = testing_dataframes["many_rows_200k"]
        result = describe_and_json_clean(df)
        self.assertTrue("stats" not in result["columns"][0])

    def test_dataframe_with_many_rows_and_color_scale_format_rules(self):
        df = testing_dataframes["many_rows_200k"]
        result = describe_and_json_clean(df, self.spec_with_format_rules)
        self.assertTrue("stats" in result["columns"][0])
        self.assertTrue("stats" in result["columns"][1])
        self.assertTrue("min" in result["columns"][1]["stats"])
        self.assertTrue("max" in result["columns"][1]["stats"])
        self.assertEqual(result["columns"][1]["stats"]["min"], "0")
        self.assertEqual(result["columns"][1]["stats"]["max"], "199999")

    def test_dataframe_with_many_rows_and_invalid_color_scale_format_rules(self):
        df = testing_dataframes["many_rows_200k"]
        result = describe_and_json_clean(df, self.spec_with_invalid_format_rules)
        self.assertTrue("stats" not in result["columns"][0])
        self.assertTrue("stats" not in result["columns"][1])
        self.assertTrue("stats" not in result["columns"][2])

    def test_dataframe_with_many_columns(self):
        df = testing_dataframes["many_columns"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(len(result["columns"]), 501)  # includes _deepnote_index_column
        self.assertEqual(len(result["rows"][0]), 501)  # includes _deepnote_index_column
        self.assertTrue("_deepnote_index_column" in result["rows"][0])
        self.assertEqual(result["column_count"], 10000)
        self.assertEqual(len(result["rows"]), 1)

    def test_no_rows(self):
        df = testing_dataframes["no_rows"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 0)
        self.assertEqual(result["column_count"], 2)

    def test_no_columns(self):
        df = testing_dataframes["no_columns"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 0)
        self.assertEqual(result["column_count"], 0)

    def test_duplicate_columns(self):
        df = testing_dataframes["duplicate_columns"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 4)
        self.assertEqual(result["column_count"], 2)
        self.assertEqual(result["columns"][0]["name"], "col1")
        self.assertEqual(result["columns"][1]["name"], "col1.1")

    def test_nans(self):
        df = testing_dataframes["nans"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 3)
        self.assertEqual(result["column_count"], 1)
        self.assertEqual(
            result["columns"][0]["stats"],
            {
                "unique_count": 0,
                "nan_count": 3,
                "histogram": None,
                "max": None,
                "min": None,
                "categories": [
                    {"name": "Missing", "count": 3},
                ],
            },
        )

    def test_large_numbers(self):
        df = testing_dataframes["large_numbers"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(result["columns"][0]["stats"]["min"], str(2**53))
        self.assertEqual(result["columns"][0]["stats"]["max"], str(2**53))

    def test_infinity(self):
        df = testing_dataframes["infinity"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["columns"][0]["stats"]["unique_count"], 3)
        self.assertNotEqual(result["columns"][0]["stats"]["histogram"], None)
        self.assertEqual(result["columns"][0]["stats"]["min"], "-inf")
        self.assertEqual(result["columns"][0]["stats"]["max"], "inf")

    def test_nat(self):
        df = testing_dataframes["nat"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["columns"][0]["stats"]["min"], "2005-02-25 00:00:00")
        self.assertEqual(result["columns"][0]["stats"]["max"], "2005-02-25 00:00:00")

    def test_nan(self):
        df = testing_dataframes["nan"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 3)

    def test_int64_nan(self):
        df = testing_dataframes["int64_nan"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 3)

    def test_dict_column(self):
        df = testing_dataframes["dict_column"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["columns"][0]["stats"]["unique_count"], 3)

    def test_list_column(self):
        df = testing_dataframes["list_column"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["columns"][0]["stats"]["unique_count"], 3)

    def test_set_column(self):
        df = testing_dataframes["set_column"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["columns"][0]["stats"]["unique_count"], 2)

    def test_datetime(self):
        df1 = testing_dataframes["datetime"]
        result1 = describe_and_json_clean(df1)
        self.assertEqual(result1["row_count"], 2)
        self.assertEqual(result1["column_count"], 3)

    def test_date_range_index(self):
        df2 = testing_dataframes["date_range_index"]
        result2 = describe_and_json_clean(df2)
        self.assertEqual(result2["row_count"], 2)
        self.assertEqual(result2["column_count"], 3)

    def test_datetime_numpy(self):
        df3 = testing_dataframes["datetime_numpy"]
        result3 = describe_and_json_clean(df3)
        self.assertEqual(result3["row_count"], 2)
        self.assertEqual(result3["column_count"], 2)

    def test_timedelta(self):
        df1 = testing_dataframes["timedelta"]
        result1 = describe_and_json_clean(df1)
        self.assertEqual(result1["row_count"], 2)
        self.assertEqual(result1["column_count"], 4)

    def test_category_dtype(self):
        df = testing_dataframes["category_dtype"]
        result = describe_and_json_clean(df)
        self.assertEqual(result["row_count"], 4)
        self.assertEqual(result["column_count"], 2)

    def test_geopandas(self):
        if "geopandas" not in testing_dataframes:
            self.skipTest("geopandas is not available")

        df_geo = testing_dataframes["geopandas"]

        result1 = describe_and_json_clean(df_geo)
        self.assertEqual(result1["row_count"], len(df_geo))

    def test_object_to_string_casting(self):
        df1 = testing_dataframes["datetime"]
        result1 = describe_and_json_clean(df1)
        self.assertTrue(type(result1["rows"][0]["col2"]) is str)
        self.assertTrue(type(result1["rows"][1]["col2"]) is str)
        self.assertEqual(result1["columns"][0]["dtype"], "int64")
        self.assertEqual(result1["columns"][1]["dtype"], "object")
        self.assertEqual(result1["columns"][1]["dtype"], "object")

    def test_object_to_string_casting_with_empty_first_row(self):
        df1 = testing_dataframes["datetime_with_empty_first_row"]
        result1 = describe_and_json_clean(df1)
        self.assertTrue(type(result1["rows"][0]["col2"]) is str)
        self.assertTrue(type(result1["rows"][1]["col2"]) is str)
        self.assertEqual(result1["columns"][0]["dtype"], "float64")
        self.assertEqual(result1["columns"][1]["dtype"], "object")
        self.assertEqual(result1["columns"][1]["dtype"], "object")

    def test_long_string(self):
        df1 = testing_dataframes["long_string"]
        result1 = describe_and_json_clean(df1)
        self.assertTrue(len(result1["rows"][0]["col1"]) < 1001)

    def test_long_dict(self):
        df1 = testing_dataframes["long_dict"]
        result1 = describe_and_json_clean(df1)
        self.assertTrue(len(result1["rows"][0]["col1"]) < 1001)
        self.assertTrue(type(result1["rows"][0]["col1"]) is str)

    def test_period_index(self):
        df1 = testing_dataframes["period_index"]
        result1 = describe_and_json_clean(df1)
        self.assertEqual(result1["rows"][0]["2021-06"], 1)


class TestDataframeFormatter(unittest.TestCase):
    """Test that the dataframe formatter returns metadata in the correct location."""

    def test_formatter_returns_tuple_with_data_and_metadata(self):
        """
        Test that the dataframe formatter returns a tuple (data, metadata) where:
        - data contains only MIME types
        - metadata is a separate dictionary
        This ensures the output structure matches Jupyter's protocol.
        """
        # Create a mock IPython instance with a mimebundle formatter
        mock_ipython = MagicMock()
        mock_formatter = MagicMock()
        mock_ipython.display_formatter.mimebundle_formatter = mock_formatter

        # Mock get_ipython to return our mock
        import deepnote_toolkit.dataframe_utils as df_utils

        original_get_ipython = df_utils.get_ipython
        df_utils.get_ipython = lambda: mock_ipython

        try:
            # Call add_formatters which registers the formatter
            add_formatters()

            # Get the registered formatter function for pd.DataFrame
            formatter_call = mock_formatter.for_type.call_args
            self.assertIsNotNone(formatter_call)

            # Extract the formatter function (second argument)
            import pandas as pd

            self.assertEqual(formatter_call[0][0], pd.DataFrame)
            formatter_func = formatter_call[0][1]

            # Test the formatter with a simple dataframe
            df = testing_dataframes["basic"]
            result = formatter_func(df)

            # Verify it returns a tuple
            self.assertIsInstance(
                result,
                tuple,
                "Formatter should return a tuple (data, metadata), not a dict",
            )
            self.assertEqual(len(result), 2, "Formatter should return (data, metadata)")

            data_dict, metadata_dict = result

            # Verify data_dict contains only MIME types
            self.assertIsInstance(data_dict, dict)
            self.assertIn(
                "application/vnd.deepnote.dataframe.v3+json",
                data_dict,
                "Data dict should contain the dataframe MIME type",
            )

            # Verify metadata is separate and contains table_state_spec
            self.assertIsInstance(metadata_dict, dict)
            self.assertIn(
                "table_state_spec",
                metadata_dict,
                "Metadata dict should contain table_state_spec",
            )

            # Verify that 'metadata' is NOT a key in the data_dict
            self.assertNotIn(
                "metadata",
                data_dict,
                "Metadata should NOT be inside the data dict as a MIME type",
            )

        finally:
            # Restore original get_ipython
            df_utils.get_ipython = original_get_ipython
