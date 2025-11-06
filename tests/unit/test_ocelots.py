import base64
import io
import unittest
import warnings
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from pyspark.sql import SparkSession

from deepnote_toolkit.ocelots.constants import DEEPNOTE_INDEX_COLUMN
from deepnote_toolkit.ocelots.dataframe import DataFrame
from deepnote_toolkit.ocelots.filters import Filter, FilterOperator

from .helpers.testing_dataframes import testing_dataframes

spark = (
    SparkSession.builder.master("local")  # type: ignore
    .appName("Toolkit")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

# Store current time to use in tests
CURRENT_TIME = datetime.now()
YESTERDAY = CURRENT_TIME - timedelta(days=1)
TOMORROW = CURRENT_TIME + timedelta(days=1)
DAY_AFTER_TOMORROW = CURRENT_TIME + timedelta(days=2)


def create_spark_df(pandas_df: pd.DataFrame, schema=None):
    with warnings.catch_warnings():
        # PySpark is noisy about not liking version of installed Pandas
        warnings.filterwarnings("ignore")
        return spark.createDataFrame(pandas_df, schema)


def _test_with_all_backends(
    test_df: Optional[Union[List[Dict[str, Any]], pd.DataFrame, Dict[str, Any]]] = None,
    *,
    initialize_ocelots_dataframe=True,
    pyspark_schema=None,
):  # noqa: E251
    """Decorator to run a test for all supported DataFrame implementations.

    Args:
        test_data: List of dictionaries representing the test data or Pandas DataFrame.
                  Each dictionary represents a row with column names as keys.
                  Defaults to basic testing DataFrame if not provided.
    """
    import sys

    if not isinstance(test_df, pd.DataFrame):
        if test_df is None:
            test_df = testing_dataframes["basic"]
        else:
            test_df = pd.DataFrame(test_df)

    def decorator(test_func: Callable):
        def wrapper(self):
            assert test_df is not None
            pandas_df = test_df.copy()
            assert isinstance(pandas_df, pd.DataFrame)
            assert isinstance(test_df, pd.DataFrame)
            with self.subTest(implementation="pandas"):
                test_func(
                    self,
                    (
                        DataFrame.from_native(pandas_df)
                        if initialize_ocelots_dataframe
                        else pandas_df
                    ),
                )

            # Skip PySpark tests for Python 3.12 since PySpark doesn't support it yet
            if sys.version_info < (3, 12):
                pyspark_df = create_spark_df(test_df)
                with self.subTest(implementation="pyspark"):
                    test_func(
                        self,
                        (
                            DataFrame.from_native(pyspark_df)
                            if initialize_ocelots_dataframe
                            else pyspark_df
                        ),
                    )
            else:
                self.skipTest("PySpark does not yet support Python 3.12")

            pyspark_df = create_spark_df(test_df, pyspark_schema)
            with self.subTest(implementation="pyspark"):
                test_func(
                    self,
                    (
                        DataFrame.from_native(pyspark_df)
                        if initialize_ocelots_dataframe
                        else pyspark_df
                    ),
                )

        return wrapper

    return decorator


class TestDataFrame(unittest.TestCase):
    @_test_with_all_backends(initialize_ocelots_dataframe=False)
    def test_is_supported(self, df):
        """Test DataFrame.is_supported method."""
        self.assertTrue(DataFrame.is_supported(df))

    def test_native_type_pandas(self):
        ocelots_df = DataFrame.from_native(testing_dataframes["basic"])
        self.assertEqual(ocelots_df.native_type, "pandas")

    def test_native_type_pyspark(self):
        import sys

        if sys.version_info >= (3, 12):
            self.skipTest("PySpark does not yet support Python 3.12")
        pyspark_df = create_spark_df(testing_dataframes["basic"])
        ocelots_df = DataFrame.from_native(pyspark_df)
        self.assertEqual(ocelots_df.native_type, "pyspark")

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_columns(self, df: DataFrame):
        col_names = [col.name for col in df.columns]
        self.assertEqual(col_names, ["col1", "col2", "col3"])

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_paginate(self, df: DataFrame):
        data = df.paginate(10, 100).to_records("python")
        self.assertEqual(len(data), 100)
        self.assertEqual(
            data[0],
            {
                "col1": 1000,
                "col2": 1000,
                "col3": 1000,
            },
        )

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_size(self, df: DataFrame):
        self.assertEqual(df.size(), 10_000)

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_sample(self, df: DataFrame):
        records = df.sample(100).to_records("python")
        self.assertLessEqual(len(records), 100)

    def test_to_native_pandas(self):
        df = DataFrame.from_native(testing_dataframes["basic"])
        self.assertIs(df.to_native(), testing_dataframes["basic"])
        self.assertIsInstance(df.sort([("col1", True)]).to_native(), pd.DataFrame)

    def test_to_native_spark(self):
        import sys

        if sys.version_info >= (3, 12):
            self.skipTest("PySpark does not yet support Python 3.12")
        spark_df = create_spark_df(testing_dataframes["basic"])
        df = DataFrame.from_native(spark_df)
        self.assertIs(df.to_native(), spark_df)
        self.assertIsInstance(df.sort([("col1", True)]).to_native(), spark_df.__class__)

    @_test_with_all_backends()
    def test_to_records(self, df: DataFrame):
        self.assertEqual(
            df.to_records("python"),
            [
                {"col1": 1, "col2": 3},
                {"col1": 2, "col2": 4},
            ],
        )

    @_test_with_all_backends(
        testing_dataframes["non_serializable_values"]["data"],
        pyspark_schema=testing_dataframes["non_serializable_values"]["pyspark_schema"],
    )
    def test_to_records_json(self, df: DataFrame):
        first_row = df.to_records(mode="json")[0]
        self.assertEqual(first_row["list"], "[1, 2, 3]")
        self.assertEqual(first_row["datetime"], "2023-01-01 12:00:00")

        expected_hello = base64.b64encode(b"hello").decode("ascii")
        self.assertEqual(first_row["binary"], expected_hello)

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_analyze_columns(self, df: DataFrame):
        summary = df.analyze_columns(["col1"])
        self.assertEqual(len(summary), 3)
        self.assertEqual(summary[0].name, "col1")

        # Only min/max is implemented for PySpark. For more comprehensive
        # analyzer tests for pandas see test_analyze_columns_pandas.py
        assert summary[0].stats is not None
        self.assertEqual(summary[0].stats.min, "0")
        self.assertEqual(summary[0].stats.max, "9999")

    @_test_with_all_backends(testing_dataframes["column_distinct_values"])
    def test_get_column_distinct_values(self, df: DataFrame):
        self.assertEqual(df.get_column_distinct_values("col1"), [2, 4, 42, 77])
        self.assertEqual(df.get_column_distinct_values("col2"), ["a", "b", "c"])

    def test_get_column_distinct_values_mixed_content(self):
        # Spark doesn't allow mixed content, so we test only with Pandas
        df = DataFrame.from_native(testing_dataframes["column_distinct_values"])
        self.assertEqual(
            df.get_column_distinct_values("col3"), [2, 1, "wow", "test"]
        )  # Mixed content can't be sorted

    @_test_with_all_backends(testing_dataframes["many_rows_100k"])
    def test_estimate_export_byte_size_csv(self, df: DataFrame):
        """Test DataFrame.estimate_export_byte_size method for CSV format."""
        estimated_size = df.estimate_export_byte_size("csv")

        correct_size = 1_766_685
        delta = correct_size * 0.1  # Allow 10% variation
        self.assertAlmostEqual(estimated_size, correct_size, delta=delta)

    @_test_with_all_backends(testing_dataframes["basic"])
    def test_to_csv(self, df: DataFrame):
        """Test DataFrame.to_csv method."""
        with io.StringIO() as buffer:
            df.to_csv(buffer)
            buffer.seek(0)
            content = buffer.getvalue()

            # Check header
            self.assertIn("col1,col2", content)

            # Check data
            self.assertIn("1,3", content)
            self.assertIn("2,4", content)

            # Check no index column
            self.assertNotIn(DEEPNOTE_INDEX_COLUMN, content)


class TestDataFrameSorting(unittest.TestCase):
    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_sort(self, df: DataFrame):
        records = df.sort([("col1", False)]).to_records("python")
        self.assertEqual(records[0]["col1"], 9_999)

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_sort_missing_column(self, df: DataFrame):
        records = df.sort([("missing_col", False)]).to_records("python")
        self.assertEqual(records[0]["col1"], 0)

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_sort_empty_sort_list(self, df: DataFrame):
        records = df.sort([]).to_records("python")
        self.assertEqual(records[0]["col1"], 0)

    @_test_with_all_backends(testing_dataframes["multi_columns_sort"])
    def test_sort_by_two_columns(self, df: DataFrame):
        records = df.sort([("numeric_col", True), ("string_col", True)]).to_records(
            "python"
        )
        expected = [
            {"numeric_col": 1, "string_col": "apple"},
            {"numeric_col": 1, "string_col": "apple"},
            {"numeric_col": 2, "string_col": "banana"},
            {"numeric_col": 2, "string_col": "cherry"},
            {"numeric_col": 3, "string_col": "apple"},
            {"numeric_col": 4, "string_col": "date"},
            {"numeric_col": 5, "string_col": "fig"},
        ]
        self.assertEqual(records, expected)


class TestFilterParsing(unittest.TestCase):
    def test_parse_legacy_contains_filter(self):
        filter_dict = {"id": "col1", "value": "test", "type": "contains"}
        expected = Filter("col1", FilterOperator.TEXT_CONTAINS, ["test"])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_conditional_filter(self):
        filter_dict = {
            "column": "col1",
            "operator": "greater-than",
            "comparativeValues": [100],
        }
        expected = Filter("col1", FilterOperator.GREATER_THAN, [100])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_invalid_operator(self):
        filter_dict = {
            "column": "col1",
            "operator": "invalid-operator",
            "comparativeValues": [100],
        }
        with self.assertRaises(ValueError):
            Filter.from_dict(filter_dict)

    def test_parse_missing_required_keys(self):
        filter_dict = {
            "column": "col1",
            "operator": "greater-than",
            # missing comparativeValues
        }
        with self.assertRaises(ValueError):
            Filter.from_dict(filter_dict)

    def test_parse_legacy_missing_keys(self):
        filter_dict = {
            "id": "col1",
            "type": "contains",
            # missing value
        }
        with self.assertRaises(ValueError):
            Filter.from_dict(filter_dict)

    def test_parse_text_contains(self):
        filter_dict = {
            "column": "text_col",
            "operator": "text-contains",
            "comparativeValues": ["ap"],
        }
        expected = Filter("text_col", FilterOperator.TEXT_CONTAINS, ["ap"])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_text_does_not_contain(self):
        filter_dict = {
            "column": "text_col",
            "operator": "text-does-not-contain",
            "comparativeValues": ["ap"],
        }
        expected = Filter("text_col", FilterOperator.TEXT_DOES_NOT_CONTAIN, ["ap"])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_equal(self):
        filter_dict = {
            "column": "num_col",
            "operator": "is-equal",
            "comparativeValues": [2],
        }
        expected = Filter("num_col", FilterOperator.IS_EQUAL, [2])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_not_equal(self):
        filter_dict = {
            "column": "num_col",
            "operator": "is-not-equal",
            "comparativeValues": [2],
        }
        expected = Filter("num_col", FilterOperator.IS_NOT_EQUAL, [2])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_greater_than(self):
        filter_dict = {
            "column": "num_col",
            "operator": "greater-than",
            "comparativeValues": [2],
        }
        expected = Filter("num_col", FilterOperator.GREATER_THAN, [2])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_less_than(self):
        filter_dict = {
            "column": "num_col",
            "operator": "less-than",
            "comparativeValues": [3],
        }
        expected = Filter("num_col", FilterOperator.LESS_THAN, [3])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_one_of(self):
        filter_dict = {
            "column": "num_col",
            "operator": "is-one-of",
            "comparativeValues": [1, 3],
        }
        expected = Filter("num_col", FilterOperator.IS_ONE_OF, [1, 3])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_not_one_of(self):
        filter_dict = {
            "column": "num_col",
            "operator": "is-not-one-of",
            "comparativeValues": [1, 3],
        }
        expected = Filter("num_col", FilterOperator.IS_NOT_ONE_OF, [1, 3])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_null(self):
        filter_dict = {
            "column": "text_col",
            "operator": "is-null",
            "comparativeValues": [],
        }
        expected = Filter("text_col", FilterOperator.IS_NULL, [])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_not_null(self):
        filter_dict = {
            "column": "text_col",
            "operator": "is-not-null",
            "comparativeValues": [],
        }
        expected = Filter("text_col", FilterOperator.IS_NOT_NULL, [])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_between(self):
        filter_dict = {
            "column": "num_col",
            "operator": "between",
            "comparativeValues": [2, 3],
        }
        expected = Filter("num_col", FilterOperator.BETWEEN, [2, 3])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_outside_of(self):
        filter_dict = {
            "column": "num_col",
            "operator": "outside-of",
            "comparativeValues": [2, 3],
        }
        expected = Filter("num_col", FilterOperator.OUTSIDE_OF, [2, 3])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_after(self):
        filter_dict = {
            "column": "date_col",
            "operator": "is-after",
            "comparativeValues": ["2020-01-02"],
        }
        expected = Filter("date_col", FilterOperator.IS_AFTER, ["2020-01-02"])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_before(self):
        filter_dict = {
            "column": "date_col",
            "operator": "is-before",
            "comparativeValues": ["2020-01-03"],
        }
        expected = Filter("date_col", FilterOperator.IS_BEFORE, ["2020-01-03"])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_on(self):
        filter_dict = {
            "column": "date_col",
            "operator": "is-on",
            "comparativeValues": ["2020-01-02 11:00:00"],
        }
        expected = Filter("date_col", FilterOperator.IS_ON, ["2020-01-02 11:00:00"])
        self.assertEqual(Filter.from_dict(filter_dict), expected)

    def test_parse_is_relative_today(self):
        filter_dict = {
            "column": "date_col",
            "operator": "is-relative-today",
            "comparativeValues": ["today"],
        }
        expected = Filter("date_col", FilterOperator.IS_RELATIVE_TODAY, ["today"])
        self.assertEqual(Filter.from_dict(filter_dict), expected)


class TestDataFrameFiltering(unittest.TestCase):
    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_filter(self, df: DataFrame):
        records = df.filter(
            Filter("col1", FilterOperator.LESS_THAN_OR_EQUAL, comparative_values=[200]),
            Filter("col1", FilterOperator.GREATER_THAN, comparative_values=[100]),
        ).to_records("python")
        self.assertEqual(len(records), 100)

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_filter_missing_column(self, df: DataFrame):
        records = df.filter(
            Filter(
                "missing_col",
                FilterOperator.LESS_THAN_OR_EQUAL,
                comparative_values=[200],
            ),
            Filter("col1", FilterOperator.LESS_THAN, comparative_values=[100]),
        ).to_records("python")
        # Should only apply the valid filter
        self.assertEqual(len(records), 100)

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_filter_empty_comparative_values(self, df: DataFrame):
        records = df.filter(
            Filter("col1", FilterOperator.LESS_THAN_OR_EQUAL, comparative_values=[]),
            Filter("col1", FilterOperator.LESS_THAN, comparative_values=[100]),
        ).to_records("python")
        # Should only apply the filter with non-empty values
        self.assertEqual(len(records), 100)

    @_test_with_all_backends(testing_dataframes["many_rows_10k"])
    def test_filter_empty_filters_list(self, df: DataFrame):
        records = df.filter().to_records("python")
        # Should return all records when no filters are provided
        self.assertEqual(len(records), 10_000)

    @_test_with_all_backends({"text_col": ["apple", "banana", "cherry", "date"]})
    def test_filter_text_contains(self, df: DataFrame):
        records = df.filter(
            Filter("text_col", FilterOperator.TEXT_CONTAINS, comparative_values=["ap"])
        ).to_records("python")
        self.assertEqual(records[0]["text_col"], "apple")

    @_test_with_all_backends({"text_col": ["apple", "banana", "cherry", "date"]})
    def test_filter_text_does_not_contain(self, df: DataFrame):
        records = df.filter(
            Filter(
                "text_col",
                FilterOperator.TEXT_DOES_NOT_CONTAIN,
                comparative_values=["ap"],
            )
        ).to_records("python")
        self.assertEqual([r["text_col"] for r in records], ["banana", "cherry", "date"])

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_is_equal(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.IS_EQUAL, comparative_values=[2])
        ).to_records("python")
        self.assertEqual(records[0]["num_col"], 2)

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_is_not_equal(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.IS_NOT_EQUAL, comparative_values=[2])
        ).to_records("python")
        self.assertEqual([r["num_col"] for r in records], [1, 3, 4])

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_greater_than(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.GREATER_THAN, comparative_values=[2])
        ).to_records("python")
        self.assertEqual([r["num_col"] for r in records], [3, 4])

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_less_than(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.LESS_THAN, comparative_values=[3])
        ).to_records("python")
        self.assertEqual([r["num_col"] for r in records], [1, 2])

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_is_one_of(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.IS_ONE_OF, comparative_values=[1, 3])
        ).to_records("python")
        self.assertEqual([r["num_col"] for r in records], [1, 3])

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_is_not_one_of(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.IS_NOT_ONE_OF, comparative_values=[1, 3])
        ).to_records("python")
        self.assertEqual([r["num_col"] for r in records], [2, 4])

    @_test_with_all_backends({"text_col": ["apple", None, "cherry", "date"]})
    def test_filter_is_null(self, df: DataFrame):
        records = df.filter(
            Filter("text_col", FilterOperator.IS_NULL, comparative_values=[])
        ).to_records("python")

        self.assertEqual(records[0]["text_col"], None)

    @_test_with_all_backends({"text_col": ["apple", None, "cherry", "date"]})
    def test_filter_is_not_null(self, df: DataFrame):
        records = df.filter(
            Filter("text_col", FilterOperator.IS_NOT_NULL, comparative_values=[])
        ).to_records("python")
        self.assertEqual([r["text_col"] for r in records], ["apple", "cherry", "date"])

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_between(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.BETWEEN, comparative_values=[2, 3])
        ).to_records("python")
        self.assertEqual([r["num_col"] for r in records], [2, 3])

    @_test_with_all_backends({"num_col": [1, 2, 3, 4]})
    def test_filter_outside_of(self, df: DataFrame):
        records = df.filter(
            Filter("num_col", FilterOperator.OUTSIDE_OF, comparative_values=[2, 3])
        ).to_records("python")
        self.assertEqual([r["num_col"] for r in records], [1, 4])

    @_test_with_all_backends(
        {
            "date_col": [
                "2020-01-01 10:00:00",
                "2020-01-02 11:00:00",
                "2020-01-03 12:00:00",
                "2020-01-04 13:00:00",
            ]
        }
    )
    def test_filter_is_after(self, df: DataFrame):
        records = df.filter(
            Filter(
                "date_col", FilterOperator.IS_AFTER, comparative_values=["2020-01-02"]
            )
        ).to_records("python")
        self.assertEqual(
            [r["date_col"] for r in records],
            ["2020-01-02 11:00:00", "2020-01-03 12:00:00", "2020-01-04 13:00:00"],
        )

    @_test_with_all_backends(
        {
            "date_col": [
                "2020-01-01 10:00:00",
                "2020-01-02 11:00:00",
                "2020-01-03 12:00:00",
                "2020-01-04 13:00:00",
            ]
        }
    )
    def test_filter_is_before(self, df: DataFrame):
        records = df.filter(
            Filter(
                "date_col", FilterOperator.IS_BEFORE, comparative_values=["2020-01-03"]
            )
        ).to_records("python")
        self.assertEqual(
            [r["date_col"] for r in records],
            ["2020-01-01 10:00:00", "2020-01-02 11:00:00"],
        )

    @_test_with_all_backends(
        {
            "date_col": [
                "2020-01-01 10:00:00",
                "2020-01-02 11:00:00",
                "2020-01-03 12:00:00",
                "2020-01-04 13:00:00",
            ]
        }
    )
    def test_filter_is_on(self, df: DataFrame):
        records = df.filter(
            Filter(
                "date_col",
                FilterOperator.IS_ON,
                comparative_values=["2020-01-02 11:00:00"],
            )
        ).to_records("python")
        self.assertEqual(records[0]["date_col"], "2020-01-02 11:00:00")

    @_test_with_all_backends(
        {
            "date_col": [
                YESTERDAY.strftime("%Y-%m-%d %H:%M:%S"),  # yesterday
                CURRENT_TIME.strftime("%Y-%m-%d %H:%M:%S"),  # today
                TOMORROW.strftime("%Y-%m-%d %H:%M:%S"),  # tomorrow
                DAY_AFTER_TOMORROW.strftime("%Y-%m-%d %H:%M:%S"),  # day after tomorrow
            ]
        }
    )
    def test_filter_is_relative_today(self, df: DataFrame):
        records = df.filter(
            Filter(
                "date_col",
                FilterOperator.IS_RELATIVE_TODAY,
                comparative_values=["today"],
            )
        ).to_records("python")
        self.assertEqual(
            records[0]["date_col"], CURRENT_TIME.strftime("%Y-%m-%d %H:%M:%S")
        )

        records = df.filter(
            Filter(
                "date_col",
                FilterOperator.IS_RELATIVE_TODAY,
                comparative_values=["yesterday"],
            )
        ).to_records("python")
        self.assertEqual(
            records[0]["date_col"], YESTERDAY.strftime("%Y-%m-%d %H:%M:%S")
        )


if __name__ == "__main__":
    unittest.main()
