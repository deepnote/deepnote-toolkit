import json
import sys
import unittest
import uuid
import warnings

import pandas as pd
from ipykernel.jsonutil import json_clean
from parameterized import parameterized
from pyspark.sql import SparkSession

import deepnote_toolkit.ocelots as oc
from deepnote_toolkit.chart import ChartError, DeepnoteChart
from deepnote_toolkit.chart.spec_utils import (
    _get_used_fields_from_vega_lite_spec,
    verify_used_fields,
)
from deepnote_toolkit.chart.types import VEGA_5_MIME_TYPE
from deepnote_toolkit.chart.utils import sanitize_dataframe_for_chart

from .helpers.testing_dataframes import testing_dataframes

# Spark session for testing
spark = (
    SparkSession.builder.master("local")  # type: ignore
    .appName("Toolkit")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)


def create_spark_df(pandas_df: pd.DataFrame):
    with warnings.catch_warnings():
        # PySpark is noisy about not liking version of installed Pandas
        warnings.filterwarnings("ignore")
        return spark.createDataFrame(pandas_df)


class TestDeepnoteChart(unittest.TestCase):
    def setUp(self):
        self.simple_df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        self.simple_spec = {
            "mark": "bar",
            "encoding": {
                "x": {"field": "x"},
                "y": {"field": "y"},
            },
        }

    def test_constructor_neither_spec_nor_spec_dict_provided(self):
        """Test that constructor raises ValueError when neither spec nor spec_dict is provided."""
        with self.assertRaises(ValueError) as context:
            DeepnoteChart(self.simple_df)

        self.assertIn(
            "either spec or spec_dict should be provided", str(context.exception)
        )

    def test_constructor_both_spec_and_spec_dict_provided(self):
        """Test that constructor raises ValueError when both spec and spec_dict are provided."""
        with self.assertRaises(ValueError) as context:
            DeepnoteChart(
                self.simple_df,
                spec=json.dumps(self.simple_spec),
                spec_dict=self.simple_spec,
            )

        self.assertIn(
            "only one of spec or spec_dict should be provided when constructing DeepnoteChart",
            str(context.exception),
        )

    def test_constructor_with_invalid_json_spec(self):
        """Test that constructor handles invalid JSON spec gracefully."""
        with self.assertRaises(json.JSONDecodeError):
            DeepnoteChart(self.simple_df, spec="invalid json")

    def test_constructor_with_filters_string(self):
        """Test that constructor works with filters as JSON string."""
        filters_json = '[{"column": "x", "operator": "greater-than-or-equal", "comparativeValues": [1]}]'
        chart = DeepnoteChart(
            self.simple_df, spec=json.dumps(self.simple_spec), filters=filters_json
        )
        self.assertIsInstance(chart, DeepnoteChart)

    def test_constructor_with_filters_list(self):
        """Test that constructor works with filters as list."""
        filters = [oc.Filter("x", oc.FilterOperator.GREATER_THAN_OR_EQUAL, [1])]
        chart = DeepnoteChart(
            self.simple_df, spec=json.dumps(self.simple_spec), filters=filters
        )
        self.assertIsInstance(chart, DeepnoteChart)

    def test_interval_selection_signal_with_attach_selection_true(self):
        """Test that interval_selection signal is present in Vega spec when attach_selection=True."""
        chart = DeepnoteChart(
            self.simple_df, spec_dict=self.simple_spec, attach_selection=True
        )
        mimebundle = chart._repr_mimebundle_(None, None)

        vega_spec = mimebundle[VEGA_5_MIME_TYPE]

        self.assertIn("signals", vega_spec)
        signal_names = [signal.get("name") for signal in vega_spec["signals"]]
        # `interval_selection` is added by Vega-Lite when compiling spec based on `params` which we set
        # if attach_selection is True
        self.assertIn("interval_selection", signal_names)

    def test_no_interval_selection_signal_with_attach_selection_false(self):
        """Test that interval_selection signal is not present when attach_selection=False."""
        chart = DeepnoteChart(
            self.simple_df, spec=json.dumps(self.simple_spec), attach_selection=False
        )
        mimebundle = chart._repr_mimebundle_(None, None)

        vega_spec = mimebundle[VEGA_5_MIME_TYPE]

        if "signals" in vega_spec:
            signal_names = [signal.get("name") for signal in vega_spec["signals"]]
            self.assertNotIn("interval_selection", signal_names)

    def test_filters_are_applied_to_dataframe(self):
        """Test that provided filters are properly applied to the dataframe."""
        test_df = pd.DataFrame(
            {
                "x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "y": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
            }
        )

        filters = [oc.Filter("x", oc.FilterOperator.GREATER_THAN, [5])]

        chart_with_filters = DeepnoteChart(
            test_df, spec_dict=self.simple_spec, filters=filters
        )
        chart_without_filters = DeepnoteChart(test_df, spec_dict=self.simple_spec)

        filtered_size = chart_with_filters.dataframe.size()
        unfiltered_size = chart_without_filters.dataframe.size()

        self.assertLess(filtered_size, unfiltered_size)
        self.assertEqual(filtered_size, 5)
        self.assertEqual(unfiltered_size, 10)

    # Regression test for https://www.notion.so/deepnote/Chart-block-output-error-ValueError-NaTType-does-not-support-strftime-de4f31ff403549bba93cd2ab3e100319
    def test_json_serializable_uuid(self):
        df = pd.DataFrame(data={"id": [uuid.uuid4()]})

        spec = {
            "mark": "bar",
            "encoding": {
                "x": {"field": "id"},
                "y": {"field": "id"},
            },
        }

        chart = DeepnoteChart(df, json.dumps(spec))
        mimebundle = chart._repr_mimebundle_(None, None)

        try:
            # We run these 2 functions on the mimebundle to check if ipykernel will be able to serialize the result into the notebook
            # json, which is necessary for our whole mechanism to work. It raises an exception for e.g. timedelta types
            cleaned_for_json = json_clean(mimebundle)
            json.dumps(cleaned_for_json)
        except:  # noqa: E722
            self.fail("cleaning for JSON or JSON serialization failed")

    @parameterized.expand([(key, df) for key, df in testing_dataframes.items()])
    def test_json_serializable_dataframes(self, key, df):
        skipped_dataframes = {
            "multi_level_columns",
            "no_columns",
            "non_serializable_values",
            "mixed_column_types",
        }

        if key in skipped_dataframes:
            return

        print("Testing chart with DF", key)
        spec = {
            "mark": "bar",
            "encoding": {
                "x": {"field": str(df.columns[0])},
            },
        }

        chart = DeepnoteChart(df, json.dumps(spec))
        mimebundle = chart._repr_mimebundle_(None, None)

        try:
            cleaned_for_json = json_clean(mimebundle)
            json.dumps(cleaned_for_json)
        except:  # noqa: E722
            self.fail(f"cleaning for JSON or JSON serialization failed for {key}")

    def test_works_with_spark_dataframe(self):
        """Test that DeepnoteChart works with Spark DataFrames."""
        if sys.version_info >= (3, 12):
            self.skipTest("PySpark does not yet support Python 3.12")

        # Create a Spark DataFrame from pandas DataFrame
        spark_df = create_spark_df(self.simple_df)

        # Create chart with Spark DataFrame
        chart = DeepnoteChart(spark_df, spec_dict=self.simple_spec)

        # Verify the chart was created successfully
        self.assertIsInstance(chart, DeepnoteChart)

        # Verify the mimebundle can be generated
        mimebundle = chart._repr_mimebundle_(None, None)
        self.assertIn(VEGA_5_MIME_TYPE, mimebundle)

        # Verify JSON serialization works
        try:
            cleaned_for_json = json_clean(mimebundle)
            json.dumps(cleaned_for_json)
        except:  # noqa: E722
            self.fail(
                "cleaning for JSON or JSON serialization failed for Spark DataFrame"
            )


class TestDeepnoteSanitizeDataframe(unittest.TestCase):
    def test_small_dataframe_remains_ordered_the_same(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        df_sanitized = sanitize_dataframe_for_chart(df)
        self.assertEqual(df_sanitized["a"].tolist(), [1, 2, 3])

    def test_large_dataframe_remains_ordered_the_same(self):
        # if more than 10000 rows, sampling is used
        df = pd.DataFrame({"a": range(21000)})

        df_sanitized = sanitize_dataframe_for_chart(df)
        # we convert to integers here because _deepnote_sanitize_dataframe converts elements to strings
        values = list(map(int, df_sanitized["a"].tolist()))

        self.assertEqual(sorted(values), values)

    @parameterized.expand([(key, df) for key, df in testing_dataframes.items()])
    def test_all_testing_dataframes_do_not_raise_error(self, key, df):
        skipped_dataframes = {"multi_level_columns", "non_serializable_values"}

        if key in skipped_dataframes:
            return

        sanitize_dataframe_for_chart(df)


class TestGetUsedFields(unittest.TestCase):
    def test_non_layered_spec_with_two_different_fields(self):
        spec = {"encoding": {"y": {"field": "a"}, "x": {"field": "b"}}}

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(fields, {"a", "b"})

    def test_non_layered_spec_with_two_same_fields(self):
        spec = {"encoding": {"y": {"field": "a"}, "x": {"field": "a"}}}

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(fields, {"a"})

    def test_two_layer_spec_with_encodings_in_each_layer(self):
        spec = {
            "layer": [
                {"mark": "bar", "encoding": {"x": {"field": "a"}}},
                {
                    "encoding": {"text": {"field": "b"}, "x": {"field": "c"}},
                },
            ]
        }

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(fields, {"a", "b", "c"})

    def test_two_layer_spec_with_encoding_only_in_one_layer(self):
        spec = {
            "layer": [
                {"mark": "bar"},
                {
                    "encoding": {"text": {"field": "b"}, "x": {"field": "c"}},
                },
            ]
        }

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(fields, {"b", "c"})

    def test_encoding_without_field(self):
        spec = {"encoding": {"y": {"field": "a"}, "x": {"datum": "a"}}}

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(fields, {"a"})

    def test_encoding_with_field_empty(self):
        spec = {"encoding": {"y": {"field": "a"}, "x": {"field": ""}}}

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(fields, {"a", ""})

    def test_multilayer_spec_v2(self):
        spec = {
            "layer": [
                {
                    "layer": [
                        {
                            "layer": [
                                {
                                    "mark": {"type": "bar"},
                                    "encoding": {
                                        "x": {
                                            "type": "temporal",
                                            "field": "datetime",
                                            "timeUnit": "yearmonth",
                                        },
                                        "y": {
                                            "type": "quantitative",
                                            "field": "order_total",
                                        },
                                    },
                                }
                            ]
                        }
                    ],
                },
                {
                    "layer": [
                        {
                            "layer": [
                                {
                                    "mark": {
                                        "type": "bar",
                                    },
                                    "encoding": {
                                        "x": {
                                            "type": "temporal",
                                            "field": "datetime",
                                            "timeUnit": "yearmonth",
                                        },
                                        "y": {
                                            "type": "quantitative",
                                            "field": "quantity",
                                        },
                                    },
                                }
                            ]
                        }
                    ],
                },
            ],
            "encoding": {},
        }

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(fields, {"datetime", "order_total", "quantity"})

    def test_field_with_special_characters(self):
        """Test that fields with special characters like [ and . are properly handled."""
        spec = {
            "encoding": {
                "x": {"field": "column\\[0\\]"},
                "y": {"field": "column\\.with\\.dots"},
                "color": {"field": "column\\\\with\\\\backslashes"},
            }
        }

        fields = _get_used_fields_from_vega_lite_spec(spec)

        self.assertSetEqual(
            fields, {"column[0]", "column.with.dots", "column\\with\\backslashes"}
        )


class TestVerifyUsedFields(unittest.TestCase):
    def setUp(self):
        self.oc_df = oc.DataFrame.from_native(
            pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        )

    def test_valid_fields(self):
        """Test that valid fields in the spec pass verification."""
        spec = {"encoding": {"x": {"field": "a"}, "y": {"field": "b"}}}
        # Should not raise any exception
        verify_used_fields(self.oc_df, spec)

    def test_invalid_field(self):
        """Test that invalid fields raise ChartError."""
        spec = {
            "encoding": {
                "x": {"field": "c"},  # 'c' doesn't exist in df
                "y": {"field": "a"},
            }
        }
        with self.assertRaises(ChartError):
            verify_used_fields(self.oc_df, spec)

    def test_count_field(self):
        """Test that COUNT(*) field is allowed."""
        spec = {"encoding": {"x": {"field": "COUNT(*)"}, "y": {"field": "a"}}}
        # Should not raise any exception
        verify_used_fields(self.oc_df, spec)

    def test_percentage_fields(self):
        """Test that percentage fields are allowed."""
        spec = {
            "encoding": {
                "x": {"field": "Percentage of a"},
                "y": {"field": "Percentage of b"},
            }
        }
        # Should not raise any exception
        verify_used_fields(self.oc_df, spec)

    def test_mixed_valid_and_invalid_fields(self):
        """Test that having both valid and invalid fields raises ChartError."""
        spec = {
            "encoding": {
                "x": {"field": "a"},
                "y": {"field": "c"},  # 'c' doesn't exist in df
                "color": {"field": "Percentage of b"},
            }
        }
        with self.assertRaises(ChartError):
            verify_used_fields(self.oc_df, spec)

    def test_multilayer_spec_v2_valid_fields(self):
        """Test that multilayer spec v2 with valid fields passes verification."""
        spec = {
            "layer": [
                {
                    "layer": [
                        {
                            "layer": [
                                {
                                    "mark": {"type": "bar"},
                                    "encoding": {
                                        "x": {
                                            "type": "temporal",
                                            "field": "a",
                                        },
                                        "y": {
                                            "type": "quantitative",
                                            "field": "b",
                                        },
                                    },
                                }
                            ]
                        }
                    ],
                },
                {
                    "layer": [
                        {
                            "layer": [
                                {
                                    "mark": {
                                        "type": "bar",
                                    },
                                    "encoding": {
                                        "x": {
                                            "type": "temporal",
                                            "field": "a",
                                        },
                                        "y": {
                                            "type": "quantitative",
                                            "field": "COUNT(*)",
                                        },
                                    },
                                }
                            ]
                        }
                    ],
                },
            ],
            "encoding": {},
        }
        # Should not raise any exception
        verify_used_fields(self.oc_df, spec)

    def test_multilayer_spec_v2_invalid_fields(self):
        """Test that multilayer spec v2 with invalid fields raises ChartError."""
        spec = {
            "layer": [
                {
                    "layer": [
                        {
                            "layer": [
                                {
                                    "mark": {"type": "bar"},
                                    "encoding": {
                                        "x": {
                                            "type": "temporal",
                                            "field": "a",
                                        },
                                        "y": {
                                            "type": "quantitative",
                                            "field": "invalid_field",
                                        },
                                    },
                                }
                            ]
                        }
                    ],
                },
                {
                    "layer": [
                        {
                            "layer": [
                                {
                                    "mark": {
                                        "type": "bar",
                                    },
                                    "encoding": {
                                        "x": {
                                            "type": "temporal",
                                            "field": "a",
                                        },
                                        "y": {
                                            "type": "quantitative",
                                            "field": "Percentage of b",
                                        },
                                    },
                                }
                            ]
                        }
                    ],
                },
            ],
            "encoding": {},
        }
        with self.assertRaises(ChartError):
            verify_used_fields(self.oc_df, spec)
