import unittest

import pandas as pd
import pandas.testing as pdt

import deepnote_toolkit.ocelots as oc
from deepnote_toolkit.dataframe_browser import (
    BrowseSpec,
    InvalidAttributesError,
    browse_df,
)


class TestBrowseDataframe(unittest.TestCase):
    def setUp(self):
        self.oc_df = oc.DataFrame.from_native(
            pd.DataFrame(
                data={
                    "col1": [
                        1214,
                        2456,
                        2431,
                        1354,
                    ],
                    "col2": [
                        4534,
                        135,
                        135,
                        43676548,
                    ],
                    "col3": [
                        32,
                        756,
                        32,
                        0,
                    ],
                    "col4": [
                        46,
                        9,
                        467,
                        846,
                    ],
                }
            )
        )

    def test_browse_with_filter_mixed_types(self):
        spec = BrowseSpec.from_json(
            '{"pageSize":1,"pageIndex":0,"filters":[{"id":"col1","value":"24","type":"contains"}], "conditionalFilters":[{"column":"col3","operator":"greater-than","comparativeValues":[500]}]}',
            self.oc_df.column_names,
        )
        expected_df = pd.DataFrame(
            data={
                "col1": [2456],
                "col2": [135],
                "col3": [756],
                "col4": [9],
                "_deepnote_index_column": [1],
            },
            index=[1],
        )
        expected_page = [
            {
                "_deepnote_index_column": 1,
                "col1": 2456,
                "col2": 135,
                "col3": 756,
                "col4": 9,
            }
        ]

        result = browse_df(self.oc_df, spec)
        pdt.assert_frame_equal(result.processed_df.to_native(), expected_df)
        self.assertEqual(result.rows, expected_page)

    def test_browse_with_filter_only_column_contains(self):
        """
        Should return df with browsing spec applied.
        """
        spec = BrowseSpec.from_json(
            '{"pageSize":1,"pageIndex":1,"filters":[{"id":"col1","value":"2","type":"contains"},{"id":"col4","value":"46","type":"contains"}],"sortBy":[{"id":"col3","type":"desc"},{"id":"col2","type":"asc"}]}',
            self.oc_df.column_names,
        )
        expected_df = pd.DataFrame(
            data={
                "col1": [
                    2431,
                    1214,
                ],
                "col2": [
                    135,
                    4534,
                ],
                "col3": [
                    32,
                    32,
                ],
                "col4": [
                    467,
                    46,
                ],
                "_deepnote_index_column": [2, 0],
            },
            index=[2, 0],
        )
        expected_page = [
            {
                "_deepnote_index_column": 0,
                "col1": 1214,
                "col2": 4534,
                "col3": 32,
                "col4": 46,
            }
        ]

        result = browse_df(self.oc_df, spec)
        pdt.assert_frame_equal(result.processed_df.to_native(), expected_df)
        self.assertEqual(result.rows, expected_page)

    def test_browse_with_filter_only_conditional(self):
        spec = BrowseSpec.from_json(
            '{"pageSize":2,"pageIndex":0,"conditionalFilters":[{"column":"col4","operator":"greater-than","comparativeValues":[100]}]}',
            self.oc_df.column_names,
        )
        expected_df = pd.DataFrame(
            data={
                "col1": [2431, 1354],
                "col2": [135, 43676548],
                "col3": [32, 0],
                "col4": [467, 846],
                "_deepnote_index_column": [2, 3],
            },
            index=[2, 3],
        )
        expected_page = [
            {
                "_deepnote_index_column": 2,
                "col1": 2431,
                "col2": 135,
                "col3": 32,
                "col4": 467,
            },
            {
                "_deepnote_index_column": 3,
                "col1": 1354,
                "col2": 43676548,
                "col3": 0,
                "col4": 846,
            },
        ]

        result = browse_df(self.oc_df, spec)
        pdt.assert_frame_equal(result.processed_df.to_native(), expected_df)
        self.assertEqual(result.rows, expected_page)

    def test_empty_spec(self):
        """
        Should return df with browsing defaults
        applied if given spec is missing.
        """
        spec = BrowseSpec.from_json("{}", self.oc_df.column_names)
        expected_df = self.oc_df.to_native().copy()
        expected_df["_deepnote_index_column"] = [0, 1, 2, 3]
        expected_page = [
            {
                "_deepnote_index_column": 0,
                "col1": 1214,
                "col2": 4534,
                "col3": 32,
                "col4": 46,
            },
            {
                "_deepnote_index_column": 1,
                "col1": 2456,
                "col2": 135,
                "col3": 756,
                "col4": 9,
            },
            {
                "_deepnote_index_column": 2,
                "col1": 2431,
                "col2": 135,
                "col3": 32,
                "col4": 467,
            },
            {
                "_deepnote_index_column": 3,
                "col1": 1354,
                "col2": 43676548,
                "col3": 0,
                "col4": 846,
            },
        ]

        result = browse_df(self.oc_df, spec)
        pdt.assert_frame_equal(result.processed_df.to_native(), expected_df)
        self.assertEqual(result.rows, expected_page)

    def test_empty_rows(self):
        """
        Should return df with given df having 0 rows.
        """
        df = pd.DataFrame(data={"_deepnote_index_column": [], "col1": [], "col2": []})
        spec = BrowseSpec.from_json("{}", tuple(df.columns.tolist()))
        expected_df = df
        expected_page = []

        result = browse_df(oc.DataFrame.from_native(df), spec)
        pdt.assert_frame_equal(result.processed_df.to_native(), expected_df)
        self.assertEqual(result.rows, expected_page)

    def test_empty_columns(self):
        """
        Should return df with given df having 0 columns.
        """
        df = pd.DataFrame(data={})
        spec = BrowseSpec.from_json("{}", tuple(df.columns.tolist()))
        expected_df = df
        expected_page = []

        result = browse_df(oc.DataFrame.from_native(df), spec)
        pdt.assert_frame_equal(result.processed_df.to_native(), expected_df)
        self.assertEqual(result.rows, expected_page)


class TestBrowseSpec(unittest.TestCase):
    def setUp(self):
        self.column_names = ("col1", "col2", "col3")

    def test_default_values(self):
        """Should create BrowseSpec with default values when empty spec is provided."""
        default_spec = BrowseSpec.from_json("{}", self.column_names)
        self.assertEqual(default_spec.filters, [])
        self.assertEqual(default_spec.sort_by, [])
        self.assertEqual(default_spec.page_size, 10)
        self.assertEqual(default_spec.page_index, 0)
        self.assertEqual(default_spec.cell_formatting_rules, [])
        self.assertEqual(default_spec.color_scale_column_names, [])

    def test_custom_page_settings(self):
        """Should create BrowseSpec with custom page settings."""
        spec = '{"pageSize": 5, "pageIndex": 2}'
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(browse_spec.page_size, 5)
        self.assertEqual(browse_spec.page_index, 2)

    def test_filters(self):
        """Should parse both regular and conditional filters."""
        # NOTE: more filter parsing tests in TestFilterParsing (test_ocelots.py)
        spec = """{
            "filters": [{"id": "col1", "value": "test", "type": "contains"}],
            "conditionalFilters": [{"column": "col2", "operator": "greater-than", "comparativeValues": [10]}]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(len(browse_spec.filters), 2)
        self.assertIsInstance(browse_spec.filters[0], oc.Filter)
        self.assertIsInstance(browse_spec.filters[1], oc.Filter)

    def test_sort_by(self):
        """Should parse sort by specifications correctly."""
        spec = """{
            "sortBy": [
                {"id": "col1", "type": "asc"},
                {"id": "col2", "type": "desc"}
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(len(browse_spec.sort_by), 2)
        self.assertEqual(browse_spec.sort_by[0], ("col1", True))
        self.assertEqual(browse_spec.sort_by[1], ("col2", False))

    def test_sort_by_invalid_column(self):
        """Should skip invalid column names in sort by."""
        spec = """{
            "sortBy": [
                {"id": "invalid_col", "type": "asc"},
                {"id": "col1", "type": "desc"}
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(len(browse_spec.sort_by), 1)
        self.assertEqual(browse_spec.sort_by[0], ("col1", False))

    def test_sort_by_invalid_type(self):
        """Should raise InvalidAttributesError when sort type is invalid."""
        spec = """{
            "sortBy": [
                {"id": "col1", "type": "invalid_type"}
            ]
        }"""

        with self.assertRaises(InvalidAttributesError):
            BrowseSpec.from_json(spec, self.column_names)

    def test_cell_formatting_rules(self):
        """Should parse cell formatting rules and color scale columns."""
        spec = """{
            "cellFormattingRules": [
                {
                    "type": "colorScale",
                    "columnSelectionMode": "only",
                    "columnNames": ["col1", "col2"]
                }
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(len(browse_spec.cell_formatting_rules), 1)
        self.assertEqual(set(browse_spec.color_scale_column_names), {"col1", "col2"})

    def test_cell_formatting_rules_all_columns(self):
        """Should include all columns when columnSelectionMode is 'all'."""
        spec = """{
            "cellFormattingRules": [
                {
                    "type": "colorScale",
                    "columnSelectionMode": "all"
                }
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(
            set(browse_spec.color_scale_column_names), set(self.column_names)
        )

    def test_cell_formatting_rules_all_except(self):
        """Should exclude specified columns when columnSelectionMode is 'allExcept'."""
        spec = """{
            "cellFormattingRules": [
                {
                    "type": "colorScale",
                    "columnSelectionMode": "allExcept",
                    "columnNames": ["col1"]
                }
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(set(browse_spec.color_scale_column_names), {"col2", "col3"})

    def test_cell_formatting_rules_non_existent_columns(self):
        """Should handle non-existent columns gracefully."""
        spec = """{
            "cellFormattingRules": [
                {
                    "type": "colorScale",
                    "columnSelectionMode": "only",
                    "columnNames": ["non_existent_col"]
                }
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(browse_spec.color_scale_column_names, [])

    def test_cell_formatting_rules_mixed_types(self):
        """Should handle mixed formatting rule types correctly."""
        spec = """{
            "cellFormattingRules": [
                {
                    "type": "singleColor",
                    "columnSelectionMode": "only",
                    "columnNames": ["col1"]
                },
                {
                    "type": "colorScale",
                    "columnSelectionMode": "only",
                    "columnNames": ["col2"]
                },
                {
                    "type": "colorScale",
                    "columnSelectionMode": "allExcept",
                    "columnNames": ["col3"]
                }
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(set(browse_spec.color_scale_column_names), {"col1", "col2"})

    def test_cell_formatting_rules_invalid_selection_mode(self):
        """Should handle invalid column selection mode gracefully."""
        spec = """{
            "cellFormattingRules": [
                {
                    "type": "colorScale",
                    "columnSelectionMode": "invalid_mode",
                    "columnNames": ["col1"]
                }
            ]
        }"""
        browse_spec = BrowseSpec.from_json(spec, self.column_names)

        self.assertEqual(browse_spec.color_scale_column_names, [])


if __name__ == "__main__":
    unittest.main()
