import datetime
import json
import tempfile
from pathlib import Path
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from deepnote_toolkit.variable_explorer import (
    ExportDataframeError,
    ExportSizeDataframeError,
    _get_elements_of,
    _get_variable_dict_entry,
    deepnote_export_df,
)


class TestVariableElementsGetter:
    def test_get_elements_from_list(self):
        expected = ["True", "5553", "nah", "None", "", "2", "2"]
        data = [True, 5553, "nah", None, "", "2", 2]
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_empty_list(self):
        expected = []
        data = []
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_ndarray(self):
        expected = ["True", "5553", "nah", "None", "", "2", "2"]
        data = np.array([True, 5553, "nah", None, "", "2", 2], dtype=object)
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_empty_ndarray(self):
        expected = []
        data = np.array([], dtype=object)
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_dataframe(self):
        expected = ["True", "5553", "nah", "None", "", "2", "2"]
        data = pd.DataFrame([True, 5553, "nah", None, "", "2", 2])
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_empty_dataframe(self):
        expected = []
        data = pd.DataFrame([])
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_empty_dataframe_2(self):
        expected = []
        data = pd.DataFrame()
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_series(self):
        expected = ["True", "5553", "nah", "None", "", "2", "2"]
        data = pd.Series([True, 5553, "nah", None, "", "2", 2], dtype=object)
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_empty_series(self):
        expected = []
        data = pd.Series([], dtype=object)
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_empty_series_2(self):
        expected = []
        data = pd.Series(dtype=object)
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_unsupported(self):
        expected = None
        data = "421421"
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_too_big(self):
        data = [*range(1001)]
        result = _get_elements_of(data)
        assert result is None

    def test_get_elements_from_single_long_string_should_return_none(self):
        data = ["a" * 10001]
        result = _get_elements_of(data)
        assert result is None

    def test_get_elements_from_array_containing_dict_should_return_none(self):
        data = ["hello", {"a": "b"}]
        result = _get_elements_of(data)
        assert result is None

    def test_get_elements_from_query_preview(self):
        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        expected = ["True", "5553", "nah", "None", "", "2", "2"]
        data = DeepnoteQueryPreview(
            {"col1": [True, 5553, "nah", None, "", "2", 2]},
            deepnote_query="SELECT * FROM table",
        )
        result = _get_elements_of(data)
        assert result == expected

    def test_get_elements_from_empty_query_preview(self):
        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        expected = []
        data = DeepnoteQueryPreview()
        result = _get_elements_of(data)
        assert result == expected


class TestGetVariableDictEntry:
    def test_simple_integer(self):
        result = _get_variable_dict_entry("x", 1)
        assert result == {
            "varName": "x",
            "varType": "int",
            "varSize": 28,
            "varShape": None,
            "varContent": "1",
            "varElements": None,
            "varColumns": None,
            "varColumnTypes": None,
            "varUnderlyingType": None,
            "numElements": None,
            "numColumns": None,
        }

    def test_deepnote_query_preview(self):
        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create a DeepnoteQueryPreview object with a sample query
        sample_query = "SELECT * FROM table"
        df = DeepnoteQueryPreview(
            {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}, deepnote_query=sample_query
        )

        # Test that _get_variable_dict_entry correctly processes the DeepnoteQueryPreview object
        result = _get_variable_dict_entry("query_result", df)
        assert result is not None

        # Verify the basic DataFrame properties are captured
        assert result["varName"] == "query_result"
        assert result["varType"] == "DeepnoteQueryPreview"
        assert result["varShape"] == "(3, 2)"
        assert "Column names: col1, col2" in result["varContent"]
        assert result["varElements"] == ["1", "2", "3"]
        assert result["varColumns"] == ["col1", "col2"]
        assert result["numElements"] == 3
        assert result["numColumns"] == 2

    def test_nb_2374_dataframe_period_column(self):
        df = pd.DataFrame([1, 2, 3], columns=pd.PeriodIndex(["2021-06"], freq="M"))
        result = _get_variable_dict_entry("df", df)
        assert result is not None
        expected = {
            "varType": "DataFrame",
            "varShape": "(3, 1)",
            "varContent": "Column names: 2021-06",
            "varElements": ["1", "2", "3"],
            "varColumns": ["2021-06"],
            "numElements": 3,
            "numColumns": 1,
        }
        # Check that all expected key-value pairs are in result
        for key, value in expected.items():
            assert result[key] == value


class TestVariableTypes:
    # Define test cases as tuples: (name, input_value, expected)
    _test_cases: ClassVar[list[tuple[str, Any, dict]]] = [
        # Native types
        ("native_boolean", True, {"varType": "bool"}),
        ("native_null", None, {"varType": "NoneType"}),
        ("native_int", 5, {"varType": "int"}),
        ("native_float", 2.0, {"varType": "float"}),
        ("native_complex", complex(1, 2), {"varType": "complex"}),
        ("native_list", [1, 2, 3], {"varType": "list"}),
        ("native_tuple", (1, 2, 3), {"varType": "tuple"}),
        ("native_string", "abc", {"varType": "str"}),
        ("native_dict", {1: 2, "key": "value"}, {"varType": "dict"}),
        ("native_set", {1, 2, 3}, {"varType": "set"}),
        (
            "native_string_multiline",
            """
a
b
c
""",
            {"varType": "str"},
        ),
        ("native_frozenset", frozenset({1, 2, 3}), {"varType": "frozenset"}),
        (
            "native_notimplemented",
            NotImplemented,
            {"varType": "NotImplementedType"},
        ),
        ("native_range", range(10), {"varType": "range"}),
        ("native_bytes", b"bytes", {"varType": "bytes"}),
        ("native_bytearray", bytearray(b".\xf0\xf1\xf2"), {"varType": "bytearray"}),
        # Python datetime types
        ("datetime_date", datetime.date.today(), {"varType": "date"}),
        ("datetime_datetime", datetime.datetime.now(), {"varType": "datetime"}),
        (
            "datetime_timedelta",
            datetime.timedelta(days=1),
            {"varType": "timedelta"},
        ),
        # Numpy types
        ("np_int8", np.int8(1), {"varType": "int8"}),
        ("np_float64", np.float64(2.0), {"varType": "float64"}),
        ("np_complex128", np.complex128(1 + 2j), {"varType": "complex128"}),
        ("np_array", np.array([1, 2, 3]), {"varType": "ndarray"}),
        ("np_bool", np.bool_(True), {"varType": "bool_"}),
        ("np_byte", np.byte(1), {"varType": "int8"}),
        ("np_ubyte", np.ubyte(1), {"varType": "uint8"}),
        ("np_short", np.short(1), {"varType": "int16"}),
        ("np_ushort", np.ushort(1), {"varType": "uint16"}),
        ("np_half", np.half(2.0), {"varType": "float16"}),
        ("np_float16", np.float16(2.0), {"varType": "float16"}),
        ("np_single", np.single(2.0), {"varType": "float32"}),
        ("np_double", np.double(2.0), {"varType": "float64"}),
        ("np_csingle", np.csingle(1 + 2j), {"varType": "complex64"}),
        ("np_cdouble", np.cdouble(1 + 2j), {"varType": "complex128"}),
        (
            "np_array_2",
            np.array([1, 2.0, "3", 1 + 1j, [1, 2, 3], [1, 2, 3]], dtype=object),
            {"varType": "ndarray"},
        ),
        (
            "np_array_3",
            np.array(
                [
                    [1.0 + 0.0j, 2.0 + 0.0j],
                    [0.0 + 0.0j, 0.0 + 0.0j],
                    [1.0 + 1.0j, 3.0 + 0.0j],
                ]
            ),
            {"varType": "ndarray"},
        ),
        ("np_range", np.arange(10), {"varType": "ndarray"}),
        ("np_range_2", np.arange(2, 10, dtype=float), {"varType": "ndarray"}),
        ("np_indices", np.indices((3, 3)), {"varType": "ndarray"}),
        # Additional NumPy types
        ("np_int64", np.int64(1), {"varType": "int64"}),
        ("np_float32", np.float32(2.0), {"varType": "float32"}),
        (
            "np_array_2d",
            np.array([[1, 2], [3, 4]]),
            {"varType": "ndarray", "varShape": "(2, 2)"},
        ),
        (
            "np_zeros",
            np.zeros((2, 3)),
            {"varType": "ndarray", "varShape": "(2, 3)"},
        ),
        # Pandas types
        ("pd_series", pd.Series([1, 2, 3]), {"varType": "Series"}),
        (
            "pd_dataframe",
            pd.DataFrame(np.random.randn(2, 3), columns=["A", "B", "C"]),
            {"varType": "DataFrame"},
        ),
        (
            "pd_period_range",
            pd.period_range("1/1/2011", "1/1/2012", freq="M"),
            {"varType": "PeriodIndex"},
        ),
        (
            "pd_interval_range",
            pd.interval_range(start=0, end=5),
            {"varType": "IntervalIndex"},
        ),
        (
            "pd_timestamp",
            pd.Timestamp("2019-01-01", tz="US/Pacific"),
            {"varType": "Timestamp"},
        ),
        (
            "pd_series_2",
            pd.Series(["a", "b", "c"], index=["a", "b", "c"], dtype="string"),
            {"varType": "Series"},
        ),
        (
            "pd_series_date_range",
            pd.Series(range(3), pd.date_range("20130101", periods=3, tz="UTC")),
            {"varType": "Series"},
        ),
        ("pd_index_2", pd.Index(list("abc")), {"varType": "Index"}),
        (
            "pd_date_range",
            pd.date_range("3/6/2012 00:00", periods=15, freq="D"),
            {"varType": "DatetimeIndex"},
        ),
        (
            "pd_date_range_tz",
            pd.date_range("3/6/2012 00:00", periods=3, freq="D", tz="Europe/London"),
            {"varType": "DatetimeIndex"},
        ),
        (
            "pd_datetime_index",
            pd.DatetimeIndex(
                [
                    "11/06/2011 00:00",
                    "11/06/2011 01:00",
                    "11/06/2011 01:00",
                    "11/06/2011 02:00",
                ]
            ),
            {"varType": "DatetimeIndex"},
        ),
        ("pd_period", pd.Period("2012", freq="A-DEC"), {"varType": "Period"}),
        (
            "pd_sparse_array",
            pd.arrays.SparseArray([1, 2, np.nan, 4]),
            {"varType": "SparseArray"},
        ),
        ("pd_interval", pd.Interval(1, 2), {"varType": "Interval"}),
        ("pd_interval_2", pd.Interval(0.5, 1.5), {"varType": "Interval"}),
        (
            "pd_array_int64",
            pd.array([1, 2, np.nan], dtype="Int64"),
            {"varType": "IntegerArray"},
        ),
        (
            "pd_array_boolean",
            pd.array([True, False, None], dtype="boolean"),
            {"varType": "BooleanArray"},
        ),
        (
            "pd_index",
            pd.Index([1, 2, 3]),
            {"varType": "Index"},
        ),
        (
            "pd_series_datetime_index",
            pd.Series([1, 2, 3], index=pd.date_range("20210101", periods=3)),
            {"varType": "Series"},
        ),
        (
            "pd_category",
            pd.Series(["a", "b", "c", "a"], dtype="category"),
            {"varType": "Series"},
        ),
    ]

    @pytest.mark.parametrize(
        ("name", "input_value", "expected"),
        _test_cases,
        ids=[case[0] for case in _test_cases],
    )
    def test_variable_types(self, name, input_value, expected):
        result = _get_variable_dict_entry(name, input_value)
        assert result is not None
        for key, value in expected.items():
            assert result.get(key) == value, f"Failed at {name} with {key}"


class TestDeepnoteExportDf:
    """Test cases for the deepnote_export_df function."""

    @pytest.fixture
    def sample_dataframe(self):
        """Create a sample pandas DataFrame for testing."""
        return pd.DataFrame(
            {"col1": [1, 2, 3, 4, 5], "col2": ["a", "b", "c", "d", "e"]}
        )

    @pytest.fixture
    def empty_spec_json(self):
        """Create an empty spec JSON for testing."""
        return json.dumps({"filters": [], "sortBy": []})

    @pytest.fixture
    def spec_json_with_filter(self):
        """Create a spec JSON with filters."""
        return json.dumps(
            {
                "filters": [
                    {
                        "column": "col1",
                        "operator": "greater-than",
                        "comparativeValues": [2],
                    }
                ],
                "sortBy": [],
            }
        )

    @pytest.fixture
    def spec_json_with_sort(self):
        """Create a spec JSON with sorting."""
        return json.dumps({"filters": [], "sortBy": [{"id": "col1", "type": "desc"}]})

    def test_export_df_success(self, sample_dataframe, empty_spec_json):
        """Test successful export of a supported DataFrame."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            # Call the export function
            deepnote_export_df(sample_dataframe, empty_spec_json, str(output_path))

            # Verify the file was created
            assert output_path.exists()

            # Verify the content
            exported_df = pd.read_csv(output_path)
            assert len(exported_df) == 5
            assert list(exported_df.columns) == ["col1", "col2"]

    def test_export_df_creates_parent_directory(
        self, sample_dataframe, empty_spec_json
    ):
        """Test that parent directories are created if they don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "subdir" / "nested" / "output.csv"

            # Call the export function
            deepnote_export_df(sample_dataframe, empty_spec_json, str(output_path))

            # Verify the file and directories were created
            assert output_path.exists()
            assert output_path.parent.exists()

    def test_export_df_with_filters(self, sample_dataframe, spec_json_with_filter):
        """Test export with filters applied."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "filtered_output.csv"

            # Call the export function
            deepnote_export_df(
                sample_dataframe, spec_json_with_filter, str(output_path)
            )

            # Verify the file was created
            assert output_path.exists()

            # Verify filtered data (col1 > 2 should give us 3 rows)
            exported_df = pd.read_csv(output_path)
            assert len(exported_df) == 3
            assert all(exported_df["col1"] > 2)

    def test_export_df_with_sorting(self, sample_dataframe, spec_json_with_sort):
        """Test export with sorting applied."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "sorted_output.csv"

            # Call the export function
            deepnote_export_df(sample_dataframe, spec_json_with_sort, str(output_path))

            # Verify the file was created
            assert output_path.exists()

            # Verify sorted data (descending order)
            exported_df = pd.read_csv(output_path)
            assert exported_df["col1"].tolist() == [5, 4, 3, 2, 1]

    def test_export_df_unsupported_type_raises_error(self, empty_spec_json):
        """Test that unsupported dataframe types raise ExportDataframeError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            # Try to export an unsupported type (e.g., a list)
            unsupported_data = [1, 2, 3, 4, 5]

            with pytest.raises(ExportDataframeError) as exc_info:
                deepnote_export_df(unsupported_data, empty_spec_json, str(output_path))

            assert "not supported as DataFrame" in str(exc_info.value)

    @patch("deepnote_toolkit.variable_explorer.oc.DataFrame")
    def test_export_df_size_limit_exceeded(
        self, mock_df_class, sample_dataframe, empty_spec_json
    ):
        """Test that ExportSizeDataframeError is raised when size exceeds limit."""
        # Mock the DataFrame class and its methods
        mock_df_instance = MagicMock()
        mock_processed_df = MagicMock()

        # Set up the mock chain
        mock_df_class.is_supported.return_value = True
        mock_df_class.from_native.return_value = mock_df_instance
        mock_df_instance.prepare_for_serialization.return_value = mock_df_instance
        mock_df_instance.filter.return_value = mock_df_instance
        mock_df_instance.sort.return_value = mock_processed_df

        # Mock estimate_export_byte_size to return more than 1GB
        mock_processed_df.estimate_export_byte_size.return_value = (
            1024 * 1024 * 1024 + 1
        )  # 1GB + 1 byte

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.csv"

            with pytest.raises(ExportSizeDataframeError) as exc_info:
                deepnote_export_df(sample_dataframe, empty_spec_json, str(output_path))

            assert "too big" in str(exc_info.value)
            assert "1.0 GB" in str(exc_info.value)

    def test_export_empty_dataframe(self, empty_spec_json):
        """Test export of an empty DataFrame."""
        empty_df = pd.DataFrame()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "empty_output.csv"

            # Call the export function
            deepnote_export_df(empty_df, empty_spec_json, str(output_path))

            # Verify the file was created
            assert output_path.exists()

            # Verify the file is empty or has no data rows
            with open(output_path, "r") as f:
                content = f.read()
                # Empty dataframe should result in an empty file or just a newline
                assert len(content) == 0 or content == "\n"

    def test_export_dataframe_with_different_types(self, empty_spec_json):
        """Test export of DataFrame with different column types."""
        df = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "string_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
                "date_col": pd.date_range("2023-01-01", periods=3),
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "mixed_types_output.csv"

            # Call the export function
            deepnote_export_df(df, empty_spec_json, str(output_path))

            # Verify the file was created
            assert output_path.exists()

            # Verify all columns are present
            exported_df = pd.read_csv(output_path)
            assert len(exported_df) == 3
            assert len(exported_df.columns) == 5

    def test_export_large_dataframe_within_limit(self, empty_spec_json):
        """Test export of a large DataFrame that's still within the size limit."""
        # Create a DataFrame with 10,000 rows
        large_df = pd.DataFrame(
            {
                "col1": range(10000),
                "col2": ["test_string"] * 10000,
                "col3": [1.23456789] * 10000,
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "large_output.csv"

            # Call the export function (should succeed as it's under 1GB)
            deepnote_export_df(large_df, empty_spec_json, str(output_path))

            # Verify the file was created
            assert output_path.exists()

            # Verify the content
            exported_df = pd.read_csv(output_path)
            assert len(exported_df) == 10000

    def test_export_with_complex_filters_and_sorting(self):
        """Test export with multiple filters and sorting."""
        df = pd.DataFrame(
            {
                "col1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "col2": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
                "col3": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
            }
        )

        spec_json = json.dumps(
            {
                "filters": [
                    {
                        "column": "col1",
                        "operator": "greater-than-or-equal",
                        "comparativeValues": [3],
                    },
                    {
                        "column": "col1",
                        "operator": "less-than-or-equal",
                        "comparativeValues": [7],
                    },
                ],
                "sortBy": [{"id": "col3", "type": "asc"}],  # Ascending order by col3
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "complex_output.csv"

            # Call the export function
            deepnote_export_df(df, spec_json, str(output_path))

            # Verify the file was created
            assert output_path.exists()

            # Verify filtered and sorted data
            exported_df = pd.read_csv(output_path)
            assert len(exported_df) == 5  # Rows with col1 between 3 and 7
            assert all((exported_df["col1"] >= 3) & (exported_df["col1"] <= 7))
            # Verify ascending order by col3
            assert exported_df["col3"].tolist() == sorted(exported_df["col3"].tolist())

    def test_export_df_with_special_characters_in_path(
        self, sample_dataframe, empty_spec_json
    ):
        """Test export with special characters in the filename."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use a filename with spaces and special characters
            output_path = Path(tmpdir) / "test output (1).csv"

            # Call the export function
            deepnote_export_df(sample_dataframe, empty_spec_json, str(output_path))

            # Verify the file was created
            assert output_path.exists()

            # Verify the content
            exported_df = pd.read_csv(output_path)
            assert len(exported_df) == 5
