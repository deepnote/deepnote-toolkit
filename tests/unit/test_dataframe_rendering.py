"""
Unit tests for DataFrame rendering with structured types.

These tests simulate the complete rendering flow that happens when the frontend
displays a DataFrame, ensuring both column analysis and data serialization work correctly.

This is a regression test suite for BLU-5140 where Trino STRUCT/ROW types caused
analyze_columns() to crash, resulting in fallback to plain DataFrame view instead of
the Deepnote native DataFrame view.
"""

import numpy as np
import pandas as pd
from trino.types import NamedRowTuple

from deepnote_toolkit.ocelots import DataFrame
from deepnote_toolkit.ocelots.pandas.analyze import analyze_columns


def _test_rendering_flow(df, expected_columns):
    """
    Simulate the complete rendering flow:
    1. analyze_columns() - for native view with stats
    2. to_records(mode="json") - for cell values

    Both paths must work for the Deepnote native DataFrame view to display correctly.
    """
    # 1. column stats (native view)
    analysis_result = analyze_columns(df)

    assert len(analysis_result) == len(expected_columns)

    for col_name in expected_columns:
        col = next(c for c in analysis_result if c.name == col_name)
        assert col.stats is not None, f"analyze_columns() failed for {col_name}"
        # Object columns should have categories for display
        if df[col_name].dtype == object:
            assert (
                col.stats.categories is not None
            ), f"No categories for object column {col_name}"

    # 2. cell values
    oc_df = DataFrame.from_native(df)
    records = oc_df.to_records(mode="json")

    assert len(records) == len(df)
    # all values are JSON-serializable (strings, numbers, None)
    for record in records:
        for col_name in expected_columns:
            value = record[col_name]
            assert isinstance(
                value, (str, int, float, type(None))
            ), f"Value for {col_name} is not JSON-serializable: {type(value)}"


def test_rendering_with_dict_objects():
    """Test rendering DataFrame with dict objects (simulates database ROW types)."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "struct_col": [
                {"a": "item_1", "b": "value_10"},
                {"a": "item_2", "b": "value_20"},
                {"a": "item_3", "b": "value_30"},
            ],
        }
    )

    _test_rendering_flow(df, ["id", "struct_col"])


def test_rendering_with_list_objects():
    """Test rendering DataFrame with list objects (simulates database ARRAY types)."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "array_col": [
                ["tag_1", "item", "test"],
                ["tag_2", "item", "test"],
                ["tag_3", "item", "test"],
            ],
        }
    )

    _test_rendering_flow(df, ["id", "array_col"])


def test_rendering_with_tuple_objects():
    """Test rendering DataFrame with tuple objects."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "tuple_col": [
                ("item_1", "value_10"),
                ("item_2", "value_20"),
                ("item_3", "value_30"),
            ],
        }
    )

    _test_rendering_flow(df, ["id", "tuple_col"])


def test_rendering_with_trino_namedrowtuple():
    """
    Test rendering DataFrame with Trino NamedRowTuple objects.

    This is the exact scenario from BLU-5140 that caused the crash.
    Before the fix, pd.Series(np_array.tolist()) would fail because
    NamedRowTuple has a broken __array_struct__ attribute.
    """
    # Create NamedRowTuple array using np.empty + assignment pattern.
    # This avoids pandas conversion issues during DataFrame creation.
    # Using [NamedRowTuple(...), ...] would trigger __array_struct__ bug.
    np_array = np.empty(3, dtype=object)
    np_array[0] = NamedRowTuple(["item_1", "value_10"], ["a", "b"], [None, None])
    np_array[1] = NamedRowTuple(["item_2", "value_20"], ["a", "b"], [None, None])
    np_array[2] = NamedRowTuple(["item_3", "value_30"], ["a", "b"], [None, None])

    df = pd.DataFrame({"id": [1, 2, 3], "struct_col": np_array})

    _test_rendering_flow(df, ["id", "struct_col"])

    # stringified values should preserve structure
    oc_df = DataFrame.from_native(df)
    records = oc_df.to_records(mode="json")

    struct_value = records[0]["struct_col"]
    assert isinstance(struct_value, str)
    assert "item_1" in struct_value
    assert "value_10" in struct_value


def test_rendering_with_nested_structures():
    """Test rendering DataFrame with nested dicts/lists."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "nested_col": [
                {"outer": ["inner_1", "inner_2"]},
                {"outer": ["inner_3", "inner_4"]},
                {"outer": ["inner_5", "inner_6"]},
            ],
        }
    )

    _test_rendering_flow(df, ["id", "nested_col"])


def test_rendering_with_mixed_types():
    """Test rendering DataFrame with multiple structured type columns."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "dict_col": [{"a": 1}, {"b": 2}, {"c": 3}],
            "list_col": [[1, 2], [3, 4], [5, 6]],
            "tuple_col": [(1, 2), (3, 4), (5, 6)],
        }
    )

    _test_rendering_flow(df, ["id", "dict_col", "list_col", "tuple_col"])


def test_rendering_with_namedrowtuple_and_missing_values():
    """Test rendering with NamedRowTuple including None values."""
    # Create NamedRowTuple array using np.empty + assignment pattern.
    # Using [NamedRowTuple(...), ...] would trigger __array_struct__ bug.
    np_array = np.empty(4, dtype=object)
    np_array[0] = NamedRowTuple(["item_1", "value_10"], ["a", "b"], [None, None])
    np_array[1] = None
    np_array[2] = NamedRowTuple(["item_2", "value_20"], ["a", "b"], [None, None])
    np_array[3] = NamedRowTuple(["item_1", "value_10"], ["a", "b"], [None, None])

    df = pd.DataFrame({"id": [1, 2, 3, 4], "struct_col": np_array})

    # Should not crash with None values
    analysis_result = analyze_columns(df)

    struct_col = next(col for col in analysis_result if col.name == "struct_col")
    assert struct_col.stats is not None
    assert struct_col.stats.categories is not None

    # Should have "Missing" category
    category_names = [cat["name"] for cat in struct_col.stats.categories]
    assert "Missing" in category_names


def test_rendering_preserves_field_names_in_str_representation():
    """
    Test that NamedRowTuple field names are preserved in stringification.
    """
    # Create NamedRowTuple array using np.empty + assignment pattern.
    # Using [NamedRowTuple(...), ...] would trigger __array_struct__ bug.
    np_array = np.empty(1, dtype=object)
    np_array[0] = NamedRowTuple(
        ["value_a", "value_b"], ["field_a", "field_b"], [None, None]
    )

    df = pd.DataFrame({"struct_col": np_array})

    # Get the stringified representation
    oc_df = DataFrame.from_native(df)
    records = oc_df.to_records(mode="json")

    stringified = records[0]["struct_col"]

    # str(NamedRowTuple) produces something like: (field_a: 'value_a', field_b: 'value_b')
    # This preserves field name information for better display
    assert "field_a: 'value_a'" in stringified
    assert "field_b: 'value_b'" in stringified
