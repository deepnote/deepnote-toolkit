import json

import pytest

from deepnote_toolkit.ocelots.pandas.utils import safe_convert_to_string


def test_safe_convert_to_string_dict():
    dict_value = {"a": "x", "b": "y"}
    result = safe_convert_to_string(dict_value)

    assert isinstance(result, str)
    parsed = json.loads(result)
    assert parsed == dict_value


def test_safe_convert_to_string_tuple():
    tuple_value = (1, "x", True)
    result = safe_convert_to_string(tuple_value)

    assert isinstance(result, str)
    parsed = json.loads(result)
    assert parsed == [1, "x", True]


def test_safe_convert_to_string_list():
    list_value = ["a", "b", "c"]
    result = safe_convert_to_string(list_value)

    assert isinstance(result, str)
    parsed = json.loads(result)
    assert parsed == list_value


def test_safe_convert_to_string_nested_structures():
    nested_value = {"key": "value", "nested": {"inner": [1, 2, 3]}}
    result = safe_convert_to_string(nested_value)

    parsed = json.loads(result)
    assert parsed == nested_value


def test_safe_convert_to_string_regular_values():
    assert safe_convert_to_string("hello") == "hello"

    assert safe_convert_to_string(42) == "42"
    assert safe_convert_to_string(3.14) == "3.14"

    assert safe_convert_to_string(True) == "True"

    assert safe_convert_to_string(None) == "None"


def test_safe_convert_to_string_unconvertible():

    class UnconvertibleObject:
        def __str__(self):
            raise ValueError("Cannot convert")

        def __repr__(self):
            raise ValueError("Cannot represent")

    result = safe_convert_to_string(UnconvertibleObject())
    assert result == "<unconvertible>"


# Tests for Trino-specific types
def test_safe_convert_to_string_trino_namedrowtuple():
    """Test that Trino's NamedRowTuple is converted to valid JSON strings."""
    pytest.importorskip("trino.types")
    from trino.types import NamedRowTuple

    # Create a NamedRowTuple with field names and values (as returned by Trino)
    row = NamedRowTuple(
        values=["item_1", "value_10"], names=["a", "b"], types=[None, None]
    )

    result = safe_convert_to_string(row)

    assert isinstance(result, str)
    parsed = json.loads(result)
    assert parsed == ["item_1", "value_10"]
    assert row.a == "item_1"
    assert row.b == "value_10"


def test_safe_convert_to_string_trino_array():
    """Test that Trino arrays (returned as Python lists) are converted to valid JSON."""

    # Trino returns ARRAY types as Python lists
    trino_array = ["tag_1", "item", "test"]

    result = safe_convert_to_string(trino_array)

    assert isinstance(result, str)

    parsed = json.loads(result)
    assert parsed == trino_array
    assert '"tag_1"' in result
    assert "'tag_1'" not in result


def test_safe_convert_to_string_trino_nested_array():
    """Test that nested Trino arrays are converted to valid JSON."""

    # Trino returns nested ARRAY types as nested Python lists
    nested_array = [[1, 2], [3, 4]]

    result = safe_convert_to_string(nested_array)

    parsed = json.loads(result)
    assert parsed == nested_array
    assert parsed[0] == [1, 2]
    assert parsed[1] == [3, 4]
