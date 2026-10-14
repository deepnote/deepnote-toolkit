"""Tests for how the artifacts API checks the names and values it is given."""

from pathlib import Path
from typing import Any, Callable
from unittest import mock

import pandas as pd
import polars as pl
import pytest
from pyarrow import feather

from deepnote_toolkit import artifacts
from tests.unit.helpers.dataframe_storage import (
    NAME,
    arrow_files,
    cleared_query_preview,
    query_preview,
    read_only,
    stored_table,
)


@pytest.mark.parametrize("name", ["x", "x" * 128, "Rev-2026_Q3"])
def test_valid_names_are_accepted(root: Path, name: str) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)

    assert stored_table(root, name).to_pydict() == {"a": [1]}


INVALID_NAMES = [
    pytest.param("", id="empty"),
    pytest.param(".", id="dot"),
    pytest.param("..", id="dotdot"),
    pytest.param("../x", id="traversal"),
    pytest.param("a/b", id="slash"),
    pytest.param("a\\b", id="backslash"),
    pytest.param("a b", id="space"),
    pytest.param("revenue\n", id="trailing-newline"),
    pytest.param("é", id="non-ascii"),
    pytest.param("x" * 129, id="too-long"),
    pytest.param(5, id="int"),
    pytest.param(None, id="none"),
]


@pytest.mark.parametrize("name", INVALID_NAMES)
def test_invalid_name_raises_before_anything_is_written(root: Path, name: Any) -> None:
    with mock.patch.object(feather, "write_feather", autospec=True) as write:
        with pytest.raises(ValueError):
            artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)

    write.assert_not_called()
    assert list(root.iterdir()) == []


@pytest.mark.parametrize("name", INVALID_NAMES)
def test_invalid_name_raises_on_a_read_only_mount(
    root: Path, monkeypatch: pytest.MonkeyPatch, name: Any
) -> None:
    read_only(monkeypatch)

    with pytest.raises(ValueError):
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)


UNSUPPORTED_VALUES = [
    pytest.param(lambda: 1, id="int"),
    pytest.param(lambda: None, id="none"),
    pytest.param(lambda: pd.Series([1, 2]), id="series"),
    pytest.param(lambda: {"a": [1]}, id="dict"),
    pytest.param(lambda: pl.DataFrame({"a": [1]}).lazy(), id="lazyframe"),
]

QUERY_PREVIEWS = [
    pytest.param(query_preview, id="query-preview"),
    pytest.param(cleared_query_preview, id="cleared-query-preview"),
    pytest.param(lambda: query_preview().copy(), id="copied-query-preview"),
]


@pytest.mark.parametrize("make_value", UNSUPPORTED_VALUES)
def test_unsupported_value_raises_type_error(
    root: Path, make_value: Callable[[], Any]
) -> None:
    with pytest.raises(TypeError, match="pandas or polars DataFrame"):
        artifacts.store_dataframe(make_value(), NAME)

    assert list(root.iterdir()) == []


@pytest.mark.parametrize("make_preview", QUERY_PREVIEWS)
def test_query_preview_raises_type_error(
    root: Path, make_preview: Callable[[], Any]
) -> None:
    with pytest.raises(TypeError, match="SQL query preview"):
        artifacts.store_dataframe(make_preview(), NAME)

    assert list(root.iterdir()) == []


@pytest.mark.parametrize("make_value", UNSUPPORTED_VALUES + QUERY_PREVIEWS)
def test_unsupported_value_raises_type_error_on_a_read_only_mount(
    root: Path, monkeypatch: pytest.MonkeyPatch, make_value: Callable[[], Any]
) -> None:
    read_only(monkeypatch)

    with pytest.raises(TypeError):
        artifacts.store_dataframe(make_value(), NAME)


def test_pyspark_frame_raises_type_error(root: Path, spark: Any) -> None:
    with pytest.raises(TypeError, match="pandas or polars DataFrame"):
        artifacts.store_dataframe(spark.range(3), NAME)

    assert list(root.iterdir()) == []


def test_pandas_on_spark_frame_raises_type_error(
    root: Path, spark_without_ansi_mode: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # pyspark.pandas warns on import unless told that pyarrow may ignore time zones
    monkeypatch.setenv("PYARROW_IGNORE_TIMEZONE", "1")
    # pyspark 3.5, which Python < 3.13 gets, imports distutils, gone since Python 3.12
    pytest.importorskip("pyspark.pandas")
    frame = spark_without_ansi_mode.range(3).pandas_api()

    with pytest.raises(TypeError):
        artifacts.store_dataframe(frame, NAME)

    assert list(root.iterdir()) == []


@pytest.mark.parametrize("name", INVALID_NAMES)
def test_delete_with_an_invalid_name_raises_and_deletes_nothing(
    root: Path, name: Any
) -> None:
    (root / "x").mkdir()
    (root / "x" / "keep.txt").write_text("keep")
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    with pytest.raises(ValueError):
        artifacts.delete_dataframe(name)

    assert (root / "x" / "keep.txt").read_text() == "keep"
    assert len(arrow_files(root)) == 1
