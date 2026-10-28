"""Tests for how the artifacts API checks the names and values it is given."""

from pathlib import Path
from typing import Any, Callable

import pandas as pd
import polars as pl
import pytest

from deepnote_toolkit import artifacts
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview
from tests.unit.helpers.dataframe_storage import (
    FRAME_A,
    NAME,
    arrow_files,
    stored_table,
)


@pytest.mark.parametrize("name", ["x", "x" * 128, "Rev-2026_Q3"])
def test_valid_names_are_accepted(tmp_project_root: Path, name: str) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)

    assert stored_table(tmp_project_root, name).to_pydict() == {"a": [1]}


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
def test_invalid_name_raises_before_anything_is_written(
    tmp_project_root: Path, name: Any
) -> None:
    with pytest.raises(ValueError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)

    assert str(raised.value).endswith(f"got {name!r}")
    assert list(tmp_project_root.iterdir()) == []


def _query_preview() -> DeepnoteQueryPreview:
    return DeepnoteQueryPreview({"a": [1]}, deepnote_query="select 1")


def _cleared_query_preview() -> DeepnoteQueryPreview:
    preview = _query_preview()
    # Mutating the frame clears its attached SQL query metadata.
    preview["b"] = 2
    return preview


UNSUPPORTED_VALUES = [
    pytest.param(lambda: 1, id="int"),
    pytest.param(lambda: None, id="none"),
    pytest.param(lambda: pd.Series([1, 2]), id="series"),
    pytest.param(lambda: {"a": [1]}, id="dict"),
    pytest.param(lambda: pl.DataFrame({"a": [1]}).lazy(), id="lazyframe"),
]

QUERY_PREVIEWS = [
    pytest.param(_query_preview, id="query-preview"),
    pytest.param(_cleared_query_preview, id="cleared-query-preview"),
    pytest.param(lambda: _query_preview().copy(), id="copied-query-preview"),
]


@pytest.mark.parametrize("make_value", UNSUPPORTED_VALUES)
def test_unsupported_value_raises_type_error(
    tmp_project_root: Path, make_value: Callable[[], Any]
) -> None:
    with pytest.raises(TypeError, match="pandas or polars DataFrame"):
        artifacts.store_dataframe(make_value(), NAME)

    assert list(tmp_project_root.iterdir()) == []


@pytest.mark.parametrize("make_preview", QUERY_PREVIEWS)
def test_query_preview_raises_type_error(
    tmp_project_root: Path, make_preview: Callable[[], DeepnoteQueryPreview]
) -> None:
    with pytest.raises(TypeError, match="SQL query preview"):
        artifacts.store_dataframe(make_preview(), NAME)

    assert list(tmp_project_root.iterdir()) == []


def test_pyspark_frame_raises_type_error(tmp_project_root: Path, spark: Any) -> None:
    with pytest.raises(TypeError, match="pandas or polars DataFrame"):
        artifacts.store_dataframe(spark.range(3), NAME)

    assert list(tmp_project_root.iterdir()) == []


def test_pandas_on_spark_frame_raises_type_error(
    tmp_project_root: Path,
    spark_without_ansi_mode: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pyspark.pandas warns on import unless told that pyarrow may ignore time zones
    monkeypatch.setenv("PYARROW_IGNORE_TIMEZONE", "1")
    # pyspark 3.5, which Python < 3.13 gets, imports distutils, gone since Python 3.12
    pytest.importorskip("pyspark.pandas")
    frame = spark_without_ansi_mode.range(3).pandas_api()

    with pytest.raises(TypeError) as raised:
        artifacts.store_dataframe(frame, NAME)

    raised.match("pandas or polars DataFrame")
    assert list(tmp_project_root.iterdir()) == []


# Let through, each of these would point delete_dataframe outside the name's own
# directory. Only the frame file is not a directory, so only it catches a name check
# moved below the is_dir() return.
@pytest.mark.parametrize(
    "name",
    [
        pytest.param("", id="empty"),
        pytest.param(".", id="dot"),
        pytest.param("..", id="dotdot"),
        pytest.param("../x", id="traversal"),
        pytest.param(f"../{FRAME_A}", id="traversal-to-a-frame-file"),
    ],
)
def test_delete_with_an_invalid_name_raises_and_deletes_nothing(
    tmp_project_root: Path, name: Any
) -> None:
    (tmp_project_root / "x").mkdir()
    (tmp_project_root / "x" / "keep.txt").write_text("keep")
    (tmp_project_root / FRAME_A).write_text("keep")
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    with pytest.raises(ValueError) as raised:
        artifacts.delete_dataframe(name)

    assert str(raised.value).endswith(f"got {name!r}")
    assert (tmp_project_root / "x" / "keep.txt").read_text() == "keep"
    assert (tmp_project_root / FRAME_A).read_text() == "keep"
    assert len(arrow_files(tmp_project_root)) == 1
