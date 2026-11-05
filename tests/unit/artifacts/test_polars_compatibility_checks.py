"""Tests for the polars versions and data types that DataFrame storage refuses."""

from pathlib import Path
from typing import Any

import polars as pl
import pytest

from deepnote_toolkit import artifacts
from deepnote_toolkit.artifacts import _polars_compatibility_checks
from tests.unit.helpers.dataframe_storage import (
    NAME,
    arrow_files,
    frame_dir,
    stored_table,
)


@pytest.mark.parametrize(
    "dtype",
    [
        pytest.param(pl.Object, id="object"),
        pytest.param(
            pl.List(pl.Object),
            id="list-of-objects",
            # polars panics, unraisably, while it builds a List(Object) column.
            marks=pytest.mark.filterwarnings(
                "ignore::pytest.PytestUnraisableExceptionWarning"
            ),
        ),
        pytest.param(pl.Array(pl.Object, 1), id="array-of-objects"),
        pytest.param(
            pl.List(pl.List(pl.Object)),
            id="list-of-lists-of-objects",
            marks=pytest.mark.filterwarnings(
                "ignore::pytest.PytestUnraisableExceptionWarning"
            ),
        ),
    ],
)
def test_polars_object_column_raises_and_keeps_the_previous_frame(
    tmp_project_root: Path, dtype: Any
) -> None:
    artifacts.store_dataframe(pl.DataFrame({"ok": [1]}), NAME)
    manifest_bytes = (frame_dir(tmp_project_root) / "manifest.json").read_bytes()
    frame = pl.DataFrame({"ok": [1]}).with_columns(pl.lit(None, dtype=dtype).alias("a"))

    with pytest.raises(TypeError) as raised:
        artifacts.store_dataframe(frame, NAME)

    raised.match(r"Object columns cannot be stored, got \['a'\]")
    assert (
        frame_dir(tmp_project_root) / "manifest.json"
    ).read_bytes() == manifest_bytes
    assert len(arrow_files(tmp_project_root)) == 1


def test_object_in_a_struct_is_refused() -> None:
    # polars refuses to build such a frame, so only the dtype walk can be checked.
    assert _polars_compatibility_checks._contains_polars_dtype(
        pl.Struct({"a": pl.Int64, "o": pl.Object}), pl.Object
    )


@pytest.mark.parametrize(
    "polars_version, dtype, message",
    [
        pytest.param(
            "1.35.0",
            pl.Int64,
            r"needs polars 1\.35\.1 or newer, found 1\.35\.0",
            id="before-1.35.1",
        ),
        pytest.param(
            "1.43.1",
            pl.Array(pl.Int64, 2),
            r"Array columns needs polars 1\.43\.2 or newer, found 1\.43\.1, "
            r"got \['a'\]",
            id="array-before-1.43.2",
        ),
        pytest.param(
            "1.43.1",
            pl.List(pl.Array(pl.Int64, 2)),
            r"Array columns needs polars 1\.43\.2",
            id="list-of-arrays-before-1.43.2",
        ),
        pytest.param(
            "1.43.1",
            pl.Struct({"x": pl.Array(pl.Int64, 2)}),
            r"Array columns needs polars 1\.43\.2",
            id="struct-of-arrays-before-1.43.2",
        ),
    ],
)
def test_polars_too_old_for_the_frame_raises_and_keeps_the_previous_frame(
    tmp_project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    polars_version: str,
    dtype: Any,
    message: str,
) -> None:
    artifacts.store_dataframe(pl.DataFrame({"ok": [1]}), NAME)
    manifest_bytes = (frame_dir(tmp_project_root) / "manifest.json").read_bytes()
    frame = pl.DataFrame({"ok": [1]}).with_columns(pl.lit(None, dtype=dtype).alias("a"))
    monkeypatch.setattr(pl, "__version__", polars_version)

    with pytest.raises(TypeError) as raised:
        artifacts.store_dataframe(frame, NAME)

    raised.match(message)
    assert (
        frame_dir(tmp_project_root) / "manifest.json"
    ).read_bytes() == manifest_bytes
    assert len(arrow_files(tmp_project_root)) == 1


@pytest.mark.parametrize(
    "polars_version, dtype",
    [
        pytest.param("1.35.1", pl.Int64, id="1.35.1"),
        pytest.param("1.43.2", pl.Array(pl.Int64, 2), id="array-from-1.43.2"),
        pytest.param("1.43.1", pl.Struct({"x": pl.Int64}), id="struct-before-1.43.2"),
    ],
)
def test_polars_frame_the_version_checks_allow_is_stored(
    tmp_project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    polars_version: str,
    dtype: Any,
) -> None:
    frame = pl.DataFrame({"ok": [1]}).with_columns(pl.lit(None, dtype=dtype).alias("a"))
    monkeypatch.setattr(pl, "__version__", polars_version)

    artifacts.store_dataframe(frame, NAME)

    assert stored_table(tmp_project_root).to_pydict() == {"ok": [1], "a": [None]}
