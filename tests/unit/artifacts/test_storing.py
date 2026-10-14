"""Tests for what the artifacts API writes when it stores a named DataFrame."""

import copy
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pytest

import deepnote_toolkit
from deepnote_toolkit import artifacts
from tests.unit.helpers.dataframe_storage import (
    NAME,
    arrow_files,
    stored_manifest,
    stored_table,
)

UUID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def test_stores_pandas_frame_and_manifest(root: Path) -> None:
    reference_id = artifacts.store_dataframe(
        pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}), NAME
    )

    assert reference_id is not None
    assert re.fullmatch(UUID_PATTERN, reference_id)
    assert [path.name for path in arrow_files(root)] == [f"{reference_id}.arrow"]
    assert stored_table(root).to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    manifest = stored_manifest(root)
    assert set(manifest) == {"version", "file", "written_at", "rows", "toolkit_version"}
    assert manifest["version"] == 1
    assert manifest["file"] == f"{reference_id}.arrow"
    assert manifest["rows"] == 3
    assert manifest["toolkit_version"] == deepnote_toolkit.__version__
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", manifest["written_at"])


@pytest.mark.usefixtures("tokyo_time")
def test_manifest_time_is_utc(root: Path) -> None:
    before = datetime.now(timezone.utc)

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    written = datetime.strptime(
        stored_manifest(root)["written_at"], "%Y-%m-%dT%H:%M:%SZ"
    )
    assert abs((written.replace(tzinfo=timezone.utc) - before).total_seconds()) < 60


def test_stores_polars_frame(root: Path) -> None:
    reference_id = artifacts.store_dataframe(
        pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}), NAME
    )

    assert stored_manifest(root)["file"] == f"{reference_id}.arrow"
    assert stored_table(root).to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    assert stored_manifest(root)["rows"] == 3


def test_writing_does_not_change_the_callers_frame(root: Path) -> None:
    frame = pd.DataFrame({1: [1, 2], 2: [3, 4]}, index=pd.Index(["a", "b"], name="idx"))
    before = copy.deepcopy(frame)

    artifacts.store_dataframe(frame, NAME)

    assert len(arrow_files(root)) == 1
    pd.testing.assert_frame_equal(frame, before)


def test_filtered_frame_is_stored_with_its_row_labels(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(self: pd.DataFrame, *args: Any, **kwargs: Any) -> None:
        raise ValueError("feather does not support serializing a non-default index")

    # pandas >= 2.1 accepts this frame, so make to_feather refuse it as < 2.1 does.
    monkeypatch.setattr(pd.DataFrame, "to_feather", refuse)
    frame = pd.DataFrame({"a": [5, 6, 7, 8]})

    artifacts.store_dataframe(frame[frame.a != 7], NAME)

    assert stored_table(root).to_pydict() == {
        "a": [5, 6, 8],
        "__index_level_0__": [0, 1, 3],
    }


def test_groupby_keys_are_stored_after_the_data_columns(root: Path) -> None:
    frame = pd.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]}).groupby("k").sum()

    artifacts.store_dataframe(frame, NAME)

    assert stored_table(root).schema.names == ["v", "k"]


def test_transposed_frame_is_stored_with_string_names(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"x": [1, 2], "y": [3, 4]}).T, NAME)

    assert stored_table(root).schema.names == ["0", "1", "__index_level_0__"]


@pytest.mark.parametrize(
    "frame",
    [pd.DataFrame({"a": []}), pl.DataFrame({"a": []})],
    ids=["pandas", "polars"],
)
def test_empty_frame_is_stored(root: Path, frame: Any) -> None:
    artifacts.store_dataframe(frame, NAME)

    assert stored_table(root).num_rows == 0
    assert stored_manifest(root)["rows"] == 0


def test_names_are_stored_apart(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), "first")
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), "second")

    assert stored_table(root, "first").to_pydict() == {"a": [1]}
    assert stored_table(root, "second").to_pydict() == {"a": [2]}


def test_the_last_call_with_a_name_wins(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pl.DataFrame({"a": [2]}), NAME)

    assert stored_table(root).to_pydict() == {"a": [2]}


def test_functions_are_exported_from_the_package() -> None:
    assert deepnote_toolkit.artifacts is artifacts
    assert "artifacts" in deepnote_toolkit.__all__
