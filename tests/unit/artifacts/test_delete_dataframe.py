"""Tests for the artifacts API that deletes named DataFrames."""

from pathlib import Path

import pandas as pd
import pytest

from deepnote_toolkit import artifacts
from tests.unit.helpers.dataframe_storage import (
    NAME,
    frame_dir,
    stored_manifest,
    stored_table,
)


def test_delete_removes_the_frames_of_the_name_only(tmp_project_root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [3]}), "other")
    neighbour = tmp_project_root / "deepnote_dataframes" / "revenue-2026"
    neighbour.mkdir()
    (neighbour / "keep.txt").write_text("keep")
    (tmp_project_root / "notes.txt").write_text("keep")

    assert artifacts.delete_dataframe(NAME) is True

    assert not frame_dir(tmp_project_root).exists()
    assert stored_table(tmp_project_root, "other").to_pydict() == {"a": [3]}
    assert (neighbour / "keep.txt").read_text() == "keep"
    assert (tmp_project_root / "notes.txt").read_text() == "keep"


def test_delete_returns_false_when_there_is_nothing_to_delete(
    tmp_project_root: Path,
) -> None:
    assert artifacts.delete_dataframe(NAME) is False
    assert list(tmp_project_root.iterdir()) == []


def test_delete_twice_returns_false_the_second_time(tmp_project_root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert artifacts.delete_dataframe(NAME) is True
    assert artifacts.delete_dataframe(NAME) is False


def test_a_name_can_be_stored_again_after_it_was_deleted(
    tmp_project_root: Path,
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.delete_dataframe(NAME)

    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert stored_manifest(tmp_project_root).previous is None
    assert stored_table(tmp_project_root).to_pydict() == {"a": [2]}


def test_delete_does_not_follow_a_link_out_of_the_frames_directory(
    tmp_project_root: Path,
) -> None:
    outside = tmp_project_root / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    (tmp_project_root / "deepnote_dataframes").mkdir()
    (tmp_project_root / "deepnote_dataframes" / NAME).symlink_to(
        outside, target_is_directory=True
    )

    with pytest.raises(OSError) as raised:
        artifacts.delete_dataframe(NAME)

    assert raised.value.args == ("Cannot call rmtree on a symbolic link",)
    assert (outside / "keep.txt").read_text() == "keep"
