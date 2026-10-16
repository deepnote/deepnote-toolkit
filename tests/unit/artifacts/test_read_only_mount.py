"""Tests for storing and deleting DataFrames on a read-only mount."""

import errno
import shutil
from pathlib import Path
from typing import Any
from unittest import mock

import pandas as pd
import pytest
from pyarrow import feather

from deepnote_toolkit import artifacts
from tests.unit.helpers.dataframe_storage import (
    NAME,
    arrow_files,
    frame_dir,
    read_only,
    stored_table,
)


def test_read_only_mount_stores_nothing_and_does_not_convert(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    read_only(monkeypatch)

    with mock.patch.object(feather, "write_feather", autospec=True) as write:
        result = artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert result is None
    write.assert_not_called()
    assert list(tmp_project_root.iterdir()) == []


def test_read_only_mount_keeps_the_stored_frame_and_does_not_convert(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    manifest_bytes = (frame_dir(tmp_project_root) / "manifest.json").read_bytes()
    read_only(monkeypatch)

    with mock.patch.object(feather, "write_feather", autospec=True) as write:
        result = artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert result is None
    write.assert_not_called()
    assert (
        frame_dir(tmp_project_root) / "manifest.json"
    ).read_bytes() == manifest_bytes
    assert stored_table(tmp_project_root).to_pydict() == {"a": [1]}


def test_erofs_from_the_write_stores_nothing(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(frame: pd.DataFrame, sink: Any, **kwargs: Any) -> None:
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(feather, "write_feather", refuse)

    result = artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert result is None
    assert arrow_files(tmp_project_root) == []
    assert not (frame_dir(tmp_project_root) / "manifest.json").exists()


def test_delete_on_a_read_only_mount_deletes_nothing(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    read_only(monkeypatch)

    assert artifacts.delete_dataframe(NAME) is False

    assert len(arrow_files(tmp_project_root)) == 1


def test_delete_returns_false_when_the_filesystem_refuses_with_erofs(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    def refuse(path: Any) -> None:
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(shutil, "rmtree", refuse)

    assert artifacts.delete_dataframe(NAME) is False
