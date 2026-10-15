"""Tests for what a failed or interrupted DataFrame write leaves behind."""

import errno
import os
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, Optional

import pandas as pd
import polars as pl
import pytest
from pyarrow import feather

from deepnote_toolkit import artifacts
from deepnote_toolkit.artifacts import dataframe_storage, dataframe_storage_manifest
from tests.unit.helpers.dataframe_storage import (
    ALWAYS_FAILING,
    DAY,
    FRAME_A,
    NAME,
    VERSION_DEPENDENT,
    arrow_files,
    backdate,
    entries_beside_the_frames,
    frame_dir,
    probe_failure,
    stored_manifest,
    stored_table,
)


def _store_a_good_frame(tmp_project_root: Path) -> bytes:
    artifacts.store_dataframe(pd.DataFrame({"ok": [1]}), NAME)
    return (frame_dir(tmp_project_root) / "manifest.json").read_bytes()


def _store_frame_after_a_good_one(
    tmp_project_root: Path, make_frame: Callable[[], Any]
) -> None:
    _store_a_good_frame(tmp_project_root)
    artifacts.store_dataframe(make_frame(), NAME)


def _store_bad_frame_after_a_good_one(
    tmp_project_root: Path, make_frame: Callable[[], Any], expected_error: type
) -> Dict[str, Any]:
    """Store a good frame, then a bad one.

    Return the good call's manifest bytes and the directory listing after the bad call.
    """
    manifest_bytes = _store_a_good_frame(tmp_project_root)
    with pytest.raises(expected_error) as raised:
        artifacts.store_dataframe(make_frame(), NAME)
    assert raised.type is expected_error
    return {
        "manifest_bytes": manifest_bytes,
        "listing": sorted(path.name for path in frame_dir(tmp_project_root).iterdir()),
    }


@pytest.mark.parametrize("make_frame", ALWAYS_FAILING)
def test_unserializable_frame_raises_and_keeps_the_previous_frame(
    tmp_project_root: Path, make_frame: Callable[[], Any]
) -> None:
    raised_by_pyarrow = probe_failure(make_frame())
    assert raised_by_pyarrow is not None, "pyarrow is expected to refuse this frame"

    left = _store_bad_frame_after_a_good_one(
        tmp_project_root, make_frame, raised_by_pyarrow
    )

    assert (frame_dir(tmp_project_root) / "manifest.json").read_bytes() == left[
        "manifest_bytes"
    ]
    assert left["listing"] == sorted(
        ["manifest.json", stored_manifest(tmp_project_root).file]
    )
    assert stored_table(tmp_project_root).to_pydict() == {"ok": [1]}


@pytest.mark.parametrize("make_frame, rows", VERSION_DEPENDENT)
def test_frames_pyarrow_may_refuse_follow_what_pyarrow_does(
    tmp_project_root: Path, make_frame: Callable[[], Any], rows: int
) -> None:
    raised_by_pyarrow = probe_failure(make_frame())

    if raised_by_pyarrow is None:
        _store_frame_after_a_good_one(tmp_project_root, make_frame)
        assert stored_table(tmp_project_root).num_rows == rows
        return
    left = _store_bad_frame_after_a_good_one(
        tmp_project_root, make_frame, raised_by_pyarrow
    )

    assert (frame_dir(tmp_project_root) / "manifest.json").read_bytes() == left[
        "manifest_bytes"
    ]
    assert left["listing"] == sorted(
        ["manifest.json", stored_manifest(tmp_project_root).file]
    )


def test_polars_object_column_raises_and_keeps_the_previous_frame(
    tmp_project_root: Path,
) -> None:
    artifacts.store_dataframe(pl.DataFrame({"ok": [1]}), NAME)
    manifest_bytes = (frame_dir(tmp_project_root) / "manifest.json").read_bytes()

    with pytest.raises(pl.exceptions.ComputeError) as raised:
        artifacts.store_dataframe(
            pl.DataFrame({"a": pl.Series([object()], dtype=pl.Object)}), NAME
        )

    raised.match("'Object' datatype")
    assert (
        frame_dir(tmp_project_root) / "manifest.json"
    ).read_bytes() == manifest_bytes
    assert len(arrow_files(tmp_project_root)) == 1


def test_partial_file_is_deleted_when_the_write_fails(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def write_some_then_fail(frame: pd.DataFrame, sink: Any, **kwargs: Any) -> None:
        sink.write(b"ARROW1 partial")
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(feather, "write_feather", write_some_then_fail)

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert raised.value.errno == errno.EIO
    assert arrow_files(tmp_project_root) == []
    assert not (frame_dir(tmp_project_root) / "manifest.json").exists()


@pytest.mark.parametrize(
    "error",
    [
        PermissionError(errno.EPERM, "Operation not permitted"),
        PermissionError(errno.EACCES, "Permission denied"),
        OSError(errno.EIO, "Input/output error"),
        OSError(errno.ENOSPC, "No space left on device"),
    ],
    ids=["EPERM", "EACCES", "EIO", "ENOSPC"],
)
def test_error_from_close_raises_and_stores_nothing(
    tmp_project_root: Path, close_error: Dict[str, Optional[OSError]], error: OSError
) -> None:
    """s3fs surfaces a failed upload only when the file is closed."""
    close_error["error"] = error

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert raised.value is error
    assert arrow_files(tmp_project_root) == []
    assert not (frame_dir(tmp_project_root) / "manifest.json").exists()


def test_failed_manifest_read_deletes_the_new_frame(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    frames = arrow_files(tmp_project_root)
    real_open = Path.open

    def open_failing_for_the_manifest(
        self: Path, mode: str = "r", *args: Any, **kwargs: Any
    ) -> Any:
        if self.name == "manifest.json":
            raise OSError(errno.EIO, "Input/output error")
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_failing_for_the_manifest)

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert raised.value.errno == errno.EIO
    assert arrow_files(tmp_project_root) == frames


def test_failure_in_the_manifest_step_deletes_nothing(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    directory = frame_dir(tmp_project_root)
    stale = directory / FRAME_A
    stale.write_bytes(b"unreferenced")
    backdate(stale, 2 * DAY)
    manifest_bytes = (directory / "manifest.json").read_bytes()
    listing = sorted(path.name for path in directory.iterdir())

    def broken_manifest(*args: Any) -> Any:
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(dataframe_storage, "write_manifest", broken_manifest)

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)

    assert raised.value.errno == errno.EIO
    assert (directory / "manifest.json").read_bytes() == manifest_bytes
    entries = {path.name for path in directory.iterdir()}
    assert set(listing) <= entries and len(entries - set(listing)) == 1
    assert stored_table(tmp_project_root).to_pydict() == {"a": [2]}


class _FailingManifestHandle:
    """A temporary manifest whose write() or close() raises `error`."""

    def __init__(self, handle: Any, failing: str, error: BaseException) -> None:
        self._handle = handle
        self._failing = failing
        self._error = error

    def write(self, data: str) -> int:
        if self._failing == "write":
            raise self._error
        return self._handle.write(data)

    def __enter__(self) -> "_FailingManifestHandle":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._handle.close()
        if self._failing == "close":
            raise self._error

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


@pytest.mark.parametrize(
    "failing, error",
    [
        ("write", OSError(errno.ENOSPC, "No space left on device")),
        ("close", OSError(errno.EIO, "Input/output error")),
        ("write", KeyboardInterrupt()),
    ],
    ids=["write", "close", "interrupt"],
)
def test_failed_manifest_write_keeps_the_previous_frame(
    tmp_project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing: str,
    error: BaseException,
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    manifest_bytes = (frame_dir(tmp_project_root) / "manifest.json").read_bytes()
    real_temporary_file = tempfile.NamedTemporaryFile

    def failing_temporary_file(*args: Any, **kwargs: Any) -> Any:
        handle = real_temporary_file(*args, **kwargs)
        return _FailingManifestHandle(handle, failing, error)

    monkeypatch.setattr(
        dataframe_storage_manifest,
        "tempfile",
        SimpleNamespace(NamedTemporaryFile=failing_temporary_file),
    )

    with pytest.raises(type(error)) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert raised.value is error
    assert (
        frame_dir(tmp_project_root) / "manifest.json"
    ).read_bytes() == manifest_bytes
    assert stored_table(tmp_project_root).to_pydict() == {"a": [1]}
    assert entries_beside_the_frames(tmp_project_root) == ["manifest.json"]


def _copy_then_fail(source: str, destination: Path) -> None:
    """s3fs's rename: the copy lands, then deleting the source fails."""
    shutil.copyfile(source, destination)
    raise OSError(errno.EIO, "Input/output error")


def test_rename_failing_after_the_copy_keeps_the_frame_it_put_in_place(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    monkeypatch.setattr(dataframe_storage_manifest.os, "replace", _copy_then_fail)

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert raised.value.errno == errno.EIO
    assert stored_table(tmp_project_root).to_pydict() == {"a": [2]}


@pytest.mark.parametrize("error", [errno.EIO, errno.EROFS], ids=["EIO", "EROFS"])
def test_failure_after_the_manifest_step_keeps_the_new_frame(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch, error: int
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    previous = stored_manifest(tmp_project_root).previous
    assert previous is not None
    replaced = frame_dir(tmp_project_root) / previous
    real_unlink = Path.unlink

    def unlink_failing_for_the_replaced_frame(
        self: Path, *args: Any, **kwargs: Any
    ) -> None:
        if self == replaced:
            raise OSError(error, os.strerror(error))
        real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink_failing_for_the_replaced_frame)

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)

    assert raised.value.errno == error
    assert stored_table(tmp_project_root).to_pydict() == {"a": [3]}


def test_interrupt_during_the_write_raises_and_keeps_the_previous_frame(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    interrupt = KeyboardInterrupt()

    def write_some_then_interrupt(
        frame: pd.DataFrame, sink: Any, **kwargs: Any
    ) -> None:
        sink.write(b"ARROW1 partial")
        raise interrupt

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    manifest_bytes = (frame_dir(tmp_project_root) / "manifest.json").read_bytes()
    monkeypatch.setattr(feather, "write_feather", write_some_then_interrupt)

    with pytest.raises(KeyboardInterrupt) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert raised.value is interrupt
    assert (
        frame_dir(tmp_project_root) / "manifest.json"
    ).read_bytes() == manifest_bytes
    assert len(arrow_files(tmp_project_root)) == 1
