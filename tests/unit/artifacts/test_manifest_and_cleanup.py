"""Tests for the manifest of a stored DataFrame and the cleanup of old frames."""

import contextlib
import json
import os
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List

import pandas as pd
import pyarrow as pa
import pytest

from deepnote_toolkit import artifacts
from deepnote_toolkit.artifacts import dataframe_storage, dataframe_storage_manifest
from tests.unit.helpers.dataframe_storage import (
    DAY,
    FRAME_A,
    FRAME_B,
    NAME,
    arrow_files,
    backdate,
    frame_dir,
    non_arrow_files,
    stored_manifest,
    stored_table,
)


def test_second_write_keeps_one_previous_frame(tmp_project_root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    first = stored_manifest(tmp_project_root)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    second = stored_manifest(tmp_project_root)
    artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)
    third = stored_manifest(tmp_project_root)

    assert first.previous is None
    assert second.previous == first.file
    assert third.previous == second.file
    assert [path.name for path in arrow_files(tmp_project_root)] == sorted(
        [second.file, third.file]
    )
    assert stored_table(tmp_project_root).to_pydict() == {"a": [3]}
    previous = pa.ipc.open_file(frame_dir(tmp_project_root) / third.previous)
    assert previous.read_all().to_pydict() == {"a": [2]}


def test_unreferenced_frames_are_deleted_only_when_stale(
    tmp_project_root: Path,
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    directory = frame_dir(tmp_project_root)
    replaced = directory / stored_manifest(tmp_project_root).file
    backdate(replaced, 2 * DAY)

    def add(name: str, age_seconds: float) -> Path:
        path = directory / name
        path.write_bytes(b"unreferenced")
        backdate(path, age_seconds)
        return path

    stale = add(FRAME_A, 2 * DAY)
    almost_stale = add(FRAME_B, DAY - 3600)
    fresh = add("99999999-8888-4777-8666-555555555555.arrow", 0)
    not_a_frame = [
        add("notes.arrow", 2 * DAY),
        add("x.txt", 2 * DAY),
        add(f"backup-{FRAME_A}", 2 * DAY),
        add(f"{FRAME_A}.bak", 2 * DAY),
    ]
    outside = tmp_project_root / "outside.arrow"
    outside.write_bytes(b"precious")
    link = directory / "77777777-6666-4555-8444-333333333333.arrow"
    link.symlink_to(outside)
    backdate(link, 2 * DAY)

    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert not stale.exists()
    assert almost_stale.exists()
    assert fresh.exists()
    assert all(path.exists() for path in not_a_frame)
    assert link.is_symlink()
    assert outside.read_bytes() == b"precious"
    assert replaced.exists()


_VALID_MANIFEST: Dict[str, Any] = {
    "version": 1,
    "file": FRAME_A,
    "previous": FRAME_B,
    "written_at": "2026-10-15T09:30:00Z",
    "rows": 1,
    "toolkit_version": "1.0.0",
}


def _manifest_bytes(without: str = "", **fields: Any) -> bytes:
    manifest = {**_VALID_MANIFEST, **fields}
    manifest.pop(without, None)
    return json.dumps(manifest).encode()


@pytest.mark.parametrize(
    "manifest_bytes",
    [
        pytest.param(b"{not json", id="invalid-json"),
        pytest.param(b"\x80\x81\x82", id="not-utf8"),
        pytest.param(
            json.dumps(list(_VALID_MANIFEST.items())).encode(), id="not-an-object"
        ),
        pytest.param(_manifest_bytes(version=True), id="version-a-bool"),
        pytest.param(_manifest_bytes(without="file"), id="no-file"),
        pytest.param(_manifest_bytes(rows="1"), id="rows-a-string"),
        pytest.param(
            _manifest_bytes(pad="x" * 5000), id="oversized-truncated-mid-string"
        ),
        pytest.param(
            _manifest_bytes() + b" " * 5000, id="oversized-valid-json-then-spaces"
        ),
    ],
)
def test_unusable_manifest_counts_as_no_manifest(
    tmp_project_root: Path, manifest_bytes: bytes
) -> None:
    directory = frame_dir(tmp_project_root)
    directory.mkdir(parents=True)
    (directory / "manifest.json").write_bytes(manifest_bytes)
    named = [directory / FRAME_A, directory / FRAME_B]
    for path in named:
        path.write_bytes(b"named by the unusable manifest")

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    manifest = stored_manifest(tmp_project_root)
    assert manifest.previous is None
    assert manifest.file not in (FRAME_A, FRAME_B)
    assert stored_table(tmp_project_root).to_pydict() == {"a": [1]}
    assert all(path.exists() for path in named)


@pytest.mark.parametrize(
    "manifest_bytes",
    [
        pytest.param(_manifest_bytes().ljust(4096), id="exactly-4096-bytes"),
        pytest.param(
            _manifest_bytes(written_by_a_newer_toolkit=True), id="unknown-key"
        ),
    ],
)
def test_usable_manifest_is_honoured(
    tmp_project_root: Path, manifest_bytes: bytes
) -> None:
    directory = frame_dir(tmp_project_root)
    directory.mkdir(parents=True)
    assert len(manifest_bytes) <= 4096
    (directory / "manifest.json").write_bytes(manifest_bytes)
    for name in (FRAME_A, FRAME_B):
        (directory / name).write_bytes(b"named by the manifest")

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert stored_manifest(tmp_project_root).previous == FRAME_A
    assert (directory / FRAME_A).exists()
    assert not (directory / FRAME_B).exists()


@pytest.mark.parametrize("absolute", [False, True], ids=["relative", "absolute"])
def test_previous_in_a_parent_directory_is_not_deleted(
    tmp_project_root: Path, absolute: bool
) -> None:
    directory = frame_dir(tmp_project_root)
    directory.mkdir(parents=True)
    # Named like a frame, so checking only the last path component would delete it.
    in_the_parent = directory.parent / FRAME_B
    in_the_parent.write_text("keep")
    previous = str(in_the_parent) if absolute else f"../{FRAME_B}"
    (directory / "manifest.json").write_bytes(_manifest_bytes(previous=previous))

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert in_the_parent.read_text() == "keep"


def test_manifest_nested_too_deeply_to_parse_counts_as_no_manifest(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Before 3.12, json raises RecursionError on the nesting 4 KiB allows."""
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    def too_deep(raw: bytes) -> Any:
        raise RecursionError(
            "maximum recursion depth exceeded while decoding a JSON array"
        )

    monkeypatch.setattr(
        dataframe_storage_manifest,
        "json",
        SimpleNamespace(loads=too_deep, dumps=json.dumps),
    )

    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert stored_manifest(tmp_project_root).previous is None
    assert stored_table(tmp_project_root).to_pydict() == {"a": [2]}


def test_manifest_is_written_after_the_frame_file_is_complete(
    tmp_project_root: Path, arrow_sinks: List[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: List[Dict[str, Any]] = []
    original = dataframe_storage.write_manifest

    def spy(directory: Path, new_file: str, *args: Any) -> None:
        table = pa.ipc.open_file(directory / new_file).read_all()
        seen.append({"closed": arrow_sinks[0].closed, "rows": table.num_rows})
        original(directory, new_file, *args)

    monkeypatch.setattr(dataframe_storage, "write_manifest", spy)

    artifacts.store_dataframe(pd.DataFrame({"a": [1, 2, 3]}), NAME)

    assert seen == [{"closed": True, "rows": 3}]


def test_concurrent_writers_leave_a_complete_current_frame(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = threading.Barrier(2, timeout=10)
    original = dataframe_storage.read_manifest

    def read_then_wait(directory: Path) -> Any:
        manifest = original(directory)
        barrier.wait()
        return manifest

    errors: List[BaseException] = []

    def write(value: int) -> None:
        try:
            artifacts.store_dataframe(pd.DataFrame({"a": [value]}), NAME)
        except BaseException as exc:
            errors.append(exc)

    with monkeypatch.context() as patch:
        patch.setattr(dataframe_storage, "read_manifest", read_then_wait)
        threads = [threading.Thread(target=write, args=(value,)) for value in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            assert not thread.is_alive()

    assert errors == []
    files = arrow_files(tmp_project_root)
    assert len(files) == 2
    current = stored_manifest(tmp_project_root).file
    stray = next(path for path in files if path.name != current)
    for path in files:
        assert pa.ipc.open_file(path).read_all().num_rows == 1

    backdate(stray, 2 * DAY)
    artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)

    assert not stray.exists()
    assert (frame_dir(tmp_project_root) / current).exists()
    assert stored_table(tmp_project_root).to_pydict() == {"a": [3]}


def test_successful_write_leaves_only_the_manifest_beside_the_frames(
    tmp_project_root: Path,
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert non_arrow_files(tmp_project_root) == ["manifest.json"]


def test_stray_pruned_by_another_writer_meanwhile_is_not_a_failure(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    directory = frame_dir(tmp_project_root)
    strays = [directory / FRAME_A, directory / FRAME_B]
    for stray in strays:
        stray.write_bytes(b"unreferenced")
        backdate(stray, 2 * DAY)
    real_scandir = os.scandir

    @contextlib.contextmanager
    def scandir_then_lose_a_stray(path: Any) -> Iterator[Any]:
        with real_scandir(path) as entries:
            listing = list(entries)
        strays[0].unlink()
        yield iter(listing)

    monkeypatch.setattr(dataframe_storage.os, "scandir", scandir_then_lose_a_stray)

    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert not strays[1].exists()
    assert stored_table(tmp_project_root).to_pydict() == {"a": [2]}
