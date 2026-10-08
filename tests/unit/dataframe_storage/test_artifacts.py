"""Tests for the artifacts API that stores and deletes named DataFrames."""

import contextlib
import copy
import errno
import json
import os
import re
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterator, List, Optional
from unittest import mock

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

import deepnote_toolkit
from deepnote_toolkit import artifacts, dataframe_storage, dataframe_storage_manifest
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview
from tests.unit.helpers.dataframe_storage import (
    ALWAYS_FAILING,
    VERSION_DEPENDENT,
    interrupt_at,
    probe_failure,
)

NAME = "revenue"
UUID_PATTERN = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
DAY = 24 * 60 * 60
FRAME_A = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.arrow"
FRAME_B = "11111111-2222-4333-8444-555555555555.arrow"


def _frame_dir(root: Path, name: str = NAME) -> Path:
    return root / "deepnote_dataframes" / name


def _arrow_files(root: Path, name: str = NAME) -> List[Path]:
    return sorted(_frame_dir(root, name).glob("*.arrow"))


def _manifest(root: Path, name: str = NAME) -> Dict[str, Any]:
    return json.loads((_frame_dir(root, name) / "manifest.json").read_text())


def _stored_table(root: Path, name: str = NAME) -> pa.Table:
    """The frame the name's manifest currently names."""
    path = _frame_dir(root, name) / _manifest(root, name)["file"]
    return pa.ipc.open_file(path).read_all()


def _backdate(path: Path, age_seconds: float) -> None:
    mtime = time.time() - age_seconds
    os.utime(path, (mtime, mtime), follow_symlinks=False)


def _read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        dataframe_storage.os,
        "statvfs",
        lambda path: SimpleNamespace(f_flag=os.ST_RDONLY),
    )


def test_stores_pandas_frame_and_manifest(root: Path) -> None:
    reference_id = artifacts.store_dataframe(
        pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}), NAME
    )

    assert reference_id is not None
    assert re.fullmatch(UUID_PATTERN, reference_id)
    assert [path.name for path in _arrow_files(root)] == [f"{reference_id}.arrow"]
    assert _stored_table(root).to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    manifest = _manifest(root)
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

    written = datetime.strptime(_manifest(root)["written_at"], "%Y-%m-%dT%H:%M:%SZ")
    assert abs((written.replace(tzinfo=timezone.utc) - before).total_seconds()) < 60


def test_stores_polars_frame(root: Path) -> None:
    reference_id = artifacts.store_dataframe(
        pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}), NAME
    )

    assert _manifest(root)["file"] == f"{reference_id}.arrow"
    assert _stored_table(root).to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    assert _manifest(root)["rows"] == 3


def test_writing_does_not_change_the_callers_frame(root: Path) -> None:
    frame = pd.DataFrame({1: [1, 2], 2: [3, 4]}, index=pd.Index(["a", "b"], name="idx"))
    before = copy.deepcopy(frame)

    artifacts.store_dataframe(frame, NAME)

    assert len(_arrow_files(root)) == 1
    pd.testing.assert_frame_equal(frame, before)


def test_groupby_keys_are_stored_after_the_data_columns(root: Path) -> None:
    frame = pd.DataFrame({"k": ["a", "a", "b"], "v": [1, 2, 3]}).groupby("k").sum()

    artifacts.store_dataframe(frame, NAME)

    assert _stored_table(root).schema.names == ["v", "k"]


def test_transposed_frame_is_stored_with_string_names(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"x": [1, 2], "y": [3, 4]}).T, NAME)

    assert _stored_table(root).schema.names == ["0", "1", "__index_level_0__"]


@pytest.mark.parametrize(
    "frame",
    [pd.DataFrame({"a": []}), pl.DataFrame({"a": []})],
    ids=["pandas", "polars"],
)
def test_empty_frame_is_stored(root: Path, frame: Any) -> None:
    artifacts.store_dataframe(frame, NAME)

    assert _stored_table(root).num_rows == 0
    assert _manifest(root)["rows"] == 0


def test_names_are_stored_apart(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), "first")
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), "second")

    assert _stored_table(root, "first").to_pydict() == {"a": [1]}
    assert _stored_table(root, "second").to_pydict() == {"a": [2]}


def test_the_last_call_with_a_name_wins(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pl.DataFrame({"a": [2]}), NAME)

    assert _stored_table(root).to_pydict() == {"a": [2]}


@pytest.mark.parametrize("name", ["x", "x" * 128, "Rev-2026_Q3"])
def test_valid_names_are_accepted(root: Path, name: str) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)

    assert _stored_table(root, name).to_pydict() == {"a": [1]}


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
    with mock.patch.object(pd.DataFrame, "to_feather", autospec=True) as to_feather:
        with pytest.raises(ValueError):
            artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)

    to_feather.assert_not_called()
    assert list(root.iterdir()) == []


@pytest.mark.parametrize("name", INVALID_NAMES)
def test_invalid_name_raises_on_a_read_only_mount(
    root: Path, monkeypatch: pytest.MonkeyPatch, name: Any
) -> None:
    _read_only(monkeypatch)

    with pytest.raises(ValueError):
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), name)


def _query_preview() -> DeepnoteQueryPreview:
    return DeepnoteQueryPreview({"a": [1]}, deepnote_query="select 1")


def _cleared_query_preview() -> DeepnoteQueryPreview:
    preview = _query_preview()
    preview["b"] = 2
    return preview


UNSUPPORTED_VALUES = [
    pytest.param(lambda: 1, id="int"),
    pytest.param(lambda: None, id="none"),
    pytest.param(lambda: pd.Series([1, 2]), id="series"),
    pytest.param(lambda: {"a": [1]}, id="dict"),
    pytest.param(lambda: pl.DataFrame({"a": [1]}).lazy(), id="lazyframe"),
    pytest.param(_query_preview, id="query-preview"),
    pytest.param(_cleared_query_preview, id="cleared-query-preview"),
    pytest.param(lambda: _query_preview().copy(), id="copied-query-preview"),
]


@pytest.mark.parametrize("make_value", UNSUPPORTED_VALUES)
def test_unsupported_value_raises_type_error(
    root: Path, make_value: Callable[[], Any]
) -> None:
    with pytest.raises(TypeError, match="pandas or polars DataFrame"):
        artifacts.store_dataframe(make_value(), NAME)

    assert list(root.iterdir()) == []


@pytest.mark.parametrize("make_value", UNSUPPORTED_VALUES)
def test_unsupported_value_raises_type_error_on_a_read_only_mount(
    root: Path, monkeypatch: pytest.MonkeyPatch, make_value: Callable[[], Any]
) -> None:
    _read_only(monkeypatch)

    with pytest.raises(TypeError):
        artifacts.store_dataframe(make_value(), NAME)


def test_pyspark_frame_raises_type_error(root: Path, spark: Any) -> None:
    with pytest.raises(TypeError, match="pandas or polars DataFrame"):
        artifacts.store_dataframe(spark.range(3), NAME)

    assert list(root.iterdir()) == []


def test_pandas_on_spark_frame_raises_type_error(
    root: Path, spark: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # pyspark.pandas warns on import unless told that pyarrow may ignore time zones
    monkeypatch.setenv("PYARROW_IGNORE_TIMEZONE", "1")
    # pyspark 3.5, which Python < 3.13 gets, imports distutils, gone since Python 3.12
    pytest.importorskip("pyspark.pandas")
    # pandas-on-Spark refuses to run under ANSI mode, which Spark 4 enables by default
    ansi_mode = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        frame = spark.range(3).pandas_api()
    finally:
        spark.conf.set("spark.sql.ansi.enabled", ansi_mode)

    with pytest.raises(TypeError):
        artifacts.store_dataframe(frame, NAME)

    assert list(root.iterdir()) == []


def test_second_write_keeps_one_previous_frame(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    first = _manifest(root)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    second = _manifest(root)
    artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)
    third = _manifest(root)

    assert "previous" not in first
    assert second["previous"] == first["file"]
    assert third["previous"] == second["file"]
    assert [path.name for path in _arrow_files(root)] == sorted(
        [second["file"], third["file"]]
    )
    assert _stored_table(root).to_pydict() == {"a": [3]}
    previous = pa.ipc.open_file(_frame_dir(root) / third["previous"])
    assert previous.read_all().to_pydict() == {"a": [2]}


def test_unreferenced_frames_are_deleted_only_when_stale(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    directory = _frame_dir(root)
    current = directory / _manifest(root)["file"]
    _backdate(current, 2 * DAY)

    def add(name: str, age_seconds: float) -> Path:
        path = directory / name
        path.write_bytes(b"unreferenced")
        _backdate(path, age_seconds)
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
    outside = root / "outside.arrow"
    outside.write_bytes(b"precious")
    link = directory / "77777777-6666-4555-8444-333333333333.arrow"
    link.symlink_to(outside)
    _backdate(link, 2 * DAY)

    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert not stale.exists()
    assert almost_stale.exists()
    assert fresh.exists()
    assert all(path.exists() for path in not_a_frame)
    assert link.is_symlink()
    assert outside.read_bytes() == b"precious"
    # the frame the replaced manifest named stays as `previous`, however old it is
    assert current.exists()


def _manifest_bytes(**fields: Any) -> bytes:
    return json.dumps({"version": 1, **fields}).encode()


@pytest.mark.parametrize(
    "manifest_bytes",
    [
        pytest.param(b"{not json", id="invalid-json"),
        pytest.param(b"\x80\x81\x82", id="not-utf8"),
        pytest.param(b"[]", id="not-an-object"),
        pytest.param(_manifest_bytes(), id="no-file"),
        pytest.param(_manifest_bytes(file="../../data/x.csv"), id="file-traversal"),
        pytest.param(_manifest_bytes(file=7, previous=FRAME_B), id="file-not-a-string"),
        pytest.param(
            _manifest_bytes(file=FRAME_A, previous="../../data/x.csv"),
            id="previous-traversal",
        ),
        pytest.param(_manifest_bytes(file=f"x{FRAME_A}"), id="file-prefixed"),
        pytest.param(_manifest_bytes(file=f"{FRAME_A}.bak"), id="file-suffixed"),
        pytest.param(
            _manifest_bytes(file=f"{FRAME_A}/../../data/x.csv"),
            id="file-valid-name-then-traversal",
        ),
        pytest.param(_manifest_bytes(file=f"../{FRAME_A}"), id="file-in-parent"),
        pytest.param(
            _manifest_bytes(file=FRAME_A, previous=f"x{FRAME_B}"),
            id="previous-prefixed",
        ),
        pytest.param(
            _manifest_bytes(file=FRAME_A, previous=f"{FRAME_B}.bak"),
            id="previous-suffixed",
        ),
        pytest.param(
            _manifest_bytes(file=FRAME_A, previous=f"{FRAME_B}/../../data/x.csv"),
            id="previous-valid-name-then-traversal",
        ),
        pytest.param(
            _manifest_bytes(file=FRAME_A, previous=f"../{FRAME_B}"),
            id="previous-in-parent",
        ),
        pytest.param(
            _manifest_bytes(file=FRAME_A, previous=FRAME_B, pad="x" * 5000),
            id="oversized-truncated-mid-string",
        ),
        pytest.param(
            _manifest_bytes(file=FRAME_A, previous=FRAME_B) + b" " * 5000,
            id="oversized-valid-json-then-spaces",
        ),
    ],
)
def test_unusable_manifest_counts_as_no_manifest(
    root: Path, manifest_bytes: bytes
) -> None:
    directory = _frame_dir(root)
    directory.mkdir(parents=True)
    (directory / "manifest.json").write_bytes(manifest_bytes)
    named = [directory / FRAME_A, directory / FRAME_B]
    for path in named:
        path.write_bytes(b"named by the hostile manifest")
    sentinel = (directory / "../../data/x.csv").resolve()
    sentinel.parent.mkdir(parents=True)
    sentinel.write_text("keep")

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    manifest = _manifest(root)
    assert "previous" not in manifest
    assert manifest["file"] not in (FRAME_A, FRAME_B)
    assert _stored_table(root).to_pydict() == {"a": [1]}
    assert sentinel.read_text() == "keep"
    # a manifest that counts as absent names nothing, so nothing it names is deleted
    assert all(path.exists() for path in named)


def test_manifest_of_exactly_4096_bytes_is_honoured(root: Path) -> None:
    directory = _frame_dir(root)
    directory.mkdir(parents=True)
    raw = _manifest_bytes(file=FRAME_A, previous=FRAME_B).ljust(4096)
    assert len(raw) == 4096
    (directory / "manifest.json").write_bytes(raw)
    for name in (FRAME_A, FRAME_B):
        (directory / name).write_bytes(b"named by the manifest")

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert _manifest(root)["previous"] == FRAME_A
    assert (directory / FRAME_A).exists()
    assert not (directory / FRAME_B).exists()


def test_manifest_nested_too_deeply_to_parse_counts_as_no_manifest(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Python before 3.13 raises RecursionError for the nesting a 4 KiB manifest allows."""
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

    assert "previous" not in _manifest(root)
    assert _stored_table(root).to_pydict() == {"a": [2]}


def test_manifest_is_written_after_the_frame_file_is_complete(
    root: Path, arrow_sinks: List[Any], monkeypatch: pytest.MonkeyPatch
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
    root: Path, monkeypatch: pytest.MonkeyPatch
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
        except BaseException as exc:  # surfaced by the assertion below
            errors.append(exc)

    with monkeypatch.context() as patch:
        patch.setattr(dataframe_storage, "read_manifest", read_then_wait)
        threads = [threading.Thread(target=write, args=(value,)) for value in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

    assert errors == []
    files = _arrow_files(root)
    assert len(files) == 2
    current = _manifest(root)["file"]
    stray = next(path for path in files if path.name != current)
    for path in files:
        assert pa.ipc.open_file(path).read_all().num_rows == 1

    _backdate(stray, 2 * DAY)
    artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)

    assert not stray.exists()
    assert (_frame_dir(root) / current).exists()
    assert _stored_table(root).to_pydict() == {"a": [3]}


@pytest.mark.parametrize(
    "make_frame", [pd.DataFrame, pl.DataFrame], ids=["pandas", "polars"]
)
def test_frames_are_compressed(root: Path, make_frame: Any) -> None:
    artifacts.store_dataframe(make_frame({"a": ["x" * 100] * 10_000}), NAME)

    stored = _frame_dir(root) / _manifest(root)["file"]
    assert 0 < stored.stat().st_size < 100_000


class _FakeCodec:
    """Stands in for pyarrow.Codec, whose extension type cannot be patched."""

    def __init__(self, availability: Dict[str, Any]) -> None:
        self._availability = availability

    def is_available(self, name: str) -> bool:
        outcome = self._availability[name]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.mark.parametrize(
    "availability, expected",
    [
        pytest.param({"zstd": True, "lz4": True}, "zstd", id="zstd"),
        pytest.param({"zstd": False, "lz4": True}, "lz4", id="zstd-missing"),
        pytest.param(
            {"zstd": ValueError("Unsupported codec"), "lz4": True},
            "lz4",
            id="zstd-unrecognised",
        ),
        pytest.param({"zstd": False, "lz4": False}, "uncompressed", id="none"),
        pytest.param(
            {"zstd": False, "lz4": ValueError("Unsupported codec")},
            "uncompressed",
            id="none-unrecognised",
        ),
    ],
)
def test_compression_falls_back_from_zstd_to_lz4_to_none(
    monkeypatch: pytest.MonkeyPatch, availability: Dict[str, Any], expected: str
) -> None:
    monkeypatch.setattr(pa, "Codec", _FakeCodec(availability))

    assert dataframe_storage._select_compression() == expected


@pytest.mark.skipif(
    not pa.Codec.is_available("zstd"), reason="this pyarrow build has no zstd"
)
def test_zstd_is_chosen_when_pyarrow_has_it() -> None:
    assert dataframe_storage._select_compression() == "zstd"


@pytest.mark.parametrize(
    "make_frame", [pd.DataFrame, pl.DataFrame], ids=["pandas", "polars"]
)
def test_frame_is_stored_uncompressed_when_no_codec_is_available(
    root: Path, monkeypatch: pytest.MonkeyPatch, make_frame: Any
) -> None:
    monkeypatch.setattr(pa, "Codec", _FakeCodec({"zstd": False, "lz4": False}))

    artifacts.store_dataframe(make_frame({"a": ["x" * 100] * 10_000}), NAME)

    stored = _frame_dir(root) / _manifest(root)["file"]
    assert pa.ipc.open_file(stored).read_all().num_rows == 10_000
    assert stored.stat().st_size > 1_000_000


def test_pandas_frame_is_written_in_64k_row_batches(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": range(70_000)}), NAME)

    reader = pa.ipc.open_file(_frame_dir(root) / _manifest(root)["file"])
    assert reader.num_record_batches == 2
    assert reader.read_all().num_rows == 70_000


def test_polars_frame_is_written_in_64k_row_batches(root: Path) -> None:
    artifacts.store_dataframe(pl.DataFrame({"a": range(200_000)}), NAME)

    reader = pa.ipc.open_file(_frame_dir(root) / _manifest(root)["file"])
    assert reader.num_record_batches == 4
    assert reader.read_all().num_rows == 200_000


@pytest.mark.parametrize(
    "polars_version, expected_options",
    [("1.36.9", {}), ("1.37.0", {"record_batch_size": 65_536})],
    ids=["before-1.37", "from-1.37"],
)
def test_polars_batch_size_is_passed_to_polars_from_1_37(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    polars_version: str,
    expected_options: Dict[str, int],
) -> None:
    monkeypatch.setattr(pl, "__version__", polars_version)
    original = pl.DataFrame.write_ipc

    with mock.patch.object(
        pl.DataFrame, "write_ipc", autospec=True, side_effect=original
    ) as write_ipc:
        artifacts.store_dataframe(pl.DataFrame({"a": [1, 2, 3]}), NAME)

    write_ipc.assert_called_once()
    options = {
        name: value
        for name, value in write_ipc.call_args.kwargs.items()
        if name == "record_batch_size"
    }
    assert options == expected_options
    assert _stored_table(root).to_pydict() == {"a": [1, 2, 3]}


def _store_bad_frame_after_a_good_one(
    root: Path, make_frame: Callable[[], Any], expected_error: Optional[type]
) -> Dict[str, Any]:
    """Store a good frame, then a bad one; return what the bad call left behind."""
    artifacts.store_dataframe(pd.DataFrame({"ok": [1]}), NAME)
    directory = _frame_dir(root)
    manifest_bytes = (directory / "manifest.json").read_bytes()
    if expected_error is None:
        artifacts.store_dataframe(make_frame(), NAME)
    else:
        with pytest.raises(expected_error):
            artifacts.store_dataframe(make_frame(), NAME)
    return {
        "manifest_bytes": manifest_bytes,
        "listing": sorted(path.name for path in directory.iterdir()),
    }


@pytest.mark.parametrize("make_frame", ALWAYS_FAILING)
def test_unserializable_frame_raises_and_keeps_the_previous_frame(
    root: Path, make_frame: Callable[[], Any]
) -> None:
    raised_by_pandas = probe_failure(make_frame())
    assert raised_by_pandas is not None, "pandas is expected to refuse this frame"

    left = _store_bad_frame_after_a_good_one(root, make_frame, raised_by_pandas)

    assert (_frame_dir(root) / "manifest.json").read_bytes() == left["manifest_bytes"]
    assert left["listing"] == sorted(["manifest.json", _manifest(root)["file"]])
    assert _stored_table(root).to_pydict() == {"ok": [1]}


@pytest.mark.parametrize("make_frame, rows", VERSION_DEPENDENT)
def test_frames_pandas_may_refuse_follow_what_pandas_does(
    root: Path, make_frame: Callable[[], Any], rows: int
) -> None:
    raised_by_pandas = probe_failure(make_frame())

    left = _store_bad_frame_after_a_good_one(root, make_frame, raised_by_pandas)

    if raised_by_pandas is None:
        assert _stored_table(root).num_rows == rows
        return
    assert (_frame_dir(root) / "manifest.json").read_bytes() == left["manifest_bytes"]
    assert left["listing"] == sorted(["manifest.json", _manifest(root)["file"]])


def test_polars_object_column_raises_and_keeps_the_previous_frame(root: Path) -> None:
    artifacts.store_dataframe(pl.DataFrame({"ok": [1]}), NAME)
    manifest_bytes = (_frame_dir(root) / "manifest.json").read_bytes()

    with pytest.raises(pl.exceptions.ComputeError):
        artifacts.store_dataframe(
            pl.DataFrame({"a": pl.Series([object()], dtype=pl.Object)}), NAME
        )

    assert (_frame_dir(root) / "manifest.json").read_bytes() == manifest_bytes
    assert len(_arrow_files(root)) == 1


def test_partial_file_is_deleted_when_the_write_fails(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def write_some_then_fail(self: pd.DataFrame, sink: Any, **kwargs: Any) -> None:
        sink.write(b"ARROW1 partial")
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(pd.DataFrame, "to_feather", write_some_then_fail)

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert raised.value.errno == errno.EIO
    assert _arrow_files(root) == []
    assert not (_frame_dir(root) / "manifest.json").exists()


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
    root: Path, close_error: Dict[str, Optional[OSError]], error: OSError
) -> None:
    """s3fs surfaces a failed upload only when the file is closed."""
    close_error["error"] = error

    with pytest.raises(OSError) as raised:
        artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert raised.value is error
    assert _arrow_files(root) == []
    assert not (_frame_dir(root) / "manifest.json").exists()


def test_failure_before_the_manifest_step_deletes_nothing(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    directory = _frame_dir(root)
    stale = directory / FRAME_A
    stale.write_bytes(b"unreferenced")
    _backdate(stale, 2 * DAY)
    manifest_bytes = (directory / "manifest.json").read_bytes()
    listing = sorted(path.name for path in directory.iterdir())

    def broken_manifest(*args: Any) -> Any:
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(dataframe_storage, "write_manifest", broken_manifest)

    with pytest.raises(OSError):
        artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)

    assert (directory / "manifest.json").read_bytes() == manifest_bytes
    assert sorted(path.name for path in directory.iterdir()) == listing
    assert _stored_table(root).to_pydict() == {"a": [2]}


def test_failure_after_the_manifest_step_keeps_the_new_frame(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    def broken_cleanup(*args: Any) -> Any:
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(dataframe_storage, "_delete_replaced_frames", broken_cleanup)

    with pytest.raises(OSError):
        artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert _stored_table(root).to_pydict() == {"a": [2]}


def test_stray_pruned_by_another_writer_meanwhile_is_not_a_failure(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    directory = _frame_dir(root)
    strays = [directory / FRAME_A, directory / FRAME_B]
    for stray in strays:
        stray.write_bytes(b"unreferenced")
        _backdate(stray, 2 * DAY)
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
    assert _stored_table(root).to_pydict() == {"a": [2]}


def test_read_only_mount_stores_nothing_and_does_not_convert(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _read_only(monkeypatch)

    with mock.patch.object(pd.DataFrame, "to_feather", autospec=True) as to_feather:
        result = artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert result is None
    to_feather.assert_not_called()
    assert list(root.iterdir()) == []


def test_erofs_from_the_write_stores_nothing(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(self: pd.DataFrame, sink: Any, **kwargs: Any) -> None:
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(pd.DataFrame, "to_feather", refuse)

    result = artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert result is None
    assert _arrow_files(root) == []


def _write_partial_file_then(interrupt: Callable[[], None]) -> Callable[..., None]:
    def to_feather(self: pd.DataFrame, sink: Any, **kwargs: Any) -> None:
        sink.write(b"ARROW1 partial")
        interrupt()

    return to_feather


def _raise_keyboard_interrupt() -> None:
    raise KeyboardInterrupt


@pytest.mark.parametrize(
    "interrupt",
    [lambda: signal.raise_signal(signal.SIGINT), _raise_keyboard_interrupt],
    ids=["sigint", "keyboard-interrupt"],
)
def test_interrupt_during_the_write_raises_and_keeps_the_previous_frame(
    root: Path, monkeypatch: pytest.MonkeyPatch, interrupt: Callable[[], None]
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    manifest_bytes = (_frame_dir(root) / "manifest.json").read_bytes()
    handler_before = signal.getsignal(signal.SIGINT)
    monkeypatch.setattr(pd.DataFrame, "to_feather", _write_partial_file_then(interrupt))

    with pytest.raises(KeyboardInterrupt):
        artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert (_frame_dir(root) / "manifest.json").read_bytes() == manifest_bytes
    assert len(_arrow_files(root)) == 1
    assert signal.getsignal(signal.SIGINT) is handler_before


@pytest.mark.parametrize(
    "point", ["manifest-truncated", "manifest-written", "old-frames-deleted"]
)
def test_interrupt_after_the_frame_is_written_leaves_a_usable_manifest(
    root: Path, monkeypatch: pytest.MonkeyPatch, point: str
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    handler_before = signal.getsignal(signal.SIGINT)
    interrupt_at(point, monkeypatch)

    with pytest.raises(KeyboardInterrupt):
        artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)

    # the manifest parses and names the new frame, which is there and complete
    assert _stored_table(root).to_pydict() == {"a": [3]}
    assert signal.getsignal(signal.SIGINT) is handler_before


def _broken_config() -> Any:
    raise ValueError("invalid config")


@pytest.mark.usefixtures("no_root_settings")
class TestProjectRoot:
    """resolve_project_root() follows the precedence of set_notebook_path()."""

    def test_notebook_root_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(tmp_path / "notebooks"))
        monkeypatch.setenv("DEEPNOTE_HOME_DIR", str(tmp_path / "home"))

        assert dataframe_storage.resolve_project_root() == tmp_path / "notebooks"

    def test_home_dir_is_used_as_is(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DEEPNOTE_HOME_DIR", str(tmp_path / "home"))

        assert dataframe_storage.resolve_project_root() == tmp_path / "home"

    def test_dev_mode_uses_slash_work(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DEEPNOTE_RUNNING_IN_DEV_MODE", "true")

        assert dataframe_storage.resolve_project_root() == Path("/work")

    def test_default_is_work_under_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("HOME", str(tmp_path))

        assert dataframe_storage.resolve_project_root() == tmp_path / "work"

    def test_unreadable_config_falls_back_to_dev_mode_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(dataframe_storage, "get_config", _broken_config)
        monkeypatch.setenv("DEEPNOTE_RUNNING_IN_DEV_MODE", "true")

        assert dataframe_storage.resolve_project_root() == Path("/work")

    def test_unreadable_config_falls_back_to_work_under_home(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(dataframe_storage, "get_config", _broken_config)
        monkeypatch.setenv("HOME", str(tmp_path))

        assert dataframe_storage.resolve_project_root() == tmp_path / "work"


@pytest.mark.usefixtures("no_root_settings")
def test_frame_lands_under_the_project_root_not_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    notebook_directory = tmp_path / "notebooks"
    project.mkdir()
    notebook_directory.mkdir()
    monkeypatch.setenv("DEEPNOTE_PATHS__NOTEBOOK_ROOT", str(project))
    monkeypatch.chdir(notebook_directory)

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert len(_arrow_files(project)) == 1
    assert list(notebook_directory.iterdir()) == []


def test_delete_removes_the_frames_of_the_name_only(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)
    artifacts.store_dataframe(pd.DataFrame({"a": [3]}), "other")
    neighbour = root / "deepnote_dataframes" / "revenue-2026"
    neighbour.mkdir()
    (neighbour / "keep.txt").write_text("keep")
    (root / "notes.txt").write_text("keep")

    assert artifacts.delete_dataframe(NAME) is True

    assert not _frame_dir(root).exists()
    assert _stored_table(root, "other").to_pydict() == {"a": [3]}
    assert (neighbour / "keep.txt").read_text() == "keep"
    assert (root / "notes.txt").read_text() == "keep"


def test_delete_returns_false_when_there_is_nothing_to_delete(root: Path) -> None:
    assert artifacts.delete_dataframe(NAME) is False
    assert list(root.iterdir()) == []


def test_delete_twice_returns_false_the_second_time(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert artifacts.delete_dataframe(NAME) is True
    assert artifacts.delete_dataframe(NAME) is False


def test_a_name_can_be_stored_again_after_it_was_deleted(root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    artifacts.delete_dataframe(NAME)

    artifacts.store_dataframe(pd.DataFrame({"a": [2]}), NAME)

    assert "previous" not in _manifest(root)
    assert _stored_table(root).to_pydict() == {"a": [2]}


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
    assert len(_arrow_files(root)) == 1


def test_delete_does_not_follow_a_link_out_of_the_frames_directory(
    root: Path,
) -> None:
    outside = root / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    (root / "deepnote_dataframes").mkdir()
    (root / "deepnote_dataframes" / NAME).symlink_to(outside, target_is_directory=True)

    with pytest.raises(OSError):
        artifacts.delete_dataframe(NAME)

    assert (outside / "keep.txt").read_text() == "keep"


def test_delete_on_a_read_only_mount_deletes_nothing(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)
    _read_only(monkeypatch)

    assert artifacts.delete_dataframe(NAME) is False

    assert len(_arrow_files(root)) == 1


def test_functions_are_exported_from_the_package() -> None:
    assert deepnote_toolkit.artifacts is artifacts
    assert "artifacts" in deepnote_toolkit.__all__
