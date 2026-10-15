"""Tests for the compression, record batches and column types of stored DataFrames."""

from pathlib import Path
from typing import Any, Dict
from unittest import mock

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from deepnote_toolkit import artifacts
from deepnote_toolkit.artifacts import dataframe_storage
from tests.unit.helpers.dataframe_storage import (
    NAME,
    frame_dir,
    stored_manifest,
    stored_table,
)


@pytest.mark.parametrize(
    "make_frame", [pd.DataFrame, pl.DataFrame], ids=["pandas", "polars"]
)
def test_frames_are_compressed(tmp_project_root: Path, make_frame: Any) -> None:
    artifacts.store_dataframe(make_frame({"a": ["x" * 100] * 10_000}), NAME)

    stored = frame_dir(tmp_project_root) / stored_manifest(tmp_project_root)["file"]
    assert 0 < stored.stat().st_size < 100_000


class _FakeCodec:
    """Stands in for pyarrow.Codec, whose extension type cannot be patched."""

    def __init__(self, availability: Dict[str, bool]) -> None:
        self._availability = availability

    def is_available(self, name: str) -> bool:
        return self._availability[name]


@pytest.mark.parametrize(
    "availability, expected",
    [
        pytest.param({"zstd": True, "lz4": True}, "zstd", id="zstd"),
        pytest.param({"zstd": False, "lz4": True}, "lz4", id="zstd-missing"),
        pytest.param({"zstd": False, "lz4": False}, "uncompressed", id="none"),
    ],
)
def test_compression_falls_back_from_zstd_to_lz4_to_none(
    monkeypatch: pytest.MonkeyPatch, availability: Dict[str, bool], expected: str
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
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch, make_frame: Any
) -> None:
    monkeypatch.setattr(pa, "Codec", _FakeCodec({"zstd": False, "lz4": False}))

    artifacts.store_dataframe(make_frame({"a": ["x" * 100] * 10_000}), NAME)

    stored = frame_dir(tmp_project_root) / stored_manifest(tmp_project_root)["file"]
    assert pa.ipc.open_file(stored).read_all().num_rows == 10_000
    assert stored.stat().st_size > 1_000_000


def test_pandas_frame_is_written_in_64k_row_batches(tmp_project_root: Path) -> None:
    artifacts.store_dataframe(pd.DataFrame({"a": range(70_000)}), NAME)

    reader = pa.ipc.open_file(
        frame_dir(tmp_project_root) / stored_manifest(tmp_project_root)["file"]
    )
    assert reader.num_record_batches == 2
    assert reader.read_all().num_rows == 70_000


def test_polars_frame_is_written_in_64k_row_batches(tmp_project_root: Path) -> None:
    artifacts.store_dataframe(pl.DataFrame({"a": range(200_000)}), NAME)

    reader = pa.ipc.open_file(
        frame_dir(tmp_project_root) / stored_manifest(tmp_project_root)["file"]
    )
    assert reader.num_record_batches == 4
    assert reader.read_all().num_rows == 200_000


@pytest.mark.parametrize(
    "polars_version, expected_options",
    [("1.36.9", {}), ("1.37.0", {"record_batch_size": 65_536})],
    ids=["before-1.37", "from-1.37"],
)
def test_polars_batch_size_is_passed_to_polars_from_1_37(
    tmp_project_root: Path,
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
    assert stored_table(tmp_project_root).to_pydict() == {"a": [1, 2, 3]}


def test_polars_strings_are_stored_without_view_types(tmp_project_root: Path) -> None:
    artifacts.store_dataframe(
        pl.DataFrame(
            {
                "s": ["a", "a string longer than twelve bytes"],
                "b": [b"x", b"y"],
                "ls": [["a"], ["b", "c"]],
            }
        ),
        NAME,
    )

    assert stored_table(tmp_project_root).schema.types == [
        pa.large_string(),
        pa.large_binary(),
        pa.large_list(pa.large_string()),
    ]


@pytest.mark.parametrize(
    "polars_version, passed",
    [("1.2.0", False), ("1.2.1", True)],
    ids=["before-1.2.1", "from-1.2.1"],
)
def test_polars_compat_level_is_passed_to_polars_from_1_2_1(
    tmp_project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    polars_version: str,
    passed: bool,
) -> None:
    monkeypatch.setattr(pl, "__version__", polars_version)
    original = pl.DataFrame.write_ipc

    with mock.patch.object(
        pl.DataFrame, "write_ipc", autospec=True, side_effect=original
    ) as write_ipc:
        artifacts.store_dataframe(pl.DataFrame({"a": ["x"]}), NAME)

    assert ("compat_level" in write_ipc.call_args.kwargs) is passed
    assert stored_table(tmp_project_root).to_pydict() == {"a": ["x"]}
