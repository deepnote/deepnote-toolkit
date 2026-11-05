"""Tests for the compression, record batches and column types of stored DataFrames."""

from pathlib import Path
from typing import Callable, Dict, List, Optional, Type, Union

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from deepnote_toolkit import artifacts
from deepnote_toolkit.artifacts import _dataframe_storage
from tests.unit.helpers.dataframe_storage import (
    NAME,
    frame_dir,
    stored_manifest,
    stored_table,
)


@pytest.mark.parametrize(
    "make_frame", [pd.DataFrame, pl.DataFrame], ids=["pandas", "polars"]
)
def test_frames_are_compressed(
    tmp_project_root: Path, make_frame: Union[Type[pd.DataFrame], Type[pl.DataFrame]]
) -> None:
    artifacts.store_dataframe(make_frame({"a": ["x" * 100] * 10_000}), NAME)

    stored = frame_dir(tmp_project_root) / stored_manifest(tmp_project_root).file
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
        pytest.param({"zstd": False, "lz4": True}, None, id="zstd-missing"),
    ],
)
def test_compression_is_zstd_or_none_never_lz4(
    monkeypatch: pytest.MonkeyPatch,
    availability: Dict[str, bool],
    expected: Optional[str],
) -> None:
    monkeypatch.setattr(pa, "Codec", _FakeCodec(availability))

    assert _dataframe_storage._select_compression() == expected


def test_frame_is_stored_uncompressed_without_zstd_even_with_lz4(
    tmp_project_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pa, "Codec", _FakeCodec({"zstd": False, "lz4": True}))

    artifacts.store_dataframe(pd.DataFrame({"a": ["x" * 100] * 10_000}), NAME)

    stored = frame_dir(tmp_project_root) / stored_manifest(tmp_project_root).file
    assert pa.ipc.open_file(stored).read_all().num_rows == 10_000
    assert stored.stat().st_size > 1_000_000


def _file_batch_rows(root: Path) -> List[int]:
    reader = pa.ipc.open_file(frame_dir(root) / stored_manifest(root).file)
    return [reader.get_batch(i).num_rows for i in range(reader.num_record_batches)]


def _categories_in_many_chunks(in_lists: bool = False) -> pl.DataFrame:
    """2,000 chunks that share one unified dictionary of 200,000 values."""
    with pl.StringCache():
        parts = []
        for i in range(2_000):
            ids = [f"id-{i:05d}-{j:03d}" for j in range(100)]
            column = (
                pl.Series([[id_] for id_ in ids], dtype=pl.List(pl.Categorical))
                if in_lists
                else pl.Series(ids, dtype=pl.Categorical)
            )
            parts.append(pl.DataFrame({"id": column}))
    return pl.concat(parts, rechunk=False)


def _categories_in_two_batches() -> pl.DataFrame:
    """Two chunks whose dictionaries differ and fall into different batches."""
    with pl.StringCache():
        first = pl.DataFrame(
            {"c": pl.Series(["x", "y"] * 32_768, dtype=pl.Categorical)}
        )
        second = pl.DataFrame({"c": pl.Series(["z", "x"], dtype=pl.Categorical)})
    return pl.concat([first, second], rechunk=False)


@pytest.mark.parametrize(
    "make_frame, expected",
    [
        pytest.param(
            lambda: pd.DataFrame({"a": range(70_000)}),
            [65_536, 4_464],
            id="pandas",
        ),
        pytest.param(
            lambda: pl.concat(
                [pl.DataFrame({"a": range(10)}), pl.DataFrame({"a": range(70_000)})],
                rechunk=False,
            ),
            [65_536, 4_474],
            id="polars-small-chunk-merged",
        ),
        pytest.param(
            lambda: pd.DataFrame(
                {"a": pd.array(range(2_000_000), dtype="int64[pyarrow]")}
            ).head(70_000),
            [65_536, 4_464],
            id="pandas-arrow-backed-head-of-a-larger-frame",
        ),
        pytest.param(
            _categories_in_many_chunks,
            [65_536, 65_536, 65_536, 3_392],
            id="polars-categorical-many-chunks",
        ),
        pytest.param(
            lambda: _categories_in_many_chunks(in_lists=True),
            [65_536, 65_536, 65_536, 3_392],
            id="polars-list-of-categoricals-many-chunks",
        ),
        pytest.param(
            lambda: pd.DataFrame(
                {"c": pd.Categorical([f"{i:0100d}" for i in range(200_000)])}
            ),
            [65_536, 65_536, 65_536, 3_392],
            id="pandas-categorical-dictionary-larger-than-a-batch",
        ),
        pytest.param(
            lambda: pd.DataFrame(
                {"t": ["y"] * 150 + ["x" * 7_000_000] * 2 + ["y"] * 19_848}
            ),
            [151, 11_747, 8_102],
            id="pandas-long-rows-cut-where-a-batch-is-full",
        ),
        pytest.param(
            lambda: pd.DataFrame(
                {"t": ["y"] * 150 + ["x" * 13_000_000] + ["y"] * 19_849}
            ),
            [150, 1, 12_656, 7_193],
            id="pandas-row-over-the-limit-among-short-ones",
        ),
        pytest.param(lambda: pd.DataFrame({"a": []}), [], id="pandas-empty"),
        pytest.param(lambda: pd.DataFrame(index=range(5)), [5], id="pandas-no-columns"),
    ],
)
def test_manifest_batch_rows_match_the_files_record_batches(
    tmp_project_root: Path,
    make_frame: Callable[[], Union[pd.DataFrame, pl.DataFrame]],
    expected: List[int],
) -> None:
    frame = make_frame()

    artifacts.store_dataframe(frame, NAME)

    manifest = stored_manifest(tmp_project_root)
    assert manifest.batch_rows == expected
    assert _file_batch_rows(tmp_project_root) == expected
    assert manifest.rows == len(frame)


def test_dictionaries_of_chunks_are_unified(tmp_project_root: Path) -> None:
    artifacts.store_dataframe(_categories_in_two_batches(), NAME)

    values = stored_table(tmp_project_root).column("c").to_pylist()
    assert values == ["x", "y"] * 32_768 + ["z", "x"]


def test_long_text_is_split_into_batches_of_at_most_8_mib(
    tmp_project_root: Path,
) -> None:
    # 20,000 rows of 2,000 bytes: about 40 MB, which needs five 8 MiB batches.
    artifacts.store_dataframe(pd.DataFrame({"text": ["x" * 2_000] * 20_000}), NAME)

    reader = pa.ipc.open_file(
        frame_dir(tmp_project_root) / stored_manifest(tmp_project_root).file
    )
    batches = [reader.get_batch(i) for i in range(reader.num_record_batches)]
    assert len(batches) == 5
    assert max(batch.nbytes for batch in batches) <= 8 * 1024 * 1024
    assert stored_manifest(tmp_project_root).batch_rows == [
        batch.num_rows for batch in batches
    ]


def test_row_larger_than_a_batch_gets_a_batch_of_its_own(
    tmp_project_root: Path,
) -> None:
    artifacts.store_dataframe(pd.DataFrame({"text": ["x" * 13_000_000] * 3}), NAME)

    assert stored_manifest(tmp_project_root).batch_rows == [1, 1, 1]
    assert _file_batch_rows(tmp_project_root) == [1, 1, 1]


def test_long_rows_together_are_split_into_batches_of_at_most_12_mib(
    tmp_project_root: Path,
) -> None:
    # 20 MB in the first and in the last 400 rows, which the frame's average row size
    # puts in one batch each.
    long_rows = ["x" * 50_000] * 400
    frame = pd.DataFrame({"t": long_rows + ["y"] * 99_200 + long_rows})

    artifacts.store_dataframe(frame, NAME)

    reader = pa.ipc.open_file(
        frame_dir(tmp_project_root) / stored_manifest(tmp_project_root).file
    )
    batches = [reader.get_batch(i) for i in range(reader.num_record_batches)]
    assert max(batch.nbytes for batch in batches) <= 12 * 1024 * 1024
    assert stored_manifest(tmp_project_root).batch_rows == [
        batch.num_rows for batch in batches
    ]
    assert pa.Table.from_batches(batches).column("t").to_pylist() == list(frame["t"])


def _arrow_text(rows: int) -> pd.Series:
    return pd.Series(["x" * 500] * rows, dtype="string[pyarrow]")


@pytest.mark.parametrize(
    "make_frame",
    [
        pytest.param(
            lambda: pd.concat(
                [pd.DataFrame({"t": _arrow_text(20_000)})] * 4, ignore_index=True
            ),
            id="pandas-frame-concatenated-with-itself",
        ),
        pytest.param(
            lambda: pd.DataFrame({"t": _arrow_text(20_000)}).pipe(
                lambda frame: frame.assign(a=frame["t"], b=frame["t"], c=frame["t"])
            ),
            id="pandas-column-and-its-copies",
        ),
    ],
)
def test_buffers_shared_by_columns_or_chunks_count_once_per_use(
    tmp_project_root: Path, make_frame: Callable[[], pd.DataFrame]
) -> None:
    # Every batch writes its own copy of each column and chunk.
    artifacts.store_dataframe(make_frame(), NAME)

    reader = pa.ipc.open_file(
        frame_dir(tmp_project_root) / stored_manifest(tmp_project_root).file
    )
    batches = [reader.get_batch(i) for i in range(reader.num_record_batches)]
    assert max(batch.nbytes for batch in batches) <= 8 * 1024 * 1024


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
