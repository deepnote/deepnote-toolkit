"""Writes named DataFrames into the project's filesystem as Arrow files."""

import contextlib
import errno
import os
import re
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import pandas as pd
from typing_extensions import TypeAlias, assert_never

from deepnote_toolkit.config import resolve_notebook_root
from deepnote_toolkit.ocelots.types import PandasDF, PolarsEagerDF

from ._dataframe_storage_manifest import (
    Manifest,
    encode_manifest,
    is_frame_name,
    read_manifest,
    write_manifest,
)
from ._polars_compatibility_checks import (
    refuse_arrays_on_old_polars,
    refuse_object_columns,
    refuse_old_polars,
)

if TYPE_CHECKING:
    import pyarrow as pa

FRAMES_DIR = Path("deepnote_dataframes")
STALE_FRAME_AGE_SECONDS = 24 * 60 * 60
MAX_BATCH_ROWS = 65_536
# A page spanning two batches reads at most half the reader's 32 MiB limit,
# leaving room for dictionaries, IPC framing and uneven rows.
TARGET_BATCH_BYTES = 8 * 1024 * 1024

StorableFrame: TypeAlias = Union[PandasDF, PolarsEagerDF]

_SAFE_ID_REGEX = re.compile(r"[A-Za-z0-9_-]{1,128}")


def is_safe_id(value: object) -> bool:
    """Return whether `value` can be used as a single path segment."""
    return isinstance(value, str) and _SAFE_ID_REGEX.fullmatch(value) is not None


def resolve_project_root() -> Path:
    """Return the project's root directory, the one set_notebook_path() uses."""
    return resolve_notebook_root()


def _select_compression() -> Optional[str]:
    """Return zstd if this pyarrow build has it, else None for uncompressed.

    Never lz4: Deepnote Cloud doesn't support it yet.
    """
    import pyarrow as pa

    return "zstd" if pa.Codec.is_available("zstd") else None


def _to_arrow_table(frame: StorableFrame) -> "pa.Table":
    """Return the frame as an Arrow table."""
    if isinstance(frame, pd.DataFrame):
        import pyarrow as pa

        # As write_feather does: RangeIndex to metadata, other indexes to columns.
        return pa.Table.from_pandas(frame, preserve_index=None)

    # polars is an optional dependency, so import it only for a non-pandas frame.
    import polars

    if isinstance(frame, polars.DataFrame):
        refuse_old_polars()
        refuse_object_columns(frame)
        refuse_arrays_on_old_polars(frame)
        # Pinned, though it's the default: pyarrow < 15 and apache-arrow < 21.2
        # can't read polars' string/binary views.
        return frame.to_arrow(compat_level=polars.CompatLevel.oldest())

    assert_never(frame)


def _has_dictionary(data_type: "pa.DataType") -> bool:
    """Return whether `data_type` is a dictionary type or nests one."""
    import pyarrow as pa

    if pa.types.is_dictionary(data_type):
        return True
    return any(
        _has_dictionary(data_type.field(i).type) for i in range(data_type.num_fields)
    )


def _rows_per_batch(table: "pa.Table") -> int:
    """Return how many rows make a batch of about TARGET_BATCH_BYTES."""
    # Every batch writes its own copy of each column and chunk, as nbytes counts
    # them, even where they share buffers. A dictionary is the exception: the file
    # holds it once, while nbytes counts it once per chunk that shares it.
    # get_total_buffer_size() counts it once, but counts a slice's whole buffer.
    nbytes = sum(
        (
            min(column.nbytes, column.get_total_buffer_size())
            if _has_dictionary(column.type)
            else column.nbytes
        )
        for column in table.columns
    )
    if table.num_rows == 0 or nbytes == 0:
        return MAX_BATCH_ROWS
    row_bytes = nbytes / table.num_rows
    return max(1, min(MAX_BATCH_ROWS, int(TARGET_BATCH_BYTES // row_bytes)))


def _write_frame_file(
    frame: StorableFrame, path: Path, compression: Optional[str]
) -> list[int]:
    """Write the frame to `path` as an Arrow IPC file; return its batches' row counts.

    The dataframe reader needs every record batch's row count, so the batches are
    cut here rather than by pyarrow.
    """
    import pyarrow as pa

    with path.open("wb") as sink:
        # A file with more than one dictionary for a column is refused by pyarrow.
        table = _to_arrow_table(frame).unify_dictionaries()
        rows_per_batch = _rows_per_batch(table)
        options = pa.ipc.IpcWriteOptions(compression=compression, allow_64bit=True)
        batch_rows: list[int] = []
        with pa.ipc.new_file(sink, table.schema, options=options) as writer:
            for offset in range(0, table.num_rows, rows_per_batch):
                # slice() does not clamp the length of a table without columns.
                length = min(rows_per_batch, table.num_rows - offset)
                # to_batches() alone never merges chunks, so small chunks would
                # make small batches; combining a slice copies at most one batch.
                batch_table = table.slice(offset, length).combine_chunks()
                for batch in batch_table.to_batches():
                    writer.write_batch(batch)
                    batch_rows.append(batch.num_rows)
    return batch_rows


def _delete_replaced_frames(
    directory: Path, old: Optional[Manifest], new_file: str
) -> None:
    """Delete the frame the replaced manifest called `previous`, and stale strays.

    Another kernel may still be writing an unnamed frame here, so a stray must be a
    day old before it is deleted.
    """
    named = {new_file}
    if old is not None:
        named.add(old.file)
        if (
            old.previous is not None
            and is_frame_name(old.previous)
            and old.previous not in named
        ):
            (directory / old.previous).unlink(missing_ok=True)

    now = time.time()
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.name in named or not is_frame_name(entry.name):
                continue
            with contextlib.suppress(FileNotFoundError):
                if (
                    entry.is_file(follow_symlinks=False)
                    and now - entry.stat(follow_symlinks=False).st_mtime
                    > STALE_FRAME_AGE_SECONDS
                ):
                    (directory / entry.name).unlink()


def store_frame(frame: StorableFrame, name: str, root: Path) -> Optional[str]:
    """Store the frame as the current one under `name` and return its reference ID.

    Returns None, having stored nothing, if the filesystem is read-only. EROFS from
    the manifest step on raises like any other error: the new frame may be current.
    """
    directory = root / FRAMES_DIR / name
    reference_id = str(uuid.uuid4())
    new_file = f"{reference_id}.arrow"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        batch_rows = _write_frame_file(
            frame, directory / new_file, _select_compression()
        )
        old = read_manifest(directory)
        manifest = encode_manifest(new_file, old.file if old else None, batch_rows)
    except BaseException as exc:
        with contextlib.suppress(OSError):
            (directory / new_file).unlink(missing_ok=True)
        if isinstance(exc, OSError) and exc.errno == errno.EROFS:
            return None
        raise

    write_manifest(directory, manifest)
    _delete_replaced_frames(directory, old, new_file)
    return reference_id
