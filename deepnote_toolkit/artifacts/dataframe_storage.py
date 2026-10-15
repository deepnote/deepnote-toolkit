"""Writes named DataFrames into the project's filesystem as Arrow files."""

import contextlib
import errno
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Literal, Optional

from packaging.version import Version

from deepnote_toolkit.config import resolve_notebook_root
from deepnote_toolkit.ocelots.utils import (
    is_pandas_dataframe,
    is_polars_eager_dataframe,
)
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

from .dataframe_storage_manifest import (
    Manifest,
    is_frame_name,
    read_manifest,
    write_manifest,
)

FRAMES_DIR = Path("deepnote_dataframes")
STALE_FRAME_AGE_SECONDS = 24 * 60 * 60
POLARS_BATCH_ROWS = 65_536
POLARS_BATCH_MIN_VERSION = Version("1.37")
POLARS_COMPAT_LEVEL_MIN_VERSION = Version("1.2.1")

FrameKind = Literal["pandas", "polars"]

_SAFE_ID_REGEX = re.compile(r"[A-Za-z0-9_-]{1,128}")


def is_safe_id(value: object) -> bool:
    """Return whether `value` can be used as a single path segment."""
    return isinstance(value, str) and _SAFE_ID_REGEX.fullmatch(value) is not None


def classify_frame(value: object) -> Optional[FrameKind]:
    """Return "pandas" or "polars" for a frame worth storing, else None."""
    if isinstance(value, DeepnoteQueryPreview):
        return None
    if is_pandas_dataframe(value):
        return "pandas"
    if is_polars_eager_dataframe(value):
        return "polars"
    return None


def resolve_project_root() -> Path:
    """Return the project's root directory, the one set_notebook_path() uses."""
    return resolve_notebook_root()


def _select_compression() -> str:
    """Return the best codec this pyarrow build has: zstd, lz4, else uncompressed."""
    import pyarrow as pa

    for codec in ("zstd", "lz4"):
        if pa.Codec.is_available(codec):
            return codec
    return "uncompressed"


def _write_frame_file(
    frame: Any, kind: FrameKind, path: Path, compression: str
) -> None:
    """Write the frame to `path` as an Arrow IPC file.

    Going through a Python file object makes `close()` raise upload errors, which
    s3fs only surfaces there.
    """
    with path.open("wb") as sink:
        if kind == "pandas":
            from pyarrow import feather

            # pandas < 2.1's to_feather needs a default index and string column names.
            feather.write_feather(frame, sink, compression=compression)
        else:
            import polars

            polars_version = Version(polars.__version__)
            options: dict[str, Any] = {}
            # Cap batches like write_feather so a page read stays cheap.
            if polars_version >= POLARS_BATCH_MIN_VERSION:
                options["record_batch_size"] = POLARS_BATCH_ROWS
            # pyarrow < 15, apache-arrow < 21.2 can't read 1.0+'s string/binary views.
            if polars_version >= POLARS_COMPAT_LEVEL_MIN_VERSION:
                options["compat_level"] = polars.CompatLevel.oldest()
            frame.write_ipc(sink, compression=compression, **options)


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
        if old.previous is not None and old.previous not in named:
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


def store_frame(frame: Any, kind: FrameKind, name: str, root: Path) -> Optional[str]:
    """Store the frame as the current one under `name` and return its reference ID.

    Returns None, having stored nothing, if the filesystem is read-only. EROFS from
    the manifest step on raises like any other error: the new frame may be current.
    """
    directory = root / FRAMES_DIR / name
    reference_id = str(uuid.uuid4())
    new_file = f"{reference_id}.arrow"
    try:
        directory.mkdir(parents=True, exist_ok=True)
        _write_frame_file(frame, kind, directory / new_file, _select_compression())
        old = read_manifest(directory)
    except BaseException as exc:
        with contextlib.suppress(OSError):
            (directory / new_file).unlink(missing_ok=True)
        if isinstance(exc, OSError) and exc.errno == errno.EROFS:
            return None
        raise

    write_manifest(directory, new_file, old.file if old else None, len(frame))
    _delete_replaced_frames(directory, old, new_file)
    return reference_id
