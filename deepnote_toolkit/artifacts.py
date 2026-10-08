"""Stores and deletes named DataFrames in the project's filesystem.

A stored frame is an ordinary file under `deepnote_dataframes/<name>/`, so anyone
who can edit the notebook could write it by hand. It holds the DataFrame last stored
under its name, whichever notebook or execution stored it.
"""

import errno
import shutil
from typing import Any, Optional

from deepnote_toolkit import dataframe_storage


def _check_name(name: object) -> None:
    if not dataframe_storage.is_safe_id(name):
        raise ValueError(
            f"A DataFrame name must match [A-Za-z0-9_-]{{1,128}}, got {name!r}"
        )


def store_dataframe(df: Any, name: str) -> Optional[str]:
    """Store `df` as the current frame under `name`, replacing the one stored before.

    Args:
        df: A pandas or polars DataFrame, written whole with no row or size limit.
        name: The frame's name, unique within the project. Must match
            `[A-Za-z0-9_-]{1,128}`.

    Returns:
        The new frame's reference ID, or None if the project's filesystem is
        read-only, in which case nothing is stored.

    Raises:
        ValueError: `name` is not valid.
        TypeError: `df` is not a pandas or polars DataFrame. That includes PySpark
            and pandas-on-Spark DataFrames, polars LazyFrames and SQL query previews.
        OSError: The write failed.
        Exception: `df` cannot be serialized to Arrow, as with `df.to_feather()`.

    A failed call leaves the name's previous frame current.
    """
    _check_name(name)
    kind = dataframe_storage.classify_frame(df)
    if kind is None:
        raise TypeError(
            "store_dataframe needs a pandas or polars DataFrame, "
            f"got {type(df).__qualname__}"
        )

    root = dataframe_storage.resolve_project_root()
    if dataframe_storage.is_read_only(root):
        return None
    try:
        return dataframe_storage.store_frame(df, kind, name, root)
    except OSError as exc:
        if exc.errno == errno.EROFS:
            return None
        raise


def delete_dataframe(name: str) -> bool:
    """Delete the frames stored under `name`.

    Returns:
        Whether there was anything to delete. False on a read-only filesystem, where
        nothing is deleted.

    Raises:
        ValueError: `name` is not valid.
    """
    _check_name(name)
    root = dataframe_storage.resolve_project_root()
    if dataframe_storage.is_read_only(root):
        return False

    directory = root / dataframe_storage.FRAMES_DIR / name
    if not directory.is_dir():
        return False
    shutil.rmtree(directory)
    return True
