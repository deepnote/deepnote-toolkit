"""Writes DataFrames into the project's filesystem as named Arrow files."""

import contextlib
import os
import re
import signal
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Optional

from packaging.version import Version

from deepnote_toolkit import env
from deepnote_toolkit.config import get_config
from deepnote_toolkit.dataframe_storage_manifest import (
    Manifest,
    is_frame_name,
    read_manifest,
    write_manifest,
)
from deepnote_toolkit.ocelots.utils import (
    is_pandas_dataframe,
    is_polars_eager_dataframe,
)
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

FRAMES_DIR = Path("deepnote_dataframes")
STALE_FRAME_AGE_SECONDS = 24 * 60 * 60
# Each record batch is one page of the read endpoint; pandas already writes 64k-row
# batches, and polars only honours a batch size from 1.37.
POLARS_BATCH_ROWS = 65_536
POLARS_BATCH_MIN_VERSION = Version("1.37")

FrameKind = Literal["pandas", "polars"]

_SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


def is_safe_id(value: object) -> bool:
    """Return whether `value` can be used as a single path segment.

    Frame names become directory names, so they must not be able to leave
    `FRAMES_DIR`.
    """
    return isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None


def classify_frame(value: object) -> Optional[FrameKind]:
    """Return "pandas" or "polars" for a frame worth storing, else None.

    PySpark and pandas-on-Spark frames are neither, so they are skipped.
    """
    # A query preview holds at most 100 rows, so storing it would pass off a
    # truncated frame as the result. isinstance, not the cleared-query marker,
    # because mutating a preview clears the marker but leaves it a preview.
    if isinstance(value, DeepnoteQueryPreview):
        return None
    if is_pandas_dataframe(value):
        return "pandas"
    if is_polars_eager_dataframe(value):
        return "polars"
    return None


def resolve_project_root() -> Path:
    """Return the project's root directory, mirroring set_notebook_path().

    The working directory cannot be used: set_notebook_path() moves it into the
    notebook's directory.
    """
    try:
        cfg = get_config()
        if cfg.paths.notebook_root:
            return Path(cfg.paths.notebook_root)
        if cfg.paths.home_dir:
            return Path(cfg.paths.home_dir)
        if cfg.runtime.dev_mode:
            return Path("/work")
        return Path(os.environ.get("HOME", str(Path.home()))) / "work"
    except Exception:  # pylint: disable=broad-exception-caught
        if env.get_env("DEEPNOTE_RUNNING_IN_DEV_MODE"):
            return Path("/work")
        return Path(os.environ.get("HOME", str(Path.home()))) / "work"


@contextlib.contextmanager
def _sigint_handled_by(handler: Callable[..., Any]) -> Iterator[None]:
    """Install `handler` for SIGINT, then restore the previous one.

    Off the main thread signal handlers cannot be set, and nothing needs to be.
    """
    try:
        previous = signal.signal(signal.SIGINT, handler)
    except ValueError:
        yield
        return
    try:
        yield
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)


@contextlib.contextmanager
def _sigint_deferred() -> Iterator[None]:
    """Hold SIGINT back until the block ends, then raise it as KeyboardInterrupt."""
    interrupted: list[bool] = []
    with _sigint_handled_by(lambda *_: interrupted.append(True)):
        yield
    if interrupted:
        raise KeyboardInterrupt


def is_read_only(root: Path) -> bool:
    """Return whether the project's filesystem is mounted read-only."""
    return bool(os.statvfs(root).f_flag & os.ST_RDONLY)


def _select_compression() -> str:
    """Return the best Arrow codec this pyarrow build has: zstd, lz4, else none."""
    import pyarrow as pa

    for codec in ("zstd", "lz4"):
        try:
            if pa.Codec.is_available(codec):
                return codec
        except Exception:  # pylint: disable=broad-exception-caught
            # pyarrow raises, rather than returning False, for names it does not know
            continue
    return "uncompressed"


def _write_frame_file(
    frame: Any, kind: FrameKind, path: Path, compression: str
) -> None:
    """Write the frame to `path` as an Arrow IPC file, with the library's own call.

    Going through a Python file object makes `close()` raise upload errors, which
    s3fs only surfaces there.
    """
    with path.open("wb") as sink:
        if kind == "pandas":
            frame.to_feather(sink, compression=compression)
        else:
            import polars

            options = {}
            if Version(polars.__version__) >= POLARS_BATCH_MIN_VERSION:
                options["record_batch_size"] = POLARS_BATCH_ROWS
            frame.write_ipc(sink, compression=compression, **options)


def _delete_replaced_frames(
    directory: Path, old: Optional[Manifest], new_file: str
) -> None:
    """Delete the frame the replaced manifest called `previous`, and stale strays.

    A stray is a `<uuid>.arrow` that no manifest names, left by a writer that lost
    a race or crashed. It must be a day old so that a write still in progress is
    never mistaken for one.
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
            # another writer may prune the same stray first
            with contextlib.suppress(FileNotFoundError):
                if (
                    entry.is_file(follow_symlinks=False)
                    and now - entry.stat(follow_symlinks=False).st_mtime
                    > STALE_FRAME_AGE_SECONDS
                ):
                    (directory / entry.name).unlink()


def store_frame(frame: Any, kind: FrameKind, name: str, root: Path) -> str:
    """Store the frame as the current one under `name` and return its reference ID.

    The order matters because S3 replaces the manifest in one step: the frame is
    complete before the manifest names it, and nothing is deleted before then.
    An interrupt stops everything but the manifest update, which finishes first:
    one between truncating the manifest and writing it would upload an empty one,
    and one after it would delete the frame it now names.
    """
    directory = root / FRAMES_DIR / name
    directory.mkdir(parents=True, exist_ok=True)

    reference_id = str(uuid.uuid4())
    new_file = f"{reference_id}.arrow"
    committed = False
    try:
        _write_frame_file(frame, kind, directory / new_file, _select_compression())
        with _sigint_deferred():
            old = read_manifest(directory)
            write_manifest(directory, new_file, old.file if old else None, len(frame))
            committed = True
    except BaseException:
        if not committed:
            with contextlib.suppress(OSError):
                (directory / new_file).unlink(missing_ok=True)
        raise
    _delete_replaced_frames(directory, old, new_file)
    return reference_id
