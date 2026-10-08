"""Writes DataFrames into the project's filesystem as named Arrow files.

Besides the function behind `artifacts.store_dataframe`, this holds the hook that
stores the DataFrame a block with a storage setting returns.
"""

import contextlib
import errno
import os
import re
import signal
import time
import uuid
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Optional

import requests
from IPython import get_ipython
from IPython.core.interactiveshell import ExecutionResult
from packaging.version import Version

from deepnote_toolkit import env
from deepnote_toolkit.config import get_config
from deepnote_toolkit.dataframe_storage_manifest import (
    Manifest,
    is_frame_name,
    read_manifest,
    write_manifest,
)
from deepnote_toolkit.get_webapp_url import (
    get_absolute_userpod_api_url,
    get_project_auth_headers,
)
from deepnote_toolkit.logging import get_logger
from deepnote_toolkit.ocelots.utils import (
    is_pandas_dataframe,
    is_polars_eager_dataframe,
)
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

ENABLED_ENV_VAR = "DEEPNOTE_DATAFRAME_STORAGE_ENABLED"
FRAMES_DIR = Path("deepnote_dataframes")
STALE_FRAME_AGE_SECONDS = 24 * 60 * 60
# The webapp's toolkit/errors endpoint rejects any other type with a 400, and groups
# runtime errors in Sentry by `code`.
REPORT_TYPE = "TOOLKIT_RUNTIME_ERROR"
REPORT_CODE = "DATAFRAME_STORAGE_WRITE_FAILED"
REPORT_TIMEOUT_SECONDS = 2
# Each record batch is one page of the read endpoint; pandas already writes 64k-row
# batches, and polars only honours a batch size from 1.37.
POLARS_BATCH_ROWS = 65_536
POLARS_BATCH_MIN_VERSION = Version("1.37")

FrameKind = Literal["pandas", "polars"]

_SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


@dataclass(frozen=True)
class StorageTarget:
    """What the platform asked the current execution to store, and for which block."""

    name: str
    block_id: str


# Failures already reported in this kernel session, keyed by errno or exception type.
_reported: set[str] = set()


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
def _sigint_raises() -> Iterator[None]:
    """Make SIGINT raise KeyboardInterrupt.

    Ordinary cells already run post_run_cell under this handler. A cell with
    top-level await runs it under one that only queues the interrupt, so without
    this, Stop would wait for the whole write to finish.
    """
    with _sigint_handled_by(signal.default_int_handler):
        yield


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


def store_frame(
    frame: Any,
    kind: FrameKind,
    name: str,
    root: Path,
    block_id: Optional[str] = None,
) -> str:
    """Store the frame as the current one under `name` and return its reference ID.

    `block_id` records the block whose result this is; a call stores none.

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
            write_manifest(
                directory, new_file, old.file if old else None, len(frame), block_id
            )
            committed = True
    except BaseException:
        if not committed:
            with contextlib.suppress(OSError):
                (directory / new_file).unlink(missing_ok=True)
        raise
    _delete_replaced_frames(directory, old, new_file)
    return reference_id


def register_dataframe_storage() -> None:
    """Register the post_run_cell hook that stores the results of blocks with a setting.

    Registration is unconditional: whether a given execution stores a frame is
    decided on every execution from the environment and the request metadata.
    """
    get_ipython().events.register("post_run_cell", _on_post_run_cell)


def _is_enabled_by_environment() -> bool:
    """Return whether the platform switched the feature on for this kernel."""
    value = env.get_env(ENABLED_ENV_VAR)
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def _read_storage_target() -> Optional[StorageTarget]:
    """Return the storage target in the current execute_request, if it is usable.

    The metadata is written by other services, so every level is validated and the
    name must be safe to use as a path segment.
    """
    kernel = getattr(get_ipython(), "kernel", None)
    if kernel is None:
        return None

    node: Any = kernel.get_parent()
    for key in ("metadata", "deepnote", "dataframeStorage"):
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    if not isinstance(node, dict):
        return None

    name, block_id = node.get("name"), node.get("blockId")
    if not (is_safe_id(name) and is_safe_id(block_id)):
        return None
    return StorageTarget(name=name, block_id=block_id)


def _post_error_report(context: dict[str, str]) -> None:
    """Tell the webapp about a failure, with one attempt and a short timeout.

    Not report_error_to_webapp(): that logs a warning to the root logger before
    every attempt, and IPython prints root-logger warnings into the user's cell.
    """
    try:
        response = requests.post(
            get_absolute_userpod_api_url("toolkit/errors"),
            json={
                "type": REPORT_TYPE,
                "message": "DataFrame storage write failed",
                "code": REPORT_CODE,
                "context": context,
            },
            headers=get_project_auth_headers(),
            timeout=REPORT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except Exception as e:  # pylint: disable=broad-exception-caught
        get_logger().warning(
            "Failed to report DataFrame storage failure: %s", type(e).__name__
        )


def _report_failure(exc: Exception) -> None:
    """Report a failed write once per kernel session per errno or exception type.

    The context carries the exception's type, never its message: pyarrow quotes
    the offending value in its messages, and frames often hold personal data.
    """
    exception_type = f"{type(exc).__module__}.{type(exc).__qualname__}"
    if isinstance(exc, OSError) and exc.errno is not None:
        code = errno.errorcode.get(exc.errno, str(exc.errno))
        key = f"io:{code}"
        context = {"kind": "io", "errno": code, "exception_type": exception_type}
    else:
        key = f"error:{exception_type}"
        context = {"kind": "error", "exception_type": exception_type}

    if key in _reported:
        return
    _reported.add(key)
    _post_error_report(context)


def _store_result_frame(result: ExecutionResult) -> None:
    """Store the cell's DataFrame result, reporting any failure to the webapp.

    Only KeyboardInterrupt propagates, so that Stop during the report is not lost.
    """
    try:
        target = _read_storage_target()
        # Helper executions (paging, export, the variable explorer) do not store
        # history. IPython 9 gives them an execution_count anyway, so ask the info.
        if target is None or not result.info.store_history:
            return
        kind = classify_frame(result.result)
        if kind is None:
            return
        root = resolve_project_root()
        # a read-only mount is a deliberate choice, not a failure
        if is_read_only(root):
            return
        with _sigint_raises():
            store_frame(result.result, kind, target.name, root, target.block_id)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if isinstance(exc, OSError) and exc.errno == errno.EROFS:
            return
        _report_failure(exc)


def _on_post_run_cell(result: Optional[ExecutionResult]) -> None:
    """Store the cell's DataFrame result when the platform asked for it.

    Never raises and never emits warnings, because IPython prints both into the
    user's cell. A failed write is reported to the webapp instead.
    """
    if result is None or not _is_enabled_by_environment() or not result.success:
        return

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            _store_result_frame(result)
        except KeyboardInterrupt as exc:
            # The reply is already decided, so this is the only way to make the
            # executor see the interrupt and stop its queue.
            result.error_in_exec = exc
            get_ipython().showtraceback()
