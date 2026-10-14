"""Writes named DataFrames into the project's filesystem as Arrow files."""

import contextlib
import errno
import os
import re
import time
import uuid
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional

import requests
from IPython import get_ipython
from IPython.core.interactiveshell import ExecutionResult
from packaging.version import Version

from deepnote_toolkit import env
from deepnote_toolkit.config import resolve_notebook_root
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

from .dataframe_storage_manifest import (
    Manifest,
    is_frame_name,
    read_manifest,
    write_manifest,
)

ENABLED_ENV_VAR = "DEEPNOTE_DATAFRAME_STORAGE_ENABLED"
FRAMES_DIR = Path("deepnote_dataframes")
STALE_FRAME_AGE_SECONDS = 24 * 60 * 60
# toolkit/errors accepts only TOOLKIT_IMPORT_ERROR and TOOLKIT_RUNTIME_ERROR, and
# sends the latter to Sentry, grouped by `code`.
REPORT_TYPE = "TOOLKIT_RUNTIME_ERROR"
REPORT_CODE = "DATAFRAME_STORAGE_WRITE_FAILED"
REPORT_TIMEOUT_SECONDS = 2
# Each record batch is one page of the read endpoint; to_feather already writes
# 64k-row batches.
POLARS_BATCH_ROWS = 65_536
POLARS_BATCH_MIN_VERSION = Version("1.37")
# From 1.0 polars writes strings and binaries as views, which pyarrow before 15 and
# apache-arrow before 21.2 can't read; `pl.CompatLevel` is public from 1.2.1.
POLARS_COMPAT_LEVEL_MIN_VERSION = Version("1.2.1")

FrameKind = Literal["pandas", "polars"]

_SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,128}")


@dataclass(frozen=True)
class StorageTarget:
    """What the platform asked the current execution to store, and for which block."""

    name: str
    block_id: str


_reported: set[str] = set()


def is_safe_id(value: object) -> bool:
    """Return whether `value` can be used as a single path segment."""
    return isinstance(value, str) and _SAFE_ID.fullmatch(value) is not None


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


def is_read_only(root: Path) -> bool:
    """Return whether the project's filesystem is mounted read-only."""
    return bool(os.statvfs(root).f_flag & os.ST_RDONLY)


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
            if Version(polars.__version__) >= POLARS_COMPAT_LEVEL_MIN_VERSION:
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


def store_frame(
    frame: Any,
    kind: FrameKind,
    name: str,
    root: Path,
    block_id: Optional[str] = None,
) -> str:
    """Store the frame as the current one under `name` and return its reference ID."""
    directory = root / FRAMES_DIR / name
    directory.mkdir(parents=True, exist_ok=True)

    reference_id = str(uuid.uuid4())
    new_file = f"{reference_id}.arrow"
    try:
        _write_frame_file(frame, kind, directory / new_file, _select_compression())
        old = read_manifest(directory)
    except BaseException:
        with contextlib.suppress(OSError):
            (directory / new_file).unlink(missing_ok=True)
        raise
    # A failure from here on keeps the new frame: an s3fs rename can put the new
    # manifest in place and still fail.
    write_manifest(directory, new_file, old.file if old else None, len(frame), block_id)
    _delete_replaced_frames(directory, old, new_file)
    return reference_id


def register_dataframe_storage() -> None:
    """Register the post_run_cell hook that stores a block's DataFrame result."""
    get_ipython().events.register("post_run_cell", _on_post_run_cell)


def _is_enabled_by_environment() -> bool:
    """Return whether the platform switched the feature on for this kernel."""
    value = env.get_env(ENABLED_ENV_VAR)
    return value is not None and value.strip().lower() in {"1", "true", "yes", "on"}


def _read_storage_target() -> Optional[StorageTarget]:
    """Return the storage target in the current execute_request, if it is usable."""
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

    Not report_error_to_webapp(): it sends no `code`, retries with sleeps, and logs
    a root-logger warning, which reaches the user's cell.
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
    """Store the cell's DataFrame result, reporting a failure to the webapp."""
    try:
        target = _read_storage_target()
        # Helper executions don't store history; IPython 9 may give them an
        # execution_count anyway.
        if target is None or not result.info.store_history:
            return
        kind = classify_frame(result.result)
        if kind is None:
            return
        root = resolve_project_root()
        if is_read_only(root):
            return
        store_frame(result.result, kind, target.name, root, target.block_id)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if isinstance(exc, OSError) and exc.errno == errno.EROFS:
            return
        _report_failure(exc)


def _on_post_run_cell(result: Optional[ExecutionResult]) -> None:
    """Store the cell's DataFrame result when the platform asked for it.

    IPython prints a callback's exceptions into the cell as "Error in callback", and
    warnings reach the cell too, so failures go to the webapp instead.
    """
    if result is None or not _is_enabled_by_environment() or not result.success:
        return

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            _store_result_frame(result)
        except KeyboardInterrupt as exc:
            # Re-raised, IPython would print "Error in callback" and keep the cell
            # successful; ipykernel builds the reply from `result` after this hook.
            result.error_in_exec = exc
            get_ipython().showtraceback()
