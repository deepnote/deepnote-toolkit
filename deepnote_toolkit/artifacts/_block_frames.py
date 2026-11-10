"""Stores a block's DataFrame result when the platform asks for it."""

import errno
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from IPython import get_ipython
from IPython.core.interactiveshell import ExecutionResult

from deepnote_toolkit import env
from deepnote_toolkit.get_webapp_url import (
    get_absolute_userpod_api_url,
    get_project_auth_headers,
)
from deepnote_toolkit.logging import get_logger

from . import _dataframe_storage

ENABLED_ENV_VAR = "DEEPNOTE_DATAFRAME_STORAGE_ENABLED"
# The webapp's dataframe reader serves block frames from here.
BLOCK_FRAMES_DIR = Path(".deepnote") / "dataframes"
# toolkit/errors accepts only TOOLKIT_IMPORT_ERROR and TOOLKIT_RUNTIME_ERROR, and
# sends the latter to Sentry, grouped by `code`.
REPORT_TYPE = "TOOLKIT_RUNTIME_ERROR"
REPORT_CODE = "DATAFRAME_STORAGE_WRITE_FAILED"
REPORT_TIMEOUT_SECONDS = 2


@dataclass(frozen=True)
class StorageTarget:
    """The block whose result the platform asked the current execution to store."""

    notebook_id: str
    block_id: str


_reported: set[str] = set()


def register_block_frame_storage() -> None:
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

    node: object = kernel.get_parent()
    for key in ("metadata", "deepnote", "dataframeStorage"):
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    if not isinstance(node, dict):
        return None

    notebook_id, block_id = node.get("notebookId"), node.get("blockId")
    if not (
        _dataframe_storage.is_safe_id(notebook_id)
        and _dataframe_storage.is_safe_id(block_id)
    ):
        return None
    return StorageTarget(notebook_id=notebook_id, block_id=block_id)


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
    """Store the cell's DataFrame result; report any failure but EROFS to the webapp."""
    try:
        target = _read_storage_target()
        # Helper executions don't store history; IPython 9 may give them an
        # execution_count anyway.
        if target is None or not result.info.store_history:
            return
        if not _dataframe_storage.is_storable_frame(result.result):
            return
        directory = (
            _dataframe_storage.resolve_project_root()
            / BLOCK_FRAMES_DIR
            / target.notebook_id
            / target.block_id
        )
        _dataframe_storage.store_frame(result.result, directory)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        if isinstance(exc, OSError) and exc.errno == errno.EROFS:
            return
        _report_failure(exc)


def _on_post_run_cell(result: Optional[ExecutionResult]) -> None:
    """Store the cell's DataFrame result when the platform asked for it.

    IPython prints a callback's exceptions into the cell as "Error in callback",
    so failures go to the webapp instead. Warnings are ignored because they would
    reach the cell too.
    """
    if result is None or not _is_enabled_by_environment() or not result.success:
        return

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            _store_result_frame(result)
        except KeyboardInterrupt as exc:
            # Re-raised, the interrupt would leave the cell successful; ipykernel
            # builds the reply from `result` after this hook.
            result.error_in_exec = exc
            get_ipython().showtraceback()
