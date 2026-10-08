"""Tests for the hook that stores the DataFrame a block with a storage setting returns."""

import errno
import json
import os
import re
import signal
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional
from unittest import mock

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
import requests
import responses
from IPython.core.interactiveshell import ExecutionResult, InteractiveShell

from deepnote_toolkit import artifacts, dataframe_storage
from deepnote_toolkit.ocelots.data_preview import DeepnoteDataFrameWithDataPreview
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview
from tests.unit.helpers.dataframe_storage import (
    ALWAYS_FAILING,
    BLOCK_ID,
    NAME,
    REPORT_URL,
    VERSION_DEPENDENT,
    interrupt_at,
    probe_failure,
    storage_request,
)

# the only runtime type the webapp's toolkit/errors endpoint accepts; its Sentry
# fingerprint comes from `code`
REPORT_TYPE = "TOOLKIT_RUNTIME_ERROR"
REPORT_CODE = "DATAFRAME_STORAGE_WRITE_FAILED"


def _frame_dir(root: Path) -> Path:
    return root / "deepnote_dataframes" / NAME


def _arrow_files(root: Path) -> List[Path]:
    return sorted(_frame_dir(root).glob("*.arrow"))


def _manifest(root: Path) -> Dict[str, Any]:
    return json.loads((_frame_dir(root) / "manifest.json").read_text())


def _stored_table(root: Path) -> pa.Table:
    """The frame the name's manifest currently names."""
    return pa.ipc.open_file(_frame_dir(root) / _manifest(root)["file"]).read_all()


def _assert_nothing_stored(root: Path, reports: SimpleNamespace) -> None:
    assert not (root / "deepnote_dataframes").exists()
    assert reports.bodies == []


def _run(
    shell: InteractiveShell, value: Any, store_history: bool = True
) -> ExecutionResult:
    shell.user_ns["df"] = value
    return shell.run_cell("df", store_history=store_history)


def test_stores_polars_frame(shell: InteractiveShell, root: Path) -> None:
    _run(shell, pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    assert _stored_table(root).to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    assert _manifest(root)["rows"] == 3


@pytest.mark.parametrize(
    "env_value",
    [None, "false", "", "0", "off", "no", "2"],
    ids=["unset", "false", "empty", "zero", "off", "no", "two"],
)
def test_env_var_off_writes_nothing(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    env_value: Optional[str],
) -> None:
    if env_value is None:
        monkeypatch.delenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED")
    else:
        monkeypatch.setenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED", env_value)

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    _assert_nothing_stored(root, reports)


def test_calls_store_frames_with_the_flag_off(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED", raising=False)

    artifacts.store_dataframe(pd.DataFrame({"a": [1]}), NAME)

    assert _stored_table(root).to_pydict() == {"a": [1]}


@pytest.mark.parametrize("env_value", ["1", "true", "TRUE", " yes ", "on"])
def test_env_var_on_writes_the_frame(
    shell: InteractiveShell,
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    env_value: str,
) -> None:
    monkeypatch.setenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED", env_value)

    _run(shell, pd.DataFrame({"a": [1]}))

    assert _stored_table(root).to_pydict() == {"a": [1]}


def test_shell_without_kernel_writes_nothing(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace
) -> None:
    del shell.kernel

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    _assert_nothing_stored(root, reports)


def test_missing_result_is_ignored(root: Path, reports: SimpleNamespace) -> None:
    """ipykernel can trigger post_run_cell without a result."""
    dataframe_storage._on_post_run_cell(None)

    _assert_nothing_stored(root, reports)


def test_helper_execution_writes_nothing(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace
) -> None:
    result = _run(shell, pd.DataFrame({"a": [1]}), store_history=False)

    assert result.success
    _assert_nothing_stored(root, reports)


@pytest.mark.parametrize(
    "cell", ["df\n1/0", "df\nraise KeyboardInterrupt"], ids=["raised", "interrupted"]
)
def test_failed_cell_writes_nothing(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace, cell: str
) -> None:
    shell.user_ns["df"] = pd.DataFrame({"a": [1]})

    result = shell.run_cell(cell, store_history=True)

    assert not result.success
    _assert_nothing_stored(root, reports)


@pytest.mark.parametrize(
    "value",
    [1, pd.Series([1, 2]), pl.DataFrame({"a": [1]}).lazy(), None],
    ids=["int", "series", "lazyframe", "none"],
)
def test_non_dataframe_result_writes_nothing(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace, value: Any
) -> None:
    _run(shell, value)

    _assert_nothing_stored(root, reports)


def test_cell_ending_in_semicolon_writes_nothing(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace
) -> None:
    shell.user_ns["df"] = pd.DataFrame({"a": [1]})

    shell.run_cell("df;", store_history=True)

    _assert_nothing_stored(root, reports)


def _query_preview() -> DeepnoteQueryPreview:
    return DeepnoteQueryPreview({"a": [1]}, deepnote_query="select 1")


def _cleared_query_preview() -> DeepnoteQueryPreview:
    preview = _query_preview()
    preview["b"] = 2
    return preview


@pytest.mark.parametrize(
    "make_preview",
    [
        _query_preview,
        _cleared_query_preview,
        lambda: _query_preview().copy(),
    ],
    ids=["preview", "cleared-query", "copy"],
)
def test_sql_query_preview_writes_nothing(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace, make_preview: Any
) -> None:
    preview = make_preview()
    assert isinstance(preview, DeepnoteQueryPreview)

    _run(shell, preview)

    _assert_nothing_stored(root, reports)


def test_pyspark_result_writes_nothing(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace, spark: Any
) -> None:
    wrapped = DeepnoteDataFrameWithDataPreview(spark.range(3))

    _run(shell, wrapped)

    _assert_nothing_stored(root, reports)


def test_pandas_on_spark_result_writes_nothing(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    spark: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pyspark.pandas warns on import unless told that pyarrow may ignore time zones
    monkeypatch.setenv("PYARROW_IGNORE_TIMEZONE", "1")
    # pyspark 3.5, which Python < 3.13 gets, imports distutils, gone since Python 3.12
    pytest.importorskip("pyspark.pandas")
    # pandas-on-Spark refuses to run under ANSI mode, which Spark 4 enables by default
    ansi_mode = spark.conf.get("spark.sql.ansi.enabled")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        wrapped = DeepnoteDataFrameWithDataPreview(spark.range(3).pandas_api())
    finally:
        spark.conf.set("spark.sql.ansi.enabled", ansi_mode)

    _run(shell, wrapped)

    _assert_nothing_stored(root, reports)


def _displayed(capsys: pytest.CaptureFixture) -> str:
    """Everything the last cells printed, with execution counts masked."""
    captured = capsys.readouterr()
    return re.sub(r"Out\[\d+\]", "Out[N]", captured.out + captured.err)


def _output_without_storage(
    shell: InteractiveShell,
    value: Any,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """What the cell prints when the writer does nothing."""
    monkeypatch.setenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED", "false")
    _run(shell, value)
    output = _displayed(capsys)
    monkeypatch.setenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED", "true")
    return output


def _qualified(exc_type: type) -> str:
    return f"{exc_type.__module__}.{exc_type.__qualname__}"


def _failing_frame_runs(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    make_frame: Callable[[], Any],
) -> Dict[str, Any]:
    """Store a good frame, then run a bad one twice; return what was left behind."""
    _run(shell, pd.DataFrame({"ok": [1]}))
    block = _frame_dir(root)
    manifest_bytes = (block / "manifest.json").read_bytes()
    first = _run(shell, make_frame())
    reported_after_first = len(reports.bodies)
    second = _run(shell, make_frame())
    return {
        "results": (first, second),
        "manifest_bytes": manifest_bytes,
        "reported_after_first": reported_after_first,
        "listing": sorted(path.name for path in block.iterdir()),
    }


@pytest.mark.parametrize("make_frame", ALWAYS_FAILING)
def test_unserializable_frame_is_not_stored_and_reported_once(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    make_frame: Callable[[], Any],
) -> None:
    probed = probe_failure(make_frame())
    assert probed is not None, "pandas is expected to refuse this frame"

    left = _failing_frame_runs(shell, root, reports, make_frame)

    assert all(result.success for result in left["results"])
    assert (_frame_dir(root) / "manifest.json").read_bytes() == left["manifest_bytes"]
    assert left["listing"] == sorted(["manifest.json", _manifest(root)["file"]])
    assert left["reported_after_first"] == 1
    assert len(reports.bodies) == 1
    assert reports.bodies[0]["type"] == REPORT_TYPE
    assert reports.bodies[0]["code"] == REPORT_CODE
    assert reports.bodies[0]["context"] == {
        "kind": "error",
        "exception_type": _qualified(probed),
    }


@pytest.mark.parametrize("make_frame, rows", VERSION_DEPENDENT)
def test_frames_pandas_may_refuse_follow_what_pandas_does(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    make_frame: Callable[[], Any],
    rows: int,
) -> None:
    probed = probe_failure(make_frame())

    left = _failing_frame_runs(shell, root, reports, make_frame)

    assert all(result.success for result in left["results"])
    if probed is None:
        assert reports.bodies == []
        assert _stored_table(root).num_rows == rows
        return
    assert (_frame_dir(root) / "manifest.json").read_bytes() == left["manifest_bytes"]
    assert left["listing"] == sorted(["manifest.json", _manifest(root)["file"]])
    assert [body["context"]["exception_type"] for body in reports.bodies] == [
        _qualified(probed)
    ]


def test_mixed_type_column_failure_names_the_exception_type_only(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [1, "two"]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)

    result = _run(shell, frame)

    assert result.success
    assert _displayed(capsys) == expected_output
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "error", "exception_type": "pyarrow.lib.ArrowInvalid"}
    ]
    posted = reports.calls[0].body.decode()
    assert "two" not in posted
    assert "tried to convert" not in posted
    assert set(reports.bodies[0]) == {"type", "message", "code", "context"}
    assert reports.bodies[0]["message"] == "DataFrame storage write failed"


def test_each_exception_type_is_reported_separately(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace
) -> None:
    _run(shell, pd.DataFrame({"a": [1, "two"]}))
    _run(shell, pd.DataFrame([[1, 2]], columns=["a", "a"]))
    _run(shell, pd.DataFrame({"a": [1, "two"]}))

    assert [body["context"]["exception_type"] for body in reports.bodies] == [
        "pyarrow.lib.ArrowInvalid",
        "builtins.ValueError",
    ]


def test_polars_object_column_is_not_stored_and_reported(
    shell: InteractiveShell, root: Path, reports: SimpleNamespace
) -> None:
    _run(shell, pl.DataFrame({"ok": [1]}))
    manifest_bytes = (_frame_dir(root) / "manifest.json").read_bytes()

    result = _run(shell, pl.DataFrame({"a": pl.Series([object()], dtype=pl.Object)}))

    assert result.success
    assert (_frame_dir(root) / "manifest.json").read_bytes() == manifest_bytes
    assert len(_arrow_files(root)) == 1
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "error", "exception_type": "polars.exceptions.ComputeError"}
    ]


def test_report_is_one_short_authenticated_attempt(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        dataframe_storage,
        "get_project_auth_headers",
        lambda: {"Authorization": "Bearer t"},
    )

    _run(shell, pd.DataFrame({"a": [1, "two"]}))

    (request,) = reports.calls
    assert request.url == REPORT_URL
    assert request.method == "POST"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["Authorization"] == "Bearer t"
    assert request.req_kwargs["timeout"] == 2


def _refuse_url(path: str) -> str:
    raise ValueError("no webapp url")


@pytest.mark.parametrize(
    "break_report, logged_type",
    [
        pytest.param(
            lambda reports, monkeypatch: reports.mock.replace(
                responses.POST, REPORT_URL, body=requests.ConnectionError("down")
            ),
            "ConnectionError",
            id="connection",
        ),
        pytest.param(
            lambda reports, monkeypatch: reports.mock.replace(
                responses.POST, REPORT_URL, body=requests.Timeout("slow")
            ),
            "Timeout",
            id="timeout",
        ),
        pytest.param(
            lambda reports, monkeypatch: reports.mock.replace(
                responses.POST, REPORT_URL, status=400
            ),
            "HTTPError",
            id="rejected-by-the-webapp",
        ),
        pytest.param(
            lambda reports, monkeypatch: monkeypatch.setattr(
                dataframe_storage, "get_absolute_userpod_api_url", _refuse_url
            ),
            "ValueError",
            id="no-url",
        ),
    ],
)
def test_report_that_cannot_be_delivered_is_silent(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    break_report: Callable[[SimpleNamespace, pytest.MonkeyPatch], Any],
    logged_type: str,
) -> None:
    frame = pd.DataFrame({"a": [1, "two"]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)
    logged: List[Any] = []
    break_report(reports, monkeypatch)
    monkeypatch.setattr(
        dataframe_storage,
        "get_logger",
        lambda: SimpleNamespace(warning=lambda *args: logged.append(args)),
    )

    result = _run(shell, frame)

    assert result.success
    assert _displayed(capsys) == expected_output
    # IPython prints root-logger warnings into the cell, so the failure must only
    # go to the file logger, and as the exception type alone
    assert caplog.records == []
    assert [args[1:] for args in logged] == [(logged_type,)]


def test_writer_adds_nothing_to_the_cell(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [1, "two"]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)
    original = pd.DataFrame.to_feather

    def noisy_to_feather(self: pd.DataFrame, *args: Any, **kwargs: Any) -> Any:
        warnings.warn("noisy", UserWarning)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_feather", noisy_to_feather)

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        for value in (frame, pd.DataFrame({"a": [1]})):
            _run(shell, value)

    assert [str(w.message) for w in recorded if "noisy" in str(w.message)] == []
    assert caplog.records == []
    assert len(reports.bodies) == 1
    printed = _displayed(capsys)
    assert printed.count("Error in callback") == 0
    assert printed.startswith(expected_output)


def test_handler_never_raises(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [1]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)

    def broken_root() -> Path:
        raise RuntimeError("no root")

    monkeypatch.setattr(dataframe_storage, "resolve_project_root", broken_root)

    result = _run(shell, frame)

    assert result.success
    assert _displayed(capsys) == expected_output
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "error", "exception_type": "builtins.RuntimeError"}
    ]


def test_eperm_from_close_is_reported_once(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    parent: Dict[str, Any],
    close_error: Dict[str, Optional[OSError]],
) -> None:
    close_error["error"] = PermissionError(errno.EPERM, "Operation not permitted")

    first = _run(shell, pd.DataFrame({"a": [1]}))
    parent["metadata"]["deepnote"]["dataframeStorage"]["name"] = "other"
    second = _run(shell, pd.DataFrame({"a": [2]}))

    assert first.success and second.success
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "io", "errno": "EPERM", "exception_type": "builtins.PermissionError"}
    ]
    assert not list((root / "deepnote_dataframes").rglob("*.arrow"))
    assert not list((root / "deepnote_dataframes").rglob("manifest.json"))


def test_each_io_error_code_is_reported_once_and_not_swallowed(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    close_error: Dict[str, Optional[OSError]],
) -> None:
    for error in (
        OSError(errno.EIO, "Input/output error"),
        OSError(errno.EIO, "Input/output error"),
        OSError(errno.ENOSPC, "No space left on device"),
    ):
        close_error["error"] = error
        assert _run(shell, pd.DataFrame({"a": [1]})).success

    assert [body["context"] for body in reports.bodies] == [
        {"kind": "io", "errno": "EIO", "exception_type": "builtins.OSError"},
        {"kind": "io", "errno": "ENOSPC", "exception_type": "builtins.OSError"},
    ]


def test_partial_file_is_deleted_when_the_write_fails(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def write_some_then_fail(self: pd.DataFrame, sink: Any, **kwargs: Any) -> None:
        sink.write(b"ARROW1 partial")
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(pd.DataFrame, "to_feather", write_some_then_fail)

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    assert _arrow_files(root) == []
    assert len(reports.bodies) == 1


def test_failure_after_the_manifest_step_keeps_the_new_frame(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run(shell, pd.DataFrame({"a": [1]}))

    def broken_cleanup(*args: Any) -> Any:
        raise OSError(errno.EIO, "Input/output error")

    monkeypatch.setattr(dataframe_storage, "_delete_replaced_frames", broken_cleanup)

    result = _run(shell, pd.DataFrame({"a": [2]}))

    assert result.success
    assert _stored_table(root).to_pydict() == {"a": [2]}
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "io", "errno": "EIO", "exception_type": "builtins.OSError"}
    ]


def test_read_only_mount_is_skipped_before_converting(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [1]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)
    monkeypatch.setattr(
        dataframe_storage.os,
        "statvfs",
        lambda path: SimpleNamespace(f_flag=os.ST_RDONLY),
    )

    with mock.patch.object(pd.DataFrame, "to_feather", autospec=True) as to_feather:
        result = _run(shell, frame)

    assert result.success
    to_feather.assert_not_called()
    assert not (root / "deepnote_dataframes").exists()
    assert reports.bodies == []
    assert _displayed(capsys) == expected_output


def test_erofs_from_the_write_is_silent(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [1]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)

    def refuse(self: pd.DataFrame, sink: Any, **kwargs: Any) -> None:
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(pd.DataFrame, "to_feather", refuse)

    result = _run(shell, frame)

    assert result.success
    assert reports.bodies == []
    assert _arrow_files(root) == []
    assert _displayed(capsys) == expected_output


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
def test_interrupt_during_the_write_fails_the_execution(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    queued_sigints: List[int],
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    interrupt: Callable[[], None],
) -> None:
    _run(shell, pd.DataFrame({"a": [1]}))
    manifest_bytes = (_frame_dir(root) / "manifest.json").read_bytes()
    handler_before = signal.getsignal(signal.SIGINT)
    monkeypatch.setattr(pd.DataFrame, "to_feather", _write_partial_file_then(interrupt))
    capsys.readouterr()

    result = _run(shell, pd.DataFrame({"a": [2]}))

    assert isinstance(result.error_in_exec, KeyboardInterrupt)
    assert not result.success
    assert queued_sigints == []
    assert "KeyboardInterrupt" in _displayed(capsys)
    assert reports.bodies == []
    assert (_frame_dir(root) / "manifest.json").read_bytes() == manifest_bytes
    assert len(_arrow_files(root)) == 1
    assert signal.getsignal(signal.SIGINT) is handler_before


@pytest.mark.parametrize(
    "point", ["manifest-truncated", "manifest-written", "old-frames-deleted"]
)
def test_interrupt_after_the_frame_is_written_leaves_a_usable_manifest(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    sigint_handler: List[int],
    monkeypatch: pytest.MonkeyPatch,
    point: str,
) -> None:
    _run(shell, pd.DataFrame({"a": [1]}))
    _run(shell, pd.DataFrame({"a": [2]}))
    handler_before = signal.getsignal(signal.SIGINT)
    interrupt_at(point, monkeypatch)

    result = _run(shell, pd.DataFrame({"a": [3]}))

    assert isinstance(result.error_in_exec, KeyboardInterrupt)
    assert sigint_handler == []
    assert reports.bodies == []
    # parses, and names a frame that is there and complete
    assert _stored_table(root).num_rows == 1
    assert signal.getsignal(signal.SIGINT) is handler_before


def test_interrupt_while_reporting_a_failure_fails_the_execution(
    shell: InteractiveShell,
    root: Path,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def interrupted(context: Dict[str, str]) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(dataframe_storage, "_post_error_report", interrupted)
    capsys.readouterr()

    result = _run(shell, pd.DataFrame({"a": [1, "two"]}))

    assert isinstance(result.error_in_exec, KeyboardInterrupt)
    assert not result.success
    printed = _displayed(capsys)
    assert "KeyboardInterrupt" in printed
    assert "Error in callback" not in printed


def test_sigint_handler_is_restored_after_a_failed_write(
    shell: InteractiveShell,
    root: Path,
    queued_sigints: List[int],
) -> None:
    handler_before = signal.getsignal(signal.SIGINT)

    _run(shell, pd.DataFrame({"a": [1, "two"]}))

    assert signal.getsignal(signal.SIGINT) is handler_before


def test_stores_the_result_under_the_requested_name(
    shell: InteractiveShell, root: Path
) -> None:
    result = _run(shell, pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    assert result.success
    assert _stored_table(root).to_pydict() == {"a": [1, 2, 3], "b": ["x", "y", "z"]}
    manifest = _manifest(root)
    assert set(manifest) == {
        "version",
        "file",
        "written_at",
        "rows",
        "toolkit_version",
        "block_id",
    }
    assert manifest["block_id"] == BLOCK_ID
    assert manifest["rows"] == 3


def test_the_name_in_the_request_chooses_the_directory(
    shell: InteractiveShell, root: Path, parent: Dict[str, Any]
) -> None:
    parent.update(storage_request(name="monthly-2026"))

    _run(shell, pd.DataFrame({"a": [1]}))

    assert (root / "deepnote_dataframes" / "monthly-2026" / "manifest.json").exists()
    assert not (root / "deepnote_dataframes" / NAME).exists()


def test_block_frames_and_calls_share_one_name(
    shell: InteractiveShell, root: Path, parent: Dict[str, Any]
) -> None:
    _run(shell, pd.DataFrame({"a": [1]}))
    first = _manifest(root)
    parent.update(storage_request(blockId="blk2"))
    _run(shell, pd.DataFrame({"a": [2]}))
    second = _manifest(root)
    artifacts.store_dataframe(pd.DataFrame({"a": [3]}), NAME)
    third = _manifest(root)

    assert first["block_id"] == BLOCK_ID
    assert second["block_id"] == "blk2"
    assert second["previous"] == first["file"]
    # a frame that code stored last belongs to no block
    assert "block_id" not in third
    assert third["previous"] == second["file"]
    assert _stored_table(root).to_pydict() == {"a": [3]}


@pytest.mark.parametrize(
    "request_message",
    [
        pytest.param({}, id="no-metadata"),
        pytest.param({"metadata": {}}, id="no-deepnote"),
        pytest.param({"metadata": {"deepnote": {}}}, id="no-storage"),
        pytest.param({"metadata": {"deepnote": "x"}}, id="deepnote-not-a-dict"),
        pytest.param(
            {"metadata": {"deepnote": {"dataframeStorage": []}}},
            id="storage-not-a-dict",
        ),
        pytest.param(
            {"metadata": {"deepnote": {"dataframeStorage": {"blockId": "blk1"}}}},
            id="no-name",
        ),
        pytest.param(
            {"metadata": {"deepnote": {"dataframeStorage": {"name": "revenue"}}}},
            id="no-block-id",
        ),
        pytest.param(storage_request(name="../x"), id="name-traversal"),
        pytest.param(storage_request(name=""), id="name-empty"),
        pytest.param(storage_request(name="a/b"), id="name-slash"),
        pytest.param(storage_request(name="."), id="name-dot"),
        pytest.param(storage_request(name=".."), id="name-dotdot"),
        pytest.param(storage_request(name=5), id="name-int"),
        pytest.param(storage_request(name=None), id="name-null"),
        pytest.param(storage_request(name="x" * 129), id="name-too-long"),
        pytest.param(storage_request(blockId="../x"), id="block-traversal"),
        pytest.param(storage_request(blockId=""), id="block-empty"),
        pytest.param(storage_request(blockId="a/b"), id="block-slash"),
        pytest.param(storage_request(blockId=".."), id="block-dotdot"),
        pytest.param(storage_request(blockId=None), id="block-null"),
        pytest.param(storage_request(blockId="x" * 129), id="block-too-long"),
    ],
)
def test_unusable_request_metadata_writes_nothing(
    shell: InteractiveShell,
    root: Path,
    reports: SimpleNamespace,
    parent: Dict[str, Any],
    request_message: Dict[str, Any],
) -> None:
    parent.clear()
    parent.update(request_message)

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    _assert_nothing_stored(root, reports)
