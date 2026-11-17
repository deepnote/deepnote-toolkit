"""Tests for the post_run_cell hook that stores a block's DataFrame result."""

import builtins
import errno
import json
import re
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterator, List, Optional

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
import requests
import responses
from IPython.core.interactiveshell import ExecutionResult, InteractiveShell
from traitlets.config import Config

from deepnote_toolkit.artifacts import _block_frames, _dataframe_storage
from deepnote_toolkit.artifacts._block_frames import register_block_frame_storage
from deepnote_toolkit.ocelots.data_preview import DeepnoteDataFrameWithDataPreview
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview
from tests.unit.helpers.dataframe_storage import (
    cleared_query_preview,
    manifest_in,
    query_preview,
    table_in,
)

NOTEBOOK_ID = "nb1"
BLOCK_ID = "blk1"
REPORT_URL = "http://userpod/toolkit/errors"
REPORT_TYPE = "TOOLKIT_RUNTIME_ERROR"
REPORT_CODE = "DATAFRAME_STORAGE_WRITE_FAILED"


def storage_request(**overrides: Any) -> Dict[str, Any]:
    """Return the request message that asks the kernel to store a block's result."""
    return {
        "metadata": {
            "deepnote": {
                "dataframeStorage": {
                    "notebookId": NOTEBOOK_ID,
                    "blockId": BLOCK_ID,
                    **overrides,
                }
            }
        }
    }


def block_dir(root: Path) -> Path:
    return root / ".deepnote" / "dataframes" / NOTEBOOK_ID / BLOCK_ID


def block_arrow_files(root: Path) -> List[Path]:
    return sorted(block_dir(root).glob("*.arrow"))


@pytest.fixture
def parent() -> Dict[str, Any]:
    """The execute_request the fake kernel reports; tests mutate it in place."""
    return storage_request()


@pytest.fixture
def reports(monkeypatch: pytest.MonkeyPatch) -> Iterator[SimpleNamespace]:
    """Replace the webapp transport; `bodies` holds each posted JSON body."""
    transport = SimpleNamespace(bodies=[], calls=[], mock=None)

    def record(request: Any) -> Any:
        transport.calls.append(request)
        transport.bodies.append(json.loads(request.body))
        return 200, {}, ""

    monkeypatch.setattr(
        _block_frames,
        "get_absolute_userpod_api_url",
        lambda path: f"http://userpod/{path}",
    )
    _block_frames._reported.clear()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        mock.add_callback(responses.POST, REPORT_URL, callback=record)
        transport.mock = mock
        yield transport
    _block_frames._reported.clear()


@pytest.fixture
def shell(
    parent: Dict[str, Any],
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[InteractiveShell]:
    """A shell whose kernel reports `parent`, with the hook registered and enabled."""
    monkeypatch.setenv(_block_frames.ENABLED_ENV_VAR, "true")
    # A shell swaps in its own __main__ and adds builtins; leave both as found
    monkeypatch.setitem(sys.modules, "__main__", sys.modules["__main__"])
    builtins_before = set(vars(builtins))
    # No history: it would write a sqlite file into the user's profile, and its
    # sqlite3 usage raises a DeprecationWarning on Python 3.12+
    config = Config()
    config.HistoryManager.enabled = False
    InteractiveShell.clear_instance()
    shell = InteractiveShell.instance(config=config)
    shell.ast_node_interactivity = "all"
    shell.kernel = SimpleNamespace(get_parent=lambda channel=None: parent)
    register_block_frame_storage()
    yield shell
    shell.events.unregister("post_run_cell", _block_frames._on_post_run_cell)
    InteractiveShell.clear_instance()
    for name in set(vars(builtins)) - builtins_before:
        delattr(builtins, name)


def _assert_nothing_stored(tmp_project_root: Path, reports: SimpleNamespace) -> None:
    assert list(tmp_project_root.iterdir()) == []
    assert reports.bodies == []


def _run(
    shell: InteractiveShell, value: Any, store_history: bool = True
) -> ExecutionResult:
    shell.user_ns["df"] = value
    return shell.run_cell("df", store_history=store_history)


def test_stores_the_result_in_the_blocks_directory(
    shell: InteractiveShell, tmp_project_root: Path
) -> None:
    result = _run(shell, pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    assert result.success
    directory = tmp_project_root / ".deepnote" / "dataframes" / "nb1" / "blk1"
    assert table_in(directory).to_pydict() == {
        "a": [1, 2, 3],
        "b": ["x", "y", "z"],
    }
    assert manifest_in(directory).rows == 3


def test_stores_polars_frame(shell: InteractiveShell, tmp_project_root: Path) -> None:
    _run(shell, pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    assert table_in(block_dir(tmp_project_root)).to_pydict() == {
        "a": [1, 2, 3],
        "b": ["x", "y", "z"],
    }


def test_env_var_unset_writes_nothing(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED")

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    assert _callback_errors(_displayed(capsys)) == []
    _assert_nothing_stored(tmp_project_root, reports)


def test_env_var_set_to_another_value_writes_nothing(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED", "2")

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    _assert_nothing_stored(tmp_project_root, reports)


@pytest.mark.parametrize("env_value", ["1", "TRUE", " yes ", "on"])
def test_env_var_on_writes_the_frame(
    shell: InteractiveShell,
    tmp_project_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    env_value: str,
) -> None:
    monkeypatch.setenv("DEEPNOTE_DATAFRAME_STORAGE_ENABLED", env_value)

    _run(shell, pd.DataFrame({"a": [1]}))

    assert table_in(block_dir(tmp_project_root)).to_pydict() == {"a": [1]}


def test_shell_without_kernel_writes_nothing(
    shell: InteractiveShell, tmp_project_root: Path, reports: SimpleNamespace
) -> None:
    del shell.kernel

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    _assert_nothing_stored(tmp_project_root, reports)


def test_missing_result_is_ignored(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
) -> None:
    """ipykernel can trigger post_run_cell without a result."""
    shell.events.trigger("post_run_cell", None)

    assert _callback_errors(_displayed(capsys)) == []
    _assert_nothing_stored(tmp_project_root, reports)


def test_helper_execution_writes_nothing(
    shell: InteractiveShell, tmp_project_root: Path, reports: SimpleNamespace
) -> None:
    result = _run(shell, pd.DataFrame({"a": [1]}), store_history=False)

    assert result.success
    _assert_nothing_stored(tmp_project_root, reports)


def test_interrupted_cell_writes_nothing(
    shell: InteractiveShell, tmp_project_root: Path, reports: SimpleNamespace
) -> None:
    shell.user_ns["df"] = pd.DataFrame({"a": [1]})

    result = shell.run_cell("df\nraise KeyboardInterrupt", store_history=True)

    assert not result.success
    _assert_nothing_stored(tmp_project_root, reports)


@pytest.mark.parametrize(
    "value",
    [pd.Series([1, 2]), pl.DataFrame({"a": [1]}).lazy()],
    ids=["series", "lazyframe"],
)
def test_non_dataframe_result_writes_nothing(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    value: Any,
) -> None:
    _run(shell, value)

    _assert_nothing_stored(tmp_project_root, reports)


@pytest.mark.parametrize(
    "make_preview",
    [
        query_preview,
        cleared_query_preview,
        lambda: query_preview().copy(),
    ],
    ids=["preview", "cleared-query", "copy"],
)
def test_sql_query_preview_writes_nothing(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    make_preview: Callable[[], DeepnoteQueryPreview],
) -> None:
    preview = make_preview()
    assert isinstance(preview, DeepnoteQueryPreview)

    _run(shell, preview)

    _assert_nothing_stored(tmp_project_root, reports)


def test_pyspark_result_writes_nothing(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    spark: Any,
) -> None:
    wrapped = DeepnoteDataFrameWithDataPreview(spark.range(3))

    _run(shell, wrapped)

    _assert_nothing_stored(tmp_project_root, reports)


# Displaying the result runs pyspark 3.5 code that uses deprecated distutils and
# numpy APIs; Python < 3.13 gets pyspark 3.5.
@pytest.mark.filterwarnings("ignore::DeprecationWarning:pyspark")
def test_pandas_on_spark_result_writes_nothing(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    spark_without_ansi_mode: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pyspark.pandas warns on import unless told that pyarrow may ignore time zones
    monkeypatch.setenv("PYARROW_IGNORE_TIMEZONE", "1")
    # pyspark 3.5, which Python < 3.13 gets, imports distutils, gone since Python 3.12
    pytest.importorskip("pyspark.pandas")
    wrapped = DeepnoteDataFrameWithDataPreview(
        spark_without_ansi_mode.range(3).pandas_api()
    )

    _run(shell, wrapped)

    _assert_nothing_stored(tmp_project_root, reports)


def _displayed(capsys: pytest.CaptureFixture) -> str:
    """Everything printed since the last read, with execution counts masked."""
    captured = capsys.readouterr()
    return re.sub(r"Out\[\d+\]", "Out[N]", captured.out + captured.err)


def _callback_errors(printed: str) -> List[str]:
    """IPython's "Error in callback" lines, skipping traceback lines that quote it."""
    return re.findall(r"^Error in callback .*", printed, flags=re.MULTILINE)


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


def test_mixed_type_column_failure_names_the_exception_type_only(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [1, "two"]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)

    result = _run(shell, frame)

    assert result.success
    assert _displayed(capsys) == expected_output
    assert reports.bodies == [
        {
            "type": REPORT_TYPE,
            "message": "DataFrame storage write failed",
            "code": REPORT_CODE,
            "context": {"kind": "error", "exception_type": "pyarrow.lib.ArrowInvalid"},
        }
    ]


def test_each_exception_type_is_reported_separately(
    shell: InteractiveShell, tmp_project_root: Path, reports: SimpleNamespace
) -> None:
    _run(shell, pd.DataFrame({"a": [1, "two"]}))
    _run(shell, pd.DataFrame([[1, 2]], columns=["a", "a"]))
    _run(shell, pd.DataFrame({"a": [1, "two"]}))

    assert [body["context"]["exception_type"] for body in reports.bodies] == [
        "pyarrow.lib.ArrowInvalid",
        "builtins.ValueError",
    ]


def test_report_is_one_short_authenticated_attempt(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _block_frames,
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
                _block_frames, "get_absolute_userpod_api_url", _refuse_url
            ),
            "ValueError",
            id="no-url",
        ),
    ],
)
def test_report_that_cannot_be_delivered_is_silent(
    shell: InteractiveShell,
    tmp_project_root: Path,
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
        _block_frames,
        "get_logger",
        lambda: SimpleNamespace(warning=lambda *args: logged.append(args)),
    )

    result = _run(shell, frame)

    assert result.success
    assert _displayed(capsys) == expected_output
    assert caplog.records == []
    assert [args[1:] for args in logged] == [(logged_type,)]


def test_writer_adds_nothing_to_the_cell(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unserializable = pd.DataFrame({"a": [1, "two"]})
    storable = pd.DataFrame({"a": [1]})
    expected_output = _output_without_storage(
        shell, unserializable, capsys, monkeypatch
    ) + _output_without_storage(shell, storable, capsys, monkeypatch)
    real_to_arrow_table = _dataframe_storage._to_arrow_table

    def noisy_to_arrow_table(frame: Any) -> pa.Table:
        warnings.warn("noisy", UserWarning)
        return real_to_arrow_table(frame)

    monkeypatch.setattr(_dataframe_storage, "_to_arrow_table", noisy_to_arrow_table)

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        _run(shell, unserializable)
        _run(shell, storable)

    assert "noisy" not in [str(w.message) for w in recorded]
    assert caplog.records == []
    assert len(reports.bodies) == 1
    assert _displayed(capsys) == expected_output


def test_unexpected_error_is_reported_not_raised(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [1]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)

    def broken_root() -> Path:
        raise RuntimeError("no root")

    monkeypatch.setattr(_dataframe_storage, "resolve_project_root", broken_root)

    result = _run(shell, frame)

    assert result.success
    assert _displayed(capsys) == expected_output
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "error", "exception_type": "builtins.RuntimeError"}
    ]


def test_eperm_from_close_is_reported_once(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    parent: Dict[str, Any],
    close_upload_error: Dict[str, Optional[OSError]],
) -> None:
    close_upload_error["error"] = PermissionError(
        errno.EPERM, "Operation not permitted"
    )

    first = _run(shell, pd.DataFrame({"a": [1]}))
    parent["metadata"]["deepnote"]["dataframeStorage"]["blockId"] = "blk2"
    second = _run(shell, pd.DataFrame({"a": [2]}))

    assert first.success and second.success
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "io", "errno": "EPERM", "exception_type": "builtins.PermissionError"}
    ]
    assert not list(tmp_project_root.rglob("*.arrow"))
    assert not list(tmp_project_root.rglob("manifest.json"))


def test_each_io_error_code_is_reported_once_and_not_swallowed(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    close_upload_error: Dict[str, Optional[OSError]],
) -> None:
    close_upload_error["error"] = OSError(errno.EIO, "Input/output error")
    first = _run(shell, pd.DataFrame({"a": [1]}))
    second = _run(shell, pd.DataFrame({"a": [1]}))
    close_upload_error["error"] = OSError(errno.ENOSPC, "No space left on device")
    third = _run(shell, pd.DataFrame({"a": [1]}))

    assert (first.success, second.success, third.success) == (True, True, True)
    assert [body["context"] for body in reports.bodies] == [
        {"kind": "io", "errno": "EIO", "exception_type": "builtins.OSError"},
        {"kind": "io", "errno": "ENOSPC", "exception_type": "builtins.OSError"},
    ]


def _write_a_batch_then_raise(
    monkeypatch: pytest.MonkeyPatch, exc: BaseException
) -> None:
    real_write_batch = pa.ipc.RecordBatchFileWriter.write_batch

    def write_a_batch_then_raise(
        self: pa.ipc.RecordBatchFileWriter, batch: pa.RecordBatch, **kwargs: Any
    ) -> None:
        real_write_batch(self, batch, **kwargs)
        raise exc

    monkeypatch.setattr(
        pa.ipc.RecordBatchFileWriter, "write_batch", write_a_batch_then_raise
    )


def test_erofs_after_the_manifest_step_is_silent(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = pd.DataFrame({"a": [3]})
    expected_output = _output_without_storage(shell, frame, capsys, monkeypatch)
    _run(shell, pd.DataFrame({"a": [1]}))
    _run(shell, pd.DataFrame({"a": [2]}))
    block = block_dir(tmp_project_root)
    previous = manifest_in(block).previous
    assert previous is not None
    replaced = block / previous
    real_unlink = Path.unlink

    def unlink_refused_for_the_replaced_frame(
        self: Path, *args: Any, **kwargs: Any
    ) -> None:
        if self == replaced:
            raise OSError(errno.EROFS, "Read-only file system")
        real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink_refused_for_the_replaced_frame)
    capsys.readouterr()

    result = _run(shell, frame)

    assert result.success
    assert table_in(block).to_pydict() == {"a": [3]}
    assert replaced.exists()
    assert reports.bodies == []
    assert _displayed(capsys) == expected_output


def test_interrupt_during_the_write_fails_the_execution(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _run(shell, pd.DataFrame({"a": [1]}))
    block = block_dir(tmp_project_root)
    manifest_bytes = (block / "manifest.json").read_bytes()
    _write_a_batch_then_raise(monkeypatch, KeyboardInterrupt())
    capsys.readouterr()

    result = _run(shell, pd.DataFrame({"a": [2]}))

    assert isinstance(result.error_in_exec, KeyboardInterrupt)
    assert not result.success
    assert "KeyboardInterrupt" in _displayed(capsys)
    assert reports.bodies == []
    assert (block / "manifest.json").read_bytes() == manifest_bytes
    assert len(block_arrow_files(tmp_project_root)) == 1


def test_interrupt_while_reporting_a_failure_fails_the_execution(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    capsys: pytest.CaptureFixture,
) -> None:
    def interrupted(request: Any) -> Any:
        raise KeyboardInterrupt

    reports.mock.replace(
        responses.CallbackResponse(responses.POST, REPORT_URL, callback=interrupted)
    )
    capsys.readouterr()

    result = _run(shell, pd.DataFrame({"a": [1, "two"]}))

    assert isinstance(result.error_in_exec, KeyboardInterrupt)
    assert not result.success
    printed = _displayed(capsys)
    assert "KeyboardInterrupt" in printed
    assert _callback_errors(printed) == []


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
            {"metadata": {"deepnote": {"dataframeStorage": {"blockId": BLOCK_ID}}}},
            id="no-notebook-id",
        ),
        pytest.param(
            {
                "metadata": {
                    "deepnote": {"dataframeStorage": {"notebookId": NOTEBOOK_ID}}
                }
            },
            id="no-block-id",
        ),
        pytest.param(storage_request(notebookId="../x"), id="notebook-traversal"),
        pytest.param(storage_request(blockId="../x"), id="block-traversal"),
    ],
)
def test_unusable_request_metadata_writes_nothing(
    shell: InteractiveShell,
    tmp_project_root: Path,
    reports: SimpleNamespace,
    parent: Dict[str, Any],
    request_message: Dict[str, Any],
) -> None:
    parent.clear()
    parent.update(request_message)

    result = _run(shell, pd.DataFrame({"a": [1]}))

    assert result.success
    _assert_nothing_stored(tmp_project_root, reports)
