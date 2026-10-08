"""Fixtures for the DataFrame storage tests.

Kept out of tests/unit/conftest.py because the names are generic.
"""

import builtins
import json
import signal
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional

import pytest
import responses
from IPython.core.interactiveshell import InteractiveShell
from traitlets.config import Config

from deepnote_toolkit import dataframe_storage
from deepnote_toolkit.dataframe_storage import register_dataframe_storage
from tests.unit.helpers.dataframe_storage import REPORT_URL, storage_request


@pytest.fixture
def parent() -> Dict[str, Any]:
    """The execute_request the fake kernel reports; tests mutate it in place."""
    return storage_request()


@pytest.fixture
def reports(monkeypatch: pytest.MonkeyPatch) -> Iterator[SimpleNamespace]:
    """Replace the webapp transport; `bodies` holds each posted JSON body.

    The writer remembers what it already reported in module state, so it is
    cleared around every test.
    """
    transport = SimpleNamespace(bodies=[], calls=[], mock=None)

    def record(request: Any) -> Any:
        transport.calls.append(request)
        transport.bodies.append(json.loads(request.body))
        return 200, {}, ""

    monkeypatch.setattr(
        dataframe_storage,
        "get_absolute_userpod_api_url",
        lambda path: f"http://userpod/{path}",
    )
    dataframe_storage._reported.clear()
    with responses.RequestsMock(assert_all_requests_are_fired=False) as mock:
        mock.add_callback(responses.POST, REPORT_URL, callback=record)
        transport.mock = mock
        yield transport
    dataframe_storage._reported.clear()


@pytest.fixture
def shell(
    parent: Dict[str, Any],
    reports: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[InteractiveShell]:
    """A shell whose kernel reports `parent`, with the hook registered and enabled."""
    monkeypatch.setenv(dataframe_storage.ENABLED_ENV_VAR, "true")
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
    register_dataframe_storage()
    yield shell
    shell.events.unregister("post_run_cell", dataframe_storage._on_post_run_cell)
    InteractiveShell.clear_instance()
    for name in set(vars(builtins)) - builtins_before:
        delattr(builtins, name)


@pytest.fixture
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """The project root, a fresh directory."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    monkeypatch.setattr(dataframe_storage, "resolve_project_root", lambda: project_root)
    return project_root


@pytest.fixture
def tokyo_time() -> Iterator[None]:
    """Run in a local time zone nine hours ahead of UTC."""
    if not hasattr(time, "tzset"):
        pytest.skip("time zones cannot be switched on this platform")
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("TZ", "Asia/Tokyo")
        time.tzset()
        yield
    # the C library caches the zone, so restoring the variable is not enough
    time.tzset()


@pytest.fixture(scope="module")
def spark() -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local")  # type: ignore
        .appName("Toolkit")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


@pytest.fixture
def arrow_sinks(monkeypatch: pytest.MonkeyPatch) -> List[Any]:
    """Every file object opened for writing a frame, in order."""
    sinks: List[Any] = []
    real_open = Path.open

    def open_and_record(self: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        handle = real_open(self, mode, *args, **kwargs)
        if self.suffix == ".arrow" and "w" in mode:
            sinks.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", open_and_record)
    return sinks


class _FailingClose:
    """A write handle whose close() really closes, then fails like s3fs's upload."""

    def __init__(self, handle: Any, error: OSError) -> None:
        self._handle = handle
        self._error = error

    def close(self) -> None:
        self._handle.close()
        raise self._error

    def __enter__(self) -> "_FailingClose":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


@pytest.fixture
def close_error(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Optional[OSError]]:
    """Set `["error"]` to make closing a frame file fail with that error."""
    state: Dict[str, Optional[OSError]] = {"error": None}
    real_open = Path.open

    def open_failing_on_close(
        self: Path, mode: str = "r", *args: Any, **kwargs: Any
    ) -> Any:
        handle = real_open(self, mode, *args, **kwargs)
        error = state["error"]
        if error is None or self.suffix != ".arrow" or "w" not in mode:
            return handle
        return _FailingClose(handle, error)

    monkeypatch.setattr(Path, "open", open_failing_on_close)
    return state


@pytest.fixture
def queued_sigints() -> Iterator[List[int]]:
    """Stand in for the handler ipykernel installs while it runs a cell.

    It only queues the interrupt. Under pytest SIGINT already raises
    KeyboardInterrupt, so without this the interrupt tests would pass vacuously.
    """
    queued: List[int] = []
    original = signal.signal(signal.SIGINT, lambda *args: queued.append(1))
    yield queued
    signal.signal(signal.SIGINT, original)


@pytest.fixture(params=["ordinary-cell", "top-level-await-cell"])
def sigint_handler(request: pytest.FixtureRequest) -> Iterator[List[int]]:
    """The handler a cell's post_run_cell runs under, and what it has queued.

    ipykernel keeps default_int_handler for ordinary cells, so SIGINT raises
    anywhere. Only a cell with top-level await runs under a handler that queues.
    """
    queued: List[int] = []

    def only_queue(*args: Any) -> None:
        queued.append(1)

    handler: Any = only_queue
    if request.param == "ordinary-cell":
        handler = signal.default_int_handler
    original = signal.signal(signal.SIGINT, handler)
    yield queued
    signal.signal(signal.SIGINT, original)


@pytest.fixture
def no_root_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "DEEPNOTE_PATHS__NOTEBOOK_ROOT",
        "DEEPNOTE_PATHS__HOME_DIR",
        "DEEPNOTE_HOME_DIR",
        "HOME_DIR",
        "DEEPNOTE_RUNTIME__DEV_MODE",
        "DEEPNOTE_RUNNING_IN_DEV_MODE",
        "RUNNING_IN_DEV_MODE",
    ):
        monkeypatch.delenv(name, raising=False)
