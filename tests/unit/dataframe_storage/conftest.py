"""Fixtures for the DataFrame storage tests.

Kept out of tests/unit/conftest.py because the names are generic.
"""

import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pytest

from deepnote_toolkit import dataframe_storage


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
