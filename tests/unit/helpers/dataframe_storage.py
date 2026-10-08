"""Frames and interrupts shared by the DataFrame storage tests."""

import io
import signal
import uuid
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from deepnote_toolkit import dataframe_storage


def probe_failure(frame: Any) -> Optional[type]:
    """The exception type pandas itself raises for this frame, if any."""
    try:
        frame.to_feather(io.BytesIO())
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return type(exc)
    return None


def dictionary_concat() -> pd.DataFrame:
    dictionary_type = pd.ArrowDtype(pa.dictionary(pa.int32(), pa.string()))
    first = pd.DataFrame({"c": pd.Series(["x", "y"], dtype=dictionary_type)})
    second = pd.DataFrame({"c": pd.Series(["z", "w"], dtype=dictionary_type)})
    return pd.concat([first, second])


# Frames the RFC lists as failing on every supported pyarrow. The UUID column and the
# dictionary concatenation only fail on some versions, so for them whether the frame
# is stored is decided by asking pandas directly.
ALWAYS_FAILING = [
    pytest.param(lambda: pd.DataFrame({"a": [1, "two"]}), id="mixed-types"),
    pytest.param(
        lambda: pd.DataFrame([[1, 2]], columns=["a", "a"]), id="duplicate-labels"
    ),
    pytest.param(lambda: pd.DataFrame({"a": [2**70]}), id="beyond-int64"),
    pytest.param(lambda: pd.DataFrame({"a": np.array([1 + 2j])}), id="complex"),
    pytest.param(lambda: pd.DataFrame({"a": ["\ud800"]}), id="lone-surrogate"),
]
VERSION_DEPENDENT = [
    pytest.param(lambda: pd.DataFrame({"a": [uuid.UUID(int=1)]}), 1, id="uuid"),
    pytest.param(dictionary_concat, 4, id="dictionary-concat"),
]


class SigintBeforeWrite:
    """A handle that gets SIGINT after the file was truncated, before it is written."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle

    def write(self, data: str) -> int:
        signal.raise_signal(signal.SIGINT)
        return self._handle.write(data)

    def __enter__(self) -> "SigintBeforeWrite":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self._handle.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


def interrupt_at(point: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the next write receive SIGINT at the named point after its frame file."""
    if point == "manifest-truncated":
        real_open = Path.open

        def open_manifest(
            self: Path, mode: str = "r", *args: Any, **kwargs: Any
        ) -> Any:
            handle = real_open(self, mode, *args, **kwargs)
            if self.name == "manifest.json" and "w" in mode:
                return SigintBeforeWrite(handle)
            return handle

        monkeypatch.setattr(Path, "open", open_manifest)
    elif point == "manifest-written":
        write_manifest = dataframe_storage.write_manifest

        def write_manifest_then_interrupt(*args: Any, **kwargs: Any) -> None:
            write_manifest(*args, **kwargs)
            signal.raise_signal(signal.SIGINT)

        monkeypatch.setattr(
            dataframe_storage, "write_manifest", write_manifest_then_interrupt
        )
    else:
        delete_replaced_frames = dataframe_storage._delete_replaced_frames

        def interrupt_then_delete(*args: Any, **kwargs: Any) -> None:
            signal.raise_signal(signal.SIGINT)
            delete_replaced_frames(*args, **kwargs)

        monkeypatch.setattr(
            dataframe_storage, "_delete_replaced_frames", interrupt_then_delete
        )
