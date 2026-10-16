"""Helpers and frames shared by the DataFrame storage tests."""

import errno
import io
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import feather

from deepnote_core.pydantic_compat_helpers import model_validate_compat
from deepnote_toolkit.artifacts.dataframe_storage_manifest import Manifest

NAME = "revenue"
DAY = 24 * 60 * 60
FRAME_A = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.arrow"
FRAME_B = "11111111-2222-4333-8444-555555555555.arrow"


def frame_dir(root: Path, name: str = NAME) -> Path:
    return root / "deepnote_dataframes" / name


def arrow_files(root: Path, name: str = NAME) -> List[Path]:
    return sorted(frame_dir(root, name).glob("*.arrow"))


def non_arrow_files(root: Path) -> List[str]:
    return sorted(
        path.name for path in frame_dir(root).iterdir() if path.suffix != ".arrow"
    )


def stored_manifest(root: Path, name: str = NAME) -> Manifest:
    raw = json.loads((frame_dir(root, name) / "manifest.json").read_text())
    return model_validate_compat(Manifest, raw)


def stored_table(root: Path, name: str = NAME) -> pa.Table:
    """The frame the name's manifest currently names."""
    path = frame_dir(root, name) / stored_manifest(root, name).file
    return pa.ipc.open_file(path).read_all()


def backdate(path: Path, age_seconds: float) -> None:
    mtime = time.time() - age_seconds
    os.utime(path, (mtime, mtime), follow_symlinks=False)


def read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail Path.open writes, mkdir and deletes with EROFS, like a read-only mount."""

    def refuse(*args: Any, **kwargs: Any) -> Any:
        raise OSError(errno.EROFS, "Read-only file system")

    real_open = Path.open

    def open_for_reading(self: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if mode.strip("rbt"):
            refuse()
        return real_open(self, mode, *args, **kwargs)

    # The kernel reports EEXIST before EROFS, so an existing directory passes.
    def mkdir(
        self: Path, mode: int = 0o777, parents: bool = False, exist_ok: bool = False
    ) -> None:
        if not (exist_ok and self.is_dir()):
            refuse()

    monkeypatch.setattr(Path, "open", open_for_reading)
    monkeypatch.setattr(Path, "mkdir", mkdir)
    monkeypatch.setattr(os, "unlink", refuse)
    monkeypatch.setattr(os, "rmdir", refuse)


def probe_failure(frame: Any) -> Optional[type]:
    """The exception type pyarrow itself raises for this frame, if any."""
    try:
        feather.write_feather(frame, io.BytesIO())
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return type(exc)
    return None


def dictionary_concat() -> pd.DataFrame:
    dictionary_type = pd.ArrowDtype(pa.dictionary(pa.int32(), pa.string()))
    first = pd.DataFrame({"c": pd.Series(["x", "y"], dtype=dictionary_type)})
    second = pd.DataFrame({"c": pd.Series(["z", "w"], dtype=dictionary_type)})
    return pd.concat([first, second])


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
