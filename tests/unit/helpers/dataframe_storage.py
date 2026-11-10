"""Helpers shared by the DataFrame storage tests."""

import errno
import json
import os
import time
from pathlib import Path
from typing import Any, List, Optional

import pyarrow as pa
import pytest

from deepnote_core.pydantic_compat_helpers import model_validate_compat
from deepnote_toolkit.artifacts._dataframe_storage_manifest import Manifest
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

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


def manifest_in(directory: Path) -> Manifest:
    raw = json.loads((directory / "manifest.json").read_text())
    return model_validate_compat(Manifest, raw)


def table_in(directory: Path) -> pa.Table:
    """The frame the directory's manifest currently names."""
    path = directory / manifest_in(directory).file
    return pa.ipc.open_file(path).read_all()


def stored_manifest(root: Path, name: str = NAME) -> Manifest:
    return manifest_in(frame_dir(root, name))


def stored_table(root: Path, name: str = NAME) -> pa.Table:
    return table_in(frame_dir(root, name))


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
        pa.Table.from_pandas(frame, preserve_index=None)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return type(exc)
    return None


def query_preview() -> DeepnoteQueryPreview:
    return DeepnoteQueryPreview({"a": [1]}, deepnote_query="select 1")


def cleared_query_preview() -> DeepnoteQueryPreview:
    preview = query_preview()
    # Mutating the frame clears its attached SQL query metadata.
    preview["b"] = 2
    return preview
