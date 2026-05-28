"""Snapshot storage backend interface and local-disk implementation.

The S3 production backend will be a sibling implementation. Callers must use
the `SnapshotStore` protocol so swapping is a one-line change.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Protocol


class SnapshotStore(Protocol):
    """Read/write opaque byte blobs by string key."""

    def read(self, key: str) -> bytes | None:
        """Return the bytes for `key`, or None if absent."""
        ...

    def write(self, key: str, data: bytes) -> None:
        """Write `data` under `key`, overwriting any prior value."""
        ...


class LocalDiskSnapshotStore:
    """File-system implementation. Keys are mapped to files under `root`.

    Atomic write (temp + rename) so a crash mid-write doesn't leave a corrupt
    snapshot that would later deserialise to garbage. Forward-slashes in keys
    are encoded so the key composition is free to use any separator.

    The default root is `/tmp/deepnote-checkpoint/`. Production-shaped use
    would point this at a per-project persistent volume, or replace the whole
    class with an S3-backed implementation.
    """

    def __init__(self, root: str | Path = "/tmp/deepnote-checkpoint") -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, key: str) -> Path:
        # Forward slashes and colons would create unwanted directories; flatten.
        safe = key.replace("/", "__").replace(":", "_")
        return self._root / safe

    def read(self, key: str) -> bytes | None:
        path = self._path_for(key)
        if not path.exists():
            return None
        return path.read_bytes()

    def write(self, key: str, data: bytes) -> None:
        path = self._path_for(key)
        # Atomic write: temp file in the same dir, then rename. Avoids a partial
        # file being readable as a "successful" snapshot.
        fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp_path, path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
