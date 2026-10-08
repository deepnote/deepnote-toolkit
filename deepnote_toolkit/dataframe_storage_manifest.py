"""Reads and writes the manifest that names a stored DataFrame's current file."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple, Optional

from deepnote_toolkit import __version__

MANIFEST_NAME = "manifest.json"
MANIFEST_VERSION = 1
MANIFEST_MAX_BYTES = 4096

_FRAME_NAME = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.arrow",
    re.IGNORECASE,
)


class Manifest(NamedTuple):
    """The frames a manifest names; `previous` is the one `file` replaced."""

    file: str
    previous: Optional[str]


def is_frame_name(name: str) -> bool:
    """Return whether `name` is `<uuid>.arrow`, the only form of frame file name."""
    return _FRAME_NAME.fullmatch(name) is not None


def read_manifest(directory: Path) -> Optional[Manifest]:
    """Return the files the manifest names, or None if there is no usable one.

    Any code in the project can write the manifest, so a manifest that is too big,
    is not JSON or names anything but `<uuid>.arrow` counts as absent: the writer
    then overwrites it and never deletes a path it supplied.
    """
    try:
        with (directory / MANIFEST_NAME).open("rb") as handle:
            raw = handle.read(MANIFEST_MAX_BYTES + 1)
    except FileNotFoundError:
        return None
    if len(raw) > MANIFEST_MAX_BYTES:
        return None
    try:
        data = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    if not isinstance(data, dict):
        return None

    file, previous = data.get("file"), data.get("previous")
    if not isinstance(file, str) or not is_frame_name(file):
        return None
    if previous is not None and not (
        isinstance(previous, str) and is_frame_name(previous)
    ):
        return None
    return Manifest(file=file, previous=previous)


def write_manifest(
    directory: Path,
    new_file: str,
    previous: Optional[str],
    rows: int,
    block_id: Optional[str] = None,
) -> None:
    """Point the manifest at `new_file`, recording the block that wrote it, if any."""
    manifest: dict[str, Any] = {"version": MANIFEST_VERSION, "file": new_file}
    if previous is not None:
        manifest["previous"] = previous
    manifest["written_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest["rows"] = rows
    manifest["toolkit_version"] = __version__
    if block_id is not None:
        manifest["block_id"] = block_id
    (directory / MANIFEST_NAME).write_text(json.dumps(manifest), encoding="utf-8")
