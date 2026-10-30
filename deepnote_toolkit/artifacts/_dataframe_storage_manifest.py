"""Reads and writes the manifest that names a stored DataFrame's current file."""

import contextlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, StrictInt, StrictStr, ValidationError

from deepnote_core.pydantic_compat_helpers import (
    _model_dump_compat,
    model_validate_compat,
)
from deepnote_toolkit import __version__

MANIFEST_NAME = "manifest.json"
MANIFEST_VERSION = 1
MANIFEST_MAX_BYTES = 64 * 1024

_FRAME_NAME_REGEX = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.arrow",
    re.IGNORECASE,
)


def is_frame_name(name: str) -> bool:
    """Return whether `name` is `<uuid>.arrow`, the only form of frame file name."""
    return _FRAME_NAME_REGEX.fullmatch(name) is not None


class Manifest(BaseModel):
    """The manifest file"""

    version: StrictInt
    file: StrictStr
    previous: Optional[StrictStr] = Field(
        default=None, description="The previous frame file, if any"
    )
    written_at: StrictStr
    rows: StrictInt = Field(description="The total number of rows in the DataFrame.")
    batch_rows: list[StrictInt] = Field(
        description="The number of rows in each record batch, in order."
    )
    toolkit_version: StrictStr


def read_manifest(directory: Path) -> Optional[Manifest]:
    """Return the directory's manifest, or None if there is no usable one.

    Any code in the project can write the manifest, so a malformed one counts as
    absent.
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
    try:
        manifest = model_validate_compat(Manifest, data)
    except (ValidationError, TypeError):
        return None

    return manifest


def encode_manifest(
    new_file: str, previous: Optional[str], batch_rows: list[int]
) -> str:
    """Return the manifest naming `new_file`, whose record batches hold `batch_rows`.

    Raises:
        ValueError: The manifest would exceed MANIFEST_MAX_BYTES, so neither the
            toolkit nor the dataframe reader could read it.
    """
    manifest = Manifest(
        version=MANIFEST_VERSION,
        file=new_file,
        previous=previous,
        written_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        rows=sum(batch_rows),
        batch_rows=batch_rows,
        toolkit_version=__version__,
    )
    encoded = json.dumps(_model_dump_compat(manifest))
    if len(encoded.encode("utf-8")) > MANIFEST_MAX_BYTES:
        raise ValueError(
            f"The DataFrame needs {len(batch_rows)} record batches, more than its "
            f"manifest can list in {MANIFEST_MAX_BYTES} bytes"
        )
    return encoded


def write_manifest(directory: Path, encoded: str) -> None:
    """Replace the directory's manifest with `encoded`.

    On s3fs a rename is a copy and then a delete, so a failed rename may already
    have replaced the manifest.
    """
    # delete=False: s3fs uploads at close(), so the file must be closed before the
    # rename, and deleting it only when the context ends needs Python 3.12's
    # delete_on_close=False.
    temporary = tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=directory,
        prefix=f".{MANIFEST_NAME}.",
        suffix=".tmp",
        delete=False,
    )
    try:
        with temporary:
            temporary.write(encoded)
        os.replace(temporary.name, directory / MANIFEST_NAME)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(temporary.name)
        raise
