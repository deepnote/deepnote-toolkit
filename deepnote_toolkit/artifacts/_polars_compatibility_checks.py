"""Refuses polars versions and data types that DataFrame storage can't handle."""

from typing import Any

from packaging.version import Version

from deepnote_toolkit.ocelots.types import PolarsEagerDF

POLARS_MIN_VERSION = Version("1.35.1")
POLARS_ARRAY_MIN_VERSION = Version("1.43.2")


def _contains_polars_dtype(dtype: Any, *targets: Any) -> bool:
    """Return whether `dtype` is one of `targets` or nests one at any depth."""
    import polars

    if any(dtype == target for target in targets):
        return True
    if isinstance(dtype, (polars.List, polars.Array)):
        return _contains_polars_dtype(dtype.inner, *targets)
    if isinstance(dtype, polars.Struct):
        return any(
            _contains_polars_dtype(field.dtype, *targets) for field in dtype.fields
        )
    return False


def refuse_old_polars() -> None:
    """Raise TypeError on polars older than POLARS_MIN_VERSION."""
    import polars

    # Before 1.35.1, to_arrow() copies a column's whole dictionary into every
    # chunk, so many chunks of a large Categorical could exhaust memory.
    if Version(polars.__version__) < POLARS_MIN_VERSION:
        raise TypeError(
            f"Storing polars DataFrames needs polars {POLARS_MIN_VERSION} or "
            f"newer, found {polars.__version__}. Upgrade it (pip install -U "
            "polars) and restart the kernel."
        )


def refuse_object_columns(frame: PolarsEagerDF) -> None:
    """Raise TypeError for Object data."""
    import polars

    # to_arrow() exports an Object value as its 8-byte memory address.
    objects = [
        name
        for name, dtype in frame.schema.items()
        if _contains_polars_dtype(dtype, polars.Object)
    ]
    if objects:
        raise TypeError(f"Polars Object columns cannot be stored, got {objects}")


def refuse_arrays_on_old_polars(frame: PolarsEagerDF) -> None:
    """Raise TypeError for Array data on polars older than POLARS_ARRAY_MIN_VERSION."""
    import polars

    # Before 1.43.2, to_arrow() applies the offset of a sliced Array column with
    # nulls twice: the file holds shifted values or aborts pyarrow on read.
    # Struct had the same bug before 1.43.0, but pyarrow rejects that export,
    # so storing fails instead of writing wrong values.
    if Version(polars.__version__) >= POLARS_ARRAY_MIN_VERSION:
        return
    arrays = [
        name
        for name, dtype in frame.schema.items()
        if _contains_polars_dtype(dtype, polars.Array)
    ]
    if arrays:
        raise TypeError(
            "Storing polars Array columns needs polars "
            f"{POLARS_ARRAY_MIN_VERSION} or newer, found "
            f"{polars.__version__}, got {arrays}. Upgrade it (pip install "
            "-U polars) and restart the kernel."
        )
