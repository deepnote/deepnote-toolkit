"""Decode the JSON shapes shared by the API, the sidecar and `.deepnote` files."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from .api_types import INPUT_BLOCK_TYPES, InputBlockType
from .models import InputBlock, NotebookOutput


def optional_string(value: Any) -> str | None:
    """Return the value when it is a string, otherwise None."""

    return value if isinstance(value, str) else None


def optional_number(value: Any) -> float | int | None:
    """Return the value when it is a number other than a boolean, otherwise None."""

    return (
        value
        if isinstance(value, (float, int)) and not isinstance(value, bool)
        else None
    )


def string_tuple(value: Any) -> tuple[str, ...]:
    """Return a list's items as strings, or an empty tuple for any other value."""

    return tuple(str(item) for item in value) if isinstance(value, list) else ()


def decode_inputs(values: Any, *, name_key: str) -> tuple[InputBlock, ...]:
    """Read an API's camelCase inputs, skipping any without a name or a known type."""

    if not isinstance(values, list):
        return ()
    return tuple(
        InputBlock(
            variable_name=value[name_key],
            type=cast(InputBlockType, value["type"]),
            label=optional_string(value.get("label")),
            value=value.get("value"),
            options=string_tuple(value.get("options")),
            multiple=value.get("multiple") is True,
            min=optional_number(value.get("min")),
            max=optional_number(value.get("max")),
            step=optional_number(value.get("step")),
        )
        for value in values
        if isinstance(value, Mapping)
        and isinstance(value.get(name_key), str)
        and isinstance(value.get("type"), str)
        and value["type"] in INPUT_BLOCK_TYPES
    )


def decode_block_outputs(blocks: Any, *, id_key: str) -> tuple[NotebookOutput, ...]:
    """Read the outputs of a list of blocks, in block order."""

    if not isinstance(blocks, list):
        return ()
    outputs: list[NotebookOutput] = []
    for block in blocks:
        if not isinstance(block, Mapping):
            continue
        block_outputs = block.get("outputs")
        if not isinstance(block_outputs, list):
            continue
        block_id = str(block.get(id_key, ""))
        block_type = optional_string(block.get("type"))
        outputs.extend(
            NotebookOutput(block_id=block_id, block_type=block_type, raw=output)
            for output in block_outputs
            if isinstance(output, Mapping)
        )
    return tuple(outputs)
