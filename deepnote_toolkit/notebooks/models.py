"""Typed models for Deepnote input blocks, outputs and runner metadata."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from .api_types import InputBlockType

DATAFRAME_MIME = "application/vnd.deepnote.dataframe.v3+json"
INDEX_COLUMN = "_deepnote_index_column"


def join_text(value: Any) -> str:
    """Normalize nbformat's string-or-list text values to one string."""

    if isinstance(value, list):
        return "".join(str(part) for part in value)
    return "" if value is None else str(value)


@dataclass(frozen=True)
class InputBlock:
    """The metadata a UI needs to render one Deepnote input block."""

    variable_name: str
    type: InputBlockType
    value: Any
    label: str | None = None
    options: tuple[str, ...] = ()
    multiple: bool = False
    min: float | int | None = None
    max: float | int | None = None
    step: float | int | None = None
    options_from_variable: bool = False


@dataclass(frozen=True)
class DeepnoteDataframe:
    """A structured Deepnote dataframe output, independent of pandas."""

    columns: tuple[Mapping[str, Any], ...]
    rows: tuple[Mapping[str, Any], ...]
    raw: Mapping[str, Any]

    @classmethod
    def from_value(cls, value: Any) -> DeepnoteDataframe | None:
        """Read a dataframe output payload. Returns None when the value is not one."""

        if not isinstance(value, Mapping):
            return None
        columns = value.get("columns")
        rows = value.get("rows")
        if not isinstance(columns, list) or not isinstance(rows, list):
            return None
        if not all(isinstance(column, Mapping) for column in columns):
            return None
        if not all(isinstance(row, Mapping) for row in rows):
            return None
        return cls(columns=tuple(columns), rows=tuple(rows), raw=value)

    @property
    def row_count(self) -> int:
        """Rows in the full dataframe. `rows` holds only the first page of them."""

        count = self.raw.get("row_count")
        return count if isinstance(count, int) else len(self.rows)

    @property
    def is_truncated(self) -> bool:
        """Whether the full dataframe has more rows than `rows` holds."""

        return self.row_count > len(self.rows)

    @property
    def data_columns(self) -> tuple[str, ...]:
        """Column names without Deepnote's index column."""

        return tuple(
            str(column.get("name"))
            for column in self.columns
            if column.get("name") not in (None, INDEX_COLUMN)
        )

    def records(self, *, include_index: bool = True) -> list[dict[str, Any]]:
        """Return rows as plain dicts, optionally without the index column."""

        if include_index:
            return [dict(row) for row in self.rows]
        return [
            {key: value for key, value in row.items() if key != INDEX_COLUMN}
            for row in self.rows
        ]


@dataclass(frozen=True)
class NotebookOutput:
    """One nbformat-compatible output emitted by a Deepnote block."""

    block_id: str
    block_type: str | None
    raw: Mapping[str, Any]

    @property
    def output_type(self) -> str:
        """The nbformat output type, such as `stream` or `execute_result`."""

        return str(self.raw.get("output_type", ""))

    @property
    def data(self) -> Mapping[str, Any]:
        """The output's MIME bundle, empty for outputs that have none."""

        value = self.raw.get("data")
        return value if isinstance(value, Mapping) else {}

    def text(self, mime: str = "text/plain") -> str:
        """The output's text for a MIME type. Stream outputs count as `text/plain`."""

        if self.output_type == "stream" and mime == "text/plain":
            return join_text(self.raw.get("text"))
        return join_text(self.data.get(mime))

    def image_bytes(self, mime: str = "image/png") -> bytes | None:
        """The decoded image for a MIME type, or None when absent or not base64."""

        value = self.data.get(mime)
        if value is None:
            return None
        encoded = "".join(join_text(value).split())
        try:
            return base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error):
            return None

    @property
    def dataframe(self) -> DeepnoteDataframe | None:
        """The output as a Deepnote dataframe, or None when it is not one."""

        return DeepnoteDataframe.from_value(self.data.get(DATAFRAME_MIME))


@dataclass(frozen=True)
class RunnerInfo:
    """The target and input contract exposed by a Deepnote runner."""

    notebook: str
    inputs: tuple[InputBlock, ...]
    run_target: str

    def accepts_inputs(self, inputs: Iterable[InputBlock]) -> bool:
        """Return whether values made for `inputs` fit this runner's notebook.

        Names, block types, single or multiple selection, slider bounds and select
        options must match. Options filled from a variable change between runs, so
        they are not compared.
        """

        expected = tuple(inputs)
        dynamic = frozenset(
            input_block.variable_name
            for input_block in expected
            if input_block.options_from_variable
        )
        return _input_contract(expected, dynamic) == _input_contract(
            self.inputs, dynamic
        )


def _input_contract(
    inputs: Iterable[InputBlock], dynamic_options: frozenset[str]
) -> frozenset[tuple[Any, ...]]:
    return frozenset(
        (
            input_block.variable_name,
            input_block.type,
            *_value_constraints(input_block, dynamic_options),
        )
        for input_block in inputs
    )


def _value_constraints(
    input_block: InputBlock, dynamic_options: frozenset[str]
) -> tuple[Any, ...]:
    if input_block.type == "input-slider":
        return (
            input_block.min if input_block.min is not None else 0,
            input_block.max if input_block.max is not None else 100,
        )
    if input_block.type == "input-select":
        is_dynamic = input_block.variable_name in dynamic_options
        return (
            input_block.multiple,
            None if is_dynamic else frozenset(input_block.options),
        )
    return ()
