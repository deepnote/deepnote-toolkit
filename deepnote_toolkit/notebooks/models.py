"""Typed models for Deepnote input blocks, outputs and runner metadata."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

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
    type: str
    value: Any
    label: str | None = None
    options: tuple[str, ...] = ()
    multiple: bool = False
    min: float | int | None = None
    max: float | int | None = None
    step: float | int | None = None

    @classmethod
    def from_block(cls, block: Mapping[str, Any]) -> InputBlock | None:
        """Read an input block from a `.deepnote` file, or None for another block."""

        block_type = str(block.get("type", ""))
        metadata = block.get("metadata")
        if not block_type.startswith("input-") or not isinstance(metadata, Mapping):
            return None
        variable_name = metadata.get("deepnote_variable_name")
        if not isinstance(variable_name, str) or not variable_name:
            return None
        options = metadata.get("deepnote_variable_options")
        return cls(
            variable_name=variable_name,
            type=block_type,
            label=optional_string(metadata.get("deepnote_input_label")),
            value=metadata.get("deepnote_variable_value"),
            options=(
                tuple(str(option) for option in options)
                if isinstance(options, list)
                else ()
            ),
            multiple=metadata.get("deepnote_allow_multiple_values") is True,
            min=optional_number(metadata.get("deepnote_slider_min_value")),
            max=optional_number(metadata.get("deepnote_slider_max_value")),
            step=optional_number(metadata.get("deepnote_slider_step")),
        )

    @classmethod
    def from_api(cls, value: Mapping[str, Any]) -> InputBlock:
        """Read the camelCase shape returned by `GET /api/info`."""

        options = value.get("options")
        return cls(
            variable_name=str(value["variableName"]),
            type=str(value["type"]),
            label=optional_string(value.get("label")),
            value=value.get("value"),
            options=(
                tuple(str(option) for option in options)
                if isinstance(options, list)
                else ()
            ),
            multiple=value.get("multiple") is True,
            min=optional_number(value.get("min")),
            max=optional_number(value.get("max")),
            step=optional_number(value.get("step")),
        )


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
        """Return rows ready for `st.dataframe`, optionally without the index column."""

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
        """Return whether input variable names and block types match this runner."""

        return _input_contract(inputs) == _input_contract(self.inputs)


def _input_contract(inputs: Iterable[InputBlock]) -> frozenset[tuple[str, str]]:
    return frozenset(
        (input_block.variable_name, input_block.type) for input_block in inputs
    )


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
