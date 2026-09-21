"""Read `.deepnote` source and snapshot files."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import yaml

from .api_types import INPUT_BLOCK_TYPES, InputBlockType
from .models import InputBlock, NotebookOutput
from .outputs import OutputCollection
from .wire import decode_block_outputs, optional_number, optional_string, string_tuple
from .yaml_loader import load_yaml


class DeepnoteDocument(OutputCollection):
    """A parsed source or snapshot `.deepnote` file.

    `notebook_id` limits the inputs and outputs to one notebook of the project.
    """

    def __init__(self, raw: Mapping[str, Any], *, notebook_id: str | None = None):
        project = raw.get("project")
        if not isinstance(project, Mapping) or not isinstance(
            project.get("notebooks"), list
        ):
            raise ValueError("Expected a .deepnote document with project.notebooks")
        notebooks = project["notebooks"]
        if notebook_id is not None:
            notebooks = [
                notebook
                for notebook in notebooks
                if isinstance(notebook, Mapping) and notebook.get("id") == notebook_id
            ]
            if not notebooks:
                raise ValueError(f"Notebook {notebook_id} is not in this document")
        self.raw = raw
        self.project_name = str(project.get("name", "Untitled project"))
        self.inputs, self.outputs = _read_blocks(notebooks)

    @classmethod
    def load(
        cls, path: str | Path, *, notebook_id: str | None = None
    ) -> DeepnoteDocument:
        """Read a `.deepnote` file from disk. Raises `ValueError` on invalid content."""

        source = Path(path)
        try:
            raw = load_yaml(source.read_text(encoding="utf-8"))
        except yaml.YAMLError as error:
            raise ValueError(f"Could not parse {source}: {error}") from error
        if not isinstance(raw, Mapping):
            raise ValueError(f"Expected {source} to contain a YAML object")
        return cls(raw, notebook_id=notebook_id)

    @classmethod
    def parse(cls, content: str, *, notebook_id: str | None = None) -> DeepnoteDocument:
        """Read `.deepnote` YAML from a string. Raises `ValueError` when invalid."""

        try:
            raw = load_yaml(content)
        except yaml.YAMLError as error:
            raise ValueError(f"Could not parse .deepnote YAML: {error}") from error
        if not isinstance(raw, Mapping):
            raise ValueError("Expected .deepnote YAML to contain an object")
        return cls(raw, notebook_id=notebook_id)


def _read_blocks(
    notebooks: Sequence[Any],
) -> tuple[tuple[InputBlock, ...], tuple[NotebookOutput, ...]]:
    inputs: list[InputBlock] = []
    outputs: list[NotebookOutput] = []
    for notebook in notebooks:
        if not isinstance(notebook, Mapping):
            continue
        blocks = notebook.get("blocks")
        if not isinstance(blocks, list):
            continue
        inputs.extend(
            input_block
            for block in blocks
            if isinstance(block, Mapping) and (input_block := _read_input_block(block))
        )
        outputs.extend(decode_block_outputs(blocks, id_key="id"))
    return tuple(inputs), tuple(outputs)


def _read_input_block(block: Mapping[str, Any]) -> InputBlock | None:
    block_type = str(block.get("type", ""))
    metadata = block.get("metadata")
    if block_type not in INPUT_BLOCK_TYPES or not isinstance(metadata, Mapping):
        return None
    variable_name = metadata.get("deepnote_variable_name")
    if not isinstance(variable_name, str) or not variable_name:
        return None
    return InputBlock(
        variable_name=variable_name,
        type=cast(InputBlockType, block_type),
        label=optional_string(metadata.get("deepnote_input_label")),
        value=metadata.get("deepnote_variable_value"),
        options=string_tuple(metadata.get("deepnote_variable_options")),
        options_from_variable=(
            metadata.get("deepnote_variable_select_type") == "from-variable"
        ),
        multiple=metadata.get("deepnote_allow_multiple_values") is True,
        min=optional_number(metadata.get("deepnote_slider_min_value")),
        max=optional_number(metadata.get("deepnote_slider_max_value")),
        step=optional_number(metadata.get("deepnote_slider_step")),
    )
