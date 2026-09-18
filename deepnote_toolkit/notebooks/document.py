"""Read `.deepnote` source and snapshot files."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import yaml

from .models import InputBlock, NotebookOutput, optional_string
from .outputs import OutputCollection
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
        """Read a `.deepnote` file from disk. Raises `ValueError` when it cannot be parsed."""

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
        """Read `.deepnote` YAML from a string. Raises `ValueError` when it cannot be parsed."""

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
        for block in blocks:
            if not isinstance(block, Mapping):
                continue
            if input_block := InputBlock.from_block(block):
                inputs.append(input_block)
            block_outputs = block.get("outputs")
            if not isinstance(block_outputs, list):
                continue
            block_id = str(block.get("id", ""))
            block_type = optional_string(block.get("type"))
            outputs.extend(
                NotebookOutput(block_id=block_id, block_type=block_type, raw=output)
                for output in block_outputs
                if isinstance(output, Mapping)
            )
    return tuple(inputs), tuple(outputs)
