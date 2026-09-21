from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import responses

from deepnote_toolkit.notebooks import (
    DeepnoteDataframe,
    DeepnoteDocument,
    DeepnoteLocalRunner,
    InputBlock,
    RunResult,
)
from deepnote_toolkit.notebooks.models import DATAFRAME_MIME, join_text
from tests.unit.helpers.notebook_api import session


def run_locally(payload: Mapping[str, Any]) -> RunResult:
    with responses.RequestsMock() as http:
        http.post("http://127.0.0.1:8787/api/run", json=payload)
        return DeepnoteLocalRunner(session=session()).run({})


def local_info(payload):
    with responses.RequestsMock() as http:
        http.get("http://127.0.0.1:8787/api/info", json=payload)
        return DeepnoteLocalRunner(session=session()).info()


SNAPSHOT_YAML = """
project:
  name: Sales performance
  notebooks:
    - blocks:
        - id: region-input
          type: input-select
          metadata:
            deepnote_variable_name: region
            deepnote_input_label: Region
            deepnote_variable_value: Europe
            deepnote_variable_options: [All, Europe]
        - id: table
          type: code
          outputs:
            - output_type: execute_result
              data:
                application/vnd.deepnote.dataframe.v3+json:
                  columns:
                    - name: _deepnote_index_column
                    - name: Revenue
                  rows:
                    - _deepnote_index_column: Europe
                      Revenue: 42
        - id: agent
          type: agent
          outputs:
            - output_type: display_data
              data:
                text/markdown: "**Done**"
"""


def test_loads_inputs_and_structured_outputs(tmp_path: Path) -> None:
    path = tmp_path / "sales.snapshot.deepnote"
    path.write_text(SNAPSHOT_YAML, encoding="utf-8")

    snapshot = DeepnoteDocument.load(path)

    assert snapshot.project_name == "Sales performance"
    assert snapshot.inputs == (
        InputBlock(
            "region",
            "input-select",
            "Europe",
            label="Region",
            options=("All", "Europe"),
        ),
    )
    dataframe = snapshot.first_dataframe()
    assert dataframe is not None
    assert dataframe.data_columns == ("Revenue",)
    assert dataframe.records(include_index=False) == [{"Revenue": 42}]
    assert snapshot.agent_text() == "**Done**"


def test_dataframe_ignores_columns_without_names() -> None:
    dataframe = DeepnoteDocument.parse("""
project:
  notebooks:
    - blocks:
        - id: table
          type: code
          outputs:
            - output_type: execute_result
              data:
                application/vnd.deepnote.dataframe.v3+json:
                  columns: [{}, {name: value}]
                  rows: [{value: 42}]
""").first_dataframe()

    assert dataframe is not None
    assert dataframe.data_columns == ("value",)


def test_reads_input_metadata_from_file_and_api_shapes() -> None:
    document = DeepnoteDocument(
        {
            "project": {
                "notebooks": [
                    {
                        "blocks": [
                            {
                                "type": "input-slider",
                                "metadata": {
                                    "deepnote_variable_name": "limit",
                                    "deepnote_input_label": "Row limit",
                                    "deepnote_variable_value": "20",
                                    "deepnote_slider_min_value": 10,
                                    "deepnote_slider_max_value": 100,
                                    "deepnote_slider_step": 10,
                                },
                            }
                        ]
                    }
                ]
            }
        }
    )
    info = local_info(
        {
            "inputs": [
                {
                    "variableName": "countries",
                    "type": "input-select",
                    "label": "Countries",
                    "value": ["Panama"],
                    "options": ["Panama", "Colombia"],
                    "multiple": True,
                }
            ]
        }
    )
    (file_input,) = document.inputs
    (api_input,) = info.inputs

    assert file_input == InputBlock(
        variable_name="limit",
        type="input-slider",
        label="Row limit",
        value="20",
        min=10,
        max=100,
        step=10,
    )
    assert api_input.options == ("Panama", "Colombia")
    assert api_input.multiple is True


def test_run_result_prefers_snapshot_outputs_and_preserves_cloud_fields() -> None:
    result = run_locally(
        {
            "target": "cloud",
            "success": True,
            "runId": "run-1",
            "status": "success",
            "viewUrl": "https://deepnote.com/project/example",
            "snapshotYaml": SNAPSHOT_YAML,
            "outputs": [],
        }
    )

    assert result.success is True
    assert result.target == "cloud"
    assert result.run_id == "run-1"
    assert result.agent_text() == "**Done**"


def test_run_result_falls_back_to_inline_outputs_without_snapshot() -> None:
    result = run_locally(
        {
            "target": "local",
            "success": True,
            "outputs": [
                {
                    "blockId": "code-1",
                    "outputs": [
                        {
                            "output_type": "execute_result",
                            "data": {
                                DATAFRAME_MIME: {
                                    "columns": [{"name": "value"}],
                                    "rows": [{"value": 42}],
                                }
                            },
                        }
                    ],
                }
            ],
        }
    )

    dataframe = result.first_dataframe()
    assert dataframe is not None
    assert dataframe.records() == [{"value": 42}]


def test_run_result_falls_back_to_inline_outputs_for_malformed_snapshot() -> None:
    result = run_locally(
        {
            "target": "cloud",
            "success": True,
            "snapshotYaml": "not: a deepnote snapshot",
            "outputs": [
                {
                    "blockId": "code-1",
                    "outputs": [
                        {
                            "output_type": "stream",
                            "text": "fallback output",
                        }
                    ],
                }
            ],
        }
    )

    assert result.snapshot is None
    assert result.text() == "fallback output"


@pytest.mark.parametrize(
    ("value", "expected"),
    [(["hello", " ", "world"], "hello world"), ("hello", "hello"), (None, "")],
)
def test_join_text(value: object, expected: str) -> None:
    assert join_text(value) == expected


@pytest.mark.parametrize("content", ["hello: world", "[]", ""])
def test_rejects_non_deepnote_yaml(content: str) -> None:
    with pytest.raises(ValueError):
        DeepnoteDocument.parse(content)


MULTI_NOTEBOOK_YAML = """
project:
  name: Sales
  notebooks:
    - id: notebook-a
      blocks:
        - type: input-text
          metadata: {deepnote_variable_name: region, deepnote_variable_value: EU}
    - id: notebook-b
      blocks:
        - type: input-text
          metadata: {deepnote_variable_name: region, deepnote_variable_value: US}
"""


def test_notebook_id_scopes_inputs_to_one_notebook() -> None:
    everything = DeepnoteDocument.parse(MULTI_NOTEBOOK_YAML)
    scoped = DeepnoteDocument.parse(MULTI_NOTEBOOK_YAML, notebook_id="notebook-b")

    assert [input_block.value for input_block in everything.inputs] == ["EU", "US"]
    assert scoped.inputs == (InputBlock("region", "input-text", "US"),)


def test_unknown_notebook_id_is_rejected() -> None:
    with pytest.raises(ValueError, match="notebook-c is not in this document"):
        DeepnoteDocument.parse(MULTI_NOTEBOOK_YAML, notebook_id="notebook-c")


def test_skips_input_blocks_of_an_unknown_type() -> None:
    document = DeepnoteDocument.parse("""
project:
  notebooks:
    - blocks:
        - type: input-unknown
          metadata: {deepnote_variable_name: mystery}
        - type: input-text
          metadata: {deepnote_variable_name: region}
""")

    assert document.inputs == (InputBlock("region", "input-text", None),)


WRITER_STYLE_YAML = """
project:
  name: Survey
  notebooks:
    - id: notebook-a
      blocks:
        - type: input-select
          metadata:
            deepnote_variable_name: answer
            deepnote_variable_value: No
            deepnote_variable_options:
              - Yes
              - No
        - type: input-date
          metadata:
            deepnote_variable_name: as_of
            deepnote_variable_value: 2026-08-17T00:00:00.000Z
        - type: input-text
          metadata:
            deepnote_variable_name: time
            deepnote_variable_value: 12:30
"""


def test_plain_scalars_the_deepnote_writer_leaves_unquoted_stay_strings() -> None:
    document = DeepnoteDocument.parse(WRITER_STYLE_YAML)

    assert document.inputs == (
        InputBlock("answer", "input-select", "No", options=("Yes", "No")),
        InputBlock("as_of", "input-date", "2026-08-17T00:00:00.000Z"),
        InputBlock("time", "input-text", "12:30"),
    )


def test_dataframe_reports_rows_beyond_the_first_page() -> None:
    dataframe = DeepnoteDataframe.from_value(
        {"columns": [{"name": "a"}], "rows": [{"a": 1}], "row_count": 250}
    )
    whole = DeepnoteDataframe.from_value(
        {"columns": [{"name": "a"}], "rows": [{"a": 1}]}
    )

    assert dataframe is not None and whole is not None
    assert (dataframe.row_count, dataframe.is_truncated) == (250, True)
    assert (whole.row_count, whole.is_truncated) == (1, False)


def test_images_decode_wrapped_base64_and_skip_invalid_data() -> None:
    result = run_locally(
        {
            "outputs": [
                {
                    "blockId": "code-1",
                    "outputs": [
                        {
                            "output_type": "display_data",
                            "data": {"image/png": "aGVs\nbG8="},
                        },
                        {
                            "output_type": "display_data",
                            "data": {"image/png": "not base64!"},
                        },
                        {"output_type": "display_data", "data": {"image/jpeg": "aGk="}},
                    ],
                }
            ]
        }
    )

    assert result.images() == [b"hello"]
    assert result.images("image/jpeg") == [b"hi"]
