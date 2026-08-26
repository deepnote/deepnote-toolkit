from pathlib import Path

import pytest

from deepnote_toolkit.streamlit import (
    DATAFRAME_MIME,
    DeepnoteDocument,
    InputBlock,
    RunResult,
    join_text,
)

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
    dataframe = DeepnoteDocument.parse(
        """
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
"""
    ).first_dataframe()

    assert dataframe is not None
    assert dataframe.data_columns == ("value",)


def test_reads_input_metadata_from_file_and_api_shapes() -> None:
    file_input = InputBlock.from_block(
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
    )
    api_input = InputBlock.from_api(
        {
            "variableName": "countries",
            "type": "input-select",
            "label": "Countries",
            "value": ["Panama"],
            "options": ["Panama", "Colombia"],
            "multiple": True,
        }
    )

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
    result = RunResult(
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
    result = RunResult(
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
    result = RunResult(
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
