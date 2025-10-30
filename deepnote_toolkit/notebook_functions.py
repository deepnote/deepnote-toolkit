import json
import time
from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict, Union

import dill
import pandas as pd
import requests
from IPython import get_ipython
from IPython.display import JSON, display

import deepnote_toolkit.ocelots as oc
from deepnote_toolkit.logging import LoggerManager

from .dataframe_utils import browse_dataframe
from .get_webapp_url import get_absolute_notebook_functions_api_url
from .ipython_utils import output_display_data
from .sql.query_preview import DeepnoteQueryPreview


class MissingInputVariableException(Exception):
    pass


class FunctionRunFailedException(Exception):
    pass


class FunctionRunCancelFailedException(Exception):
    pass


class FunctionNotebookNotModuleException(Exception):
    pass


class FunctionCyclicDependencyException(Exception):
    pass


class FunctionNotAvailableException(Exception):
    pass


class FunctionExportFailedException(Exception):
    pass


# This is a special output type used for passing metadata about notebook function run.
NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE = (
    "application/vnd.deepnote.notebook-function-run-metadata+json"
)

# This is a special output type used for passing metadata about notebook function block outputs based on imported data.
# The info in this output is related to the next output in row. It includes info about the function export name and import data type.
NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE = (
    "application/vnd.deepnote.notebook-function-import-metadata+json"
)


class ValueInput(TypedDict):
    type: Literal["value"]
    value: Any


class VariableInput(TypedDict):
    type: Literal["variable"]
    variable_name: str


InputDefinition = Union[ValueInput, VariableInput]
InputsDict = Dict[str, InputDefinition]
SerializationFormat = Literal["json", "dill"]


class RunSubmissionData(TypedDict):
    notebook_function_run_id: str
    notebook_id: str
    notebook_name: str


class ErrorInfo(TypedDict):
    output: Dict[str, str]
    block_id: Optional[str]
    block_export_name: Optional[str]


class ExportInfo(TypedDict):
    download_url: str
    format: SerializationFormat
    data_type: str
    export_name: str
    table_state: Optional[Dict[str, Any]]


class RunResultData(TypedDict):
    errors: Optional[List[ErrorInfo]]
    exports: Optional[List[ExportInfo]]


class ExportMapping(TypedDict):
    variable_name: Optional[str]
    enabled: bool


class NotebookFunctionResult(TypedDict):
    cursors: Dict[str, Any]


logger = LoggerManager().get_logger()


def serialize_export(
    data: Any, format: SerializationFormat
) -> Tuple[Any, Optional[str]]:
    export_data = None
    export_data_content_type = None

    if data is not None:
        if format == "json":
            try:
                export_data = data.to_json()
            except Exception:
                export_data = json.dumps(data, default=str)
            export_data_content_type = "application/json"
        elif format == "dill":
            try:
                export_data = dill.dumps(data)
                export_data_content_type = "application/octet-stream"
            except Exception:
                logger.exception("Couldn't serialize export data with dill")

    return export_data, export_data_content_type


def parse_export_data(data: Any, format: SerializationFormat, data_type: str) -> Any:
    if format == "json":
        result = json.loads(data)
        if data_type == "DataFrame":
            return pd.DataFrame(result).reset_index(drop=True)
        if data_type == "DeepnoteQueryPreview":
            return DeepnoteQueryPreview(result).reset_index(drop=True)
        return result

    if format == "dill":
        return dill.loads(data)

    return str(data)


def _sanitize_function_input_value(value: Any) -> Union[str, List[str]]:
    if oc.utils.is_pandas_dataframe(value):
        return [str(cell) for cell in value.to_numpy().flatten()]
    if isinstance(value, list):
        return [str(item) for item in value]
    return str(value)


def _create_notebook_function_api_headers(
    notebook_function_api_token: str,
) -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + notebook_function_api_token,
    }


def _create_notebook_function_submit_payload(
    scope: Dict[str, Any],
    parent_notebook_function_run_id: str,
    inputs: InputsDict,
) -> Dict[str, Any]:
    try:
        return {
            "parent_notebook_function_run_id": parent_notebook_function_run_id,
            "inputs": {
                input_name: (
                    _sanitize_function_input_value(input_definition["value"])
                    if input_definition["type"] == "value"
                    else (
                        _sanitize_function_input_value(
                            scope[input_definition["variable_name"]]
                        )
                        if input_definition["type"] == "variable"
                        else None
                    )
                )
                for input_name, input_definition in inputs.items()
            },
        }
    except (TypeError, ValueError, KeyError) as e:
        raise MissingInputVariableException(f"Missing input variable: {e}")


def _submit_notebook_function(
    scope: Dict[str, Any],
    notebook_function_api_token: str,
    function_notebook_id: str,
    inputs: InputsDict,
    parent_notebook_function_run_id: str,
    debug: bool,
) -> Tuple[RunSubmissionData, Dict[str, Any]]:

    body = _create_notebook_function_submit_payload(
        scope,
        parent_notebook_function_run_id,
        inputs,
    )
    if debug:
        print(body)

    headers = _create_notebook_function_api_headers(notebook_function_api_token)

    url = get_absolute_notebook_functions_api_url(function_notebook_id)
    response = requests.post(url, headers=headers, json=body)
    run_submission_data = response.json()
    if debug:
        print(run_submission_data)

    if (
        response.status_code == 404
        and run_submission_data.get("error") == "NotebookNotAvailable"
    ):
        raise FunctionNotAvailableException(
            "The notebook is not available (make sure you have the required permissions)"
        )

    if (
        response.status_code == 405
        and run_submission_data.get("error") == "NotebookNotModule"
    ):
        raise FunctionNotebookNotModuleException(
            "The notebook is not published as a module"
        )

    if response.status_code == 400:
        if run_submission_data.get("error") == "NestedFunctionNotAvailable":
            raise FunctionNotAvailableException(
                "There is a nested function you are not allowed to run."
                if run_submission_data.get("has_view_only_access")
                else "Nested function not available (make sure you have the required permissions)"
            )

        if run_submission_data.get("error") == "NestedFunctionNotebookNotModule":
            raise FunctionNotebookNotModuleException(
                "There is a nested notebook that is not published as a module."
            )

        if run_submission_data.get("error") == "FunctionCyclicDependency":
            raise FunctionCyclicDependencyException("Cyclic dependency")

        raise FunctionRunFailedException(
            run_submission_data.get("error") or "Failed to run function"
        )

    if response.status_code != 202:
        raise FunctionRunFailedException(
            f"Failed to run function (status code: {response.status_code})"
        )

    return run_submission_data, body["inputs"]


def _wait_for_notebook_function_run_finish(
    notebook_function_api_token: str,
    function_notebook_id: str,
    run_id: str,
    debug: bool,
) -> RunResultData:

    # Extract the polling URL
    polling_url = get_absolute_notebook_functions_api_url(
        function_notebook_id + "/" + run_id
    )
    headers = _create_notebook_function_api_headers(notebook_function_api_token)

    # Start polling until status is 'done'
    while True:
        poll_response = requests.get(polling_url, headers=headers)
        poll_data = poll_response.json()
        if debug:
            print(poll_data)

        if poll_response.status_code == 400 or poll_response.status_code == 401:
            error_code = poll_data.get("error", "")
            raise FunctionRunFailedException(
                f"Failed to observe function run progress ({poll_response.status_code}"
                + (f": {error_code}" if error_code else "")
                + ")"
            )

        if poll_response.status_code == 200 and poll_data.get("status") == "done":
            return poll_data

        time.sleep(1)  # Wait for 1 second before polling again


def _output_notebook_function_run_metadata(
    run_submission_data: RunSubmissionData,
    run_result_data: RunResultData,
    submitted_inputs: Dict[str, Any],
    export_mappings: Dict[str, ExportMapping],
) -> None:
    output_display_data(
        {
            NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                "notebook_function_run_id": run_submission_data.get(
                    "notebook_function_run_id"
                ),
                "executed_notebook_id": run_submission_data.get("notebook_id"),
                "executed_notebook_name": run_submission_data.get("notebook_name"),
                "executed_notebook_inputs": {
                    key: {"value": value} for key, value in submitted_inputs.items()
                },
                "executed_notebook_imports": export_mappings,
                "executed_notebook_errors": [
                    {
                        "error_output": error_info["output"],
                        "error_block_id": error_info.get("block_id"),
                        "error_block_export_name": error_info.get("block_export_name"),
                    }
                    for error_info in run_result_data.get("errors", [])
                    if "errors" in run_result_data
                ],
            }
        }
    )


def _apply_notebook_function_run_import(
    scope: Dict[str, Any],
    export_info: ExportInfo,
    variable_name: str,
    export_table_state: Optional[Dict[str, Any]],
) -> Tuple[Any, bool]:

    success = True
    try:
        export_content = requests.get(export_info["download_url"]).content
        export_data = parse_export_data(
            export_content, export_info["format"], export_info["data_type"]
        )
    except Exception:
        logger.exception("Parsing notebook function export data failed with exception.")
        export_data = None
        success = False

    table_state = export_table_state or export_info.get("table_state", None)
    if oc.DataFrame.is_supported(export_data) and table_state is not None:
        browse_dataframe(export_data, json.dumps(table_state))

    output_display_data(
        {
            NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                "export_name": export_info["export_name"],
                "export_data_type": export_info["data_type"],
                "export_table_state": table_state,
                "variable_name": variable_name,
            }
        }
    )

    scope[variable_name] = export_data

    return export_data, success


def _apply_notebook_function_run_imports(
    scope: Dict[str, Any],
    run_result_data: RunResultData,
    export_mappings: Dict[str, ExportMapping],
    export_table_states_json: str,
    debug: bool,
) -> Dict[str, Any]:
    export_variable_mappings = {
        export_name: mapping["variable_name"]
        for export_name, mapping in export_mappings.items()
        if mapping["variable_name"] is not None and mapping["enabled"] is True
    }
    if debug:
        print(export_variable_mappings)

    for variable_name in export_variable_mappings.values():
        scope[variable_name] = None

    cursors = {}

    if "exports" in run_result_data:
        all_successful = True

        export_table_states = json.loads(export_table_states_json)

        for export_info in run_result_data["exports"]:
            variable_name = export_variable_mappings.get(export_info["export_name"])
            if variable_name is None:
                continue

            export_data, success = _apply_notebook_function_run_import(
                scope,
                export_info,
                variable_name,
                export_table_states.get(export_info["export_name"], None),
            )

            cursors[variable_name] = export_data
            if success:
                display(export_data)
            else:
                all_successful = False

        if not all_successful:
            raise FunctionExportFailedException("Failed to load some exports")

    return cursors


def _apply_notebook_function_run_errors(run_result_data: RunResultData) -> None:
    if "errors" in run_result_data and run_result_data["errors"]:
        for error_info in run_result_data["errors"]:
            err_name = error_info["output"].get("ename")

            if err_name == "MissingInputVariableException":
                raise MissingInputVariableException(error_info["output"].get("evalue"))

            if err_name == "FunctionNotAvailableException":
                raise FunctionNotAvailableException(error_info["output"].get("evalue"))

            if err_name == "FunctionNotebookNotModuleException":
                raise FunctionNotebookNotModuleException(
                    error_info["output"].get("evalue")
                )

            if err_name == "FunctionCyclicDependencyException":
                raise FunctionCyclicDependencyException(
                    error_info["output"].get("evalue")
                )

            if err_name == "FunctionRunFailedException":
                raise FunctionRunFailedException(error_info["output"].get("evalue"))

            if err_name == "FunctionExportFailedException":
                raise FunctionExportFailedException(error_info["output"].get("evalue"))

        raise FunctionRunFailedException("Notebook function run failed")


def run_notebook_function(
    scope: Dict[str, Any],
    notebook_function_api_token: str,
    function_notebook_id: str,
    inputs: InputsDict,
    export_mappings: Dict[str, ExportMapping],
    export_table_states_json: str = "{}",
    parent_notebook_function_run_id: Optional[str] = None,
    debug: bool = False,
) -> NotebookFunctionResult:
    run_submission_data, submitted_inputs = _submit_notebook_function(
        scope,
        notebook_function_api_token,
        function_notebook_id,
        inputs,
        parent_notebook_function_run_id,
        debug,
    )

    run_id = run_submission_data.get("notebook_function_run_id")
    if run_id is None:
        raise Exception("Failed to start the notebook function")

    run_result_data = _wait_for_notebook_function_run_finish(
        notebook_function_api_token,
        function_notebook_id,
        run_id,
        debug,
    )

    _output_notebook_function_run_metadata(
        run_submission_data, run_result_data, submitted_inputs, export_mappings
    )

    cursors = _apply_notebook_function_run_imports(
        scope,
        run_result_data,
        export_mappings,
        export_table_states_json,
        debug,
    )

    _apply_notebook_function_run_errors(run_result_data)

    return {
        "cursors": cursors,
    }


def cancel_notebook_function(
    notebook_function_api_token: str, function_notebook_id: str
) -> None:
    headers = _create_notebook_function_api_headers(notebook_function_api_token)
    url = get_absolute_notebook_functions_api_url(function_notebook_id)

    response = requests.delete(url, headers=headers)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        raise FunctionRunCancelFailedException("Failed to cancel the function run")

    return


def export_last_block_result(
    Out: Dict[int, Any],
    upload_url: str,
    format: SerializationFormat,
) -> None:

    ipython = get_ipython()
    if ipython is None:
        raise RuntimeError("This function must be run inside an IPython environment.")

    execution_count = ipython.execution_count - 1

    result = Out[execution_count] if execution_count in Out else None

    result_data_type = type(result).__name__ if result is not None else None
    export_data, export_data_content_type = serialize_export(result, format)

    if export_data is None:
        exported_format = None
    else:
        response = requests.put(
            upload_url,
            headers={"Content-Type": export_data_content_type},
            data=export_data,
        )
        response.raise_for_status()
        exported_format = format

    JSON(
        {
            "exported_data_type": result_data_type,
            "exported_data_format": exported_format,
        }
    )

    # NOTE: It is important that this function does not return anything. We need to avoid an execute_result output.
    return
