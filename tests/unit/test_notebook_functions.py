import json
from unittest import TestCase
from unittest.mock import patch

import dill
import pandas
import responses

from deepnote_toolkit.dataframe_utils import get_dataframe_browsing_spec
from deepnote_toolkit.get_webapp_url import get_absolute_notebook_functions_api_url
from deepnote_toolkit.notebook_functions import (
    NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE,
    NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE,
    FunctionCyclicDependencyException,
    FunctionExportFailedException,
    FunctionNotAvailableException,
    FunctionNotebookNotModuleException,
    FunctionRunCancelFailedException,
    FunctionRunFailedException,
    MissingInputVariableException,
    cancel_notebook_function,
    export_last_block_result,
    parse_export_data,
    run_notebook_function,
)


class TestRunNotebookFunction(TestCase):
    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_return_empty_cursor_list_for_success_with_no_inputs_and_imports(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [],
            },
        )

        block_result = run_notebook_function(
            scope={},
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={},
        )
        self.assertEqual(block_result, {"cursors": {}})

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [],
                }
            }
        )

    @responses.activate
    def test_it_should_call_notebook_functions_api_with_api_token_and_json_content_type(
        self,
    ):
        def request_callback(request):
            self.assertEqual(request.headers["Authorization"], "Bearer secret-token")
            self.assertEqual(request.headers["Content-Type"], "application/json")
            self.assertEqual(request.headers["Accept"], "application/json")
            request_callback.callCount += 1

            if (
                request.method == "POST"
                and request.url
                == get_absolute_notebook_functions_api_url("test-notebook-id")
            ):
                return (
                    202,
                    {},
                    json.dumps(
                        {
                            "notebook_function_run_id": "test-run-id",
                            "notebook_id": "test-notebook-id",
                            "notebook_name": "test-notebook-name",
                        }
                    ),
                )
            if (
                request.method == "GET"
                and request.url
                == get_absolute_notebook_functions_api_url(
                    "test-notebook-id/test-run-id"
                )
            ):
                return (
                    200,
                    {},
                    json.dumps(
                        {
                            "status": "done",
                            "exports": [],
                            "errors": [],
                        }
                    ),
                )

        request_callback.callCount = 0
        responses.add_callback(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            callback=request_callback,
            content_type="application/json",
        )
        responses.add_callback(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            callback=request_callback,
            content_type="application/json",
        )

        run_notebook_function(
            scope={},
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={},
        )

        self.assertEqual(
            request_callback.callCount, 2, "The request_callback was not called 2x"
        )

    @responses.activate
    def test_it_should_submit_child_notebook_function_run(
        self,
    ):
        def request_callback(request):
            self.assertEqual(
                json.loads(request.body)["parent_notebook_function_run_id"],
                "test-parent-run-id",
            )
            request_callback.called += 1
            return (
                202,
                {},
                json.dumps(
                    {
                        "notebook_function_run_id": "test-run-id",
                        "notebook_id": "test-notebook-id",
                        "notebook_name": "test-notebook-name",
                    }
                ),
            )

        request_callback.called = 0
        responses.add_callback(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            callback=request_callback,
            content_type="application/json",
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            content_type="application/json",
            json={
                "status": "done",
                "exports": [],
                "errors": [],
            },
        )

        run_notebook_function(
            scope={},
            notebook_function_api_token="secret-token",
            parent_notebook_function_run_id="test-parent-run-id",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={},
        )

        self.assertTrue(request_callback.called, "The request_callback was not called")

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    def test_it_should_return_cursors_and_set_variables_for_success_with_imports(
        self, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [
                    {
                        "export_name": "link_a",
                        "format": "json",
                        "data_type": "str",
                        "download_url": 'http://example.com/test/a"',
                    },
                    {
                        "export_name": "link_b",
                        "format": "dill",
                        "data_type": "str",
                        "download_url": 'http://example.com/test/b"',
                    },
                ],
                "errors": [],
            },
        )
        responses.add(
            responses.GET, 'http://example.com/test/a"', status=200, json="test-value-a"
        )
        responses.add(
            responses.GET,
            'http://example.com/test/b"',
            status=200,
            body=dill.dumps("test-value-b"),
        )

        scope = {}
        block_result = run_notebook_function(
            scope=scope,
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={
                "link_a": {"variable_name": "var_a", "enabled": True},
                "link_b": {"variable_name": "var_b", "enabled": True},
            },
        )
        self.assertEqual(
            block_result,
            {
                "cursors": {
                    "var_a": "test-value-a",
                    "var_b": "test-value-b",
                }
            },
        )
        self.assertEqual(scope["var_a"], "test-value-a")
        self.assertEqual(scope["var_b"], "test-value-b")

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {
                        "link_a": {"variable_name": "var_a", "enabled": True},
                        "link_b": {"variable_name": "var_b", "enabled": True},
                    },
                    "executed_notebook_errors": [],
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_a",
                    "export_data_type": "str",
                    "export_table_state": None,
                    "variable_name": "var_a",
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_b",
                    "export_data_type": "str",
                    "export_table_state": None,
                    "variable_name": "var_b",
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_pass_and_output_inputs(self, mock_output_display_data):
        def request_callback(request):
            self.assertEqual(
                json.loads(request.body)["inputs"],
                {
                    "string_value_input": "input-value-a",
                    "string_var_input": "input-value-b",
                    "string_array_value_input": ["input-value-c1", "input-value-c2"],
                    "string_array_var_input": ["input-value-d1", "input-value-d2"],
                    "int_value_input": "123",
                    "df_value_input": ["a", "10", "b", "20"],
                    "int_var_input": "456",
                    "df_var_input": ["d", "30", "e", "40"],
                },
            )

            return [
                202,
                {},
                json.dumps(
                    {
                        "notebook_function_run_id": "test-run-id",
                        "notebook_id": "test-notebook-id",
                        "notebook_name": "test-notebook-name",
                    }
                ),
            ]

        responses.add_callback(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            callback=request_callback,
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [],
            },
        )

        run_notebook_function(
            scope={
                "string_var": "input-value-b",
                "string_array_var": ["input-value-d1", "input-value-d2"],
                "int_var": 456,
                "df_var": pandas.DataFrame({"col1": ["d", "e"], "col2": [30, 40]}),
            },
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={
                "string_value_input": {"type": "value", "value": "input-value-a"},
                "string_var_input": {
                    "type": "variable",
                    "variable_name": "string_var",
                },
                "string_array_value_input": {
                    "type": "value",
                    "value": ["input-value-c1", "input-value-c2"],
                },
                "string_array_var_input": {
                    "type": "variable",
                    "variable_name": "string_array_var",
                },
                "int_value_input": {"type": "value", "value": 123},
                "df_value_input": {
                    "type": "value",
                    "value": pandas.DataFrame({"col1": ["a", "b"], "col2": [10, 20]}),
                },
                "int_var_input": {"type": "variable", "variable_name": "int_var"},
                "df_var_input": {"type": "variable", "variable_name": "df_var"},
            },
            export_mappings={},
        )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {
                        "string_value_input": {"value": "input-value-a"},
                        "string_var_input": {"value": "input-value-b"},
                        "string_array_value_input": {
                            "value": ["input-value-c1", "input-value-c2"]
                        },
                        "string_array_var_input": {
                            "value": ["input-value-d1", "input-value-d2"]
                        },
                        "int_value_input": {"value": "123"},
                        "df_value_input": {"value": ["a", "10", "b", "20"]},
                        "int_var_input": {"value": "456"},
                        "df_var_input": {"value": ["d", "30", "e", "40"]},
                    },
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [],
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_throw_MissingInputVariableException_when_input_variable_is_missing(
        self, mock_output_display_data
    ):
        with self.assertRaises(MissingInputVariableException):
            run_notebook_function(
                scope={"another_variable": "abc"},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={
                    "var_input": {
                        "type": "variable",
                        "variable_name": "missing_variable",
                    },
                },
                export_mappings={},
            )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    def test_it_should_return_cursors_and_set_variables_for_success_with_dataframe_imports(
        self, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [
                    {
                        "export_name": "link_a",
                        "format": "dill",
                        "data_type": "DataFrame",
                        "download_url": 'http://example.com/test/a"',
                    },
                    {
                        "export_name": "link_b",
                        "format": "json",
                        "data_type": "DataFrame",
                        "download_url": 'http://example.com/test/b"',
                    },
                ],
                "errors": [],
            },
        )

        df_a = pandas.DataFrame({"a": [1, 2, 3]})
        df_b = pandas.DataFrame({"b": [1, 2, 3]})
        responses.add(
            responses.GET,
            'http://example.com/test/a"',
            status=200,
            body=dill.dumps(df_a),
        )
        responses.add(
            responses.GET,
            'http://example.com/test/b"',
            status=200,
            body=df_b.to_json(),
        )

        scope = {}
        block_result = run_notebook_function(
            scope=scope,
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={
                "link_a": {"variable_name": "var_a", "enabled": True},
                "link_b": {"variable_name": "var_b", "enabled": True},
            },
        )
        pandas.testing.assert_frame_equal(block_result["cursors"]["var_a"], df_a)
        pandas.testing.assert_frame_equal(block_result["cursors"]["var_b"], df_b)
        pandas.testing.assert_frame_equal(scope["var_a"], df_a)
        pandas.testing.assert_frame_equal(scope["var_b"], df_b)

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {
                        "link_a": {"variable_name": "var_a", "enabled": True},
                        "link_b": {"variable_name": "var_b", "enabled": True},
                    },
                    "executed_notebook_errors": [],
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_a",
                    "export_data_type": "DataFrame",
                    "export_table_state": None,
                    "variable_name": "var_a",
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_b",
                    "export_data_type": "DataFrame",
                    "export_table_state": None,
                    "variable_name": "var_b",
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    def test_it_should_apply_partial_results_and_throw_FunctionExportFailedException_on_export_download_error(
        self, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [
                    {
                        "export_name": "link_a",
                        "format": "json",
                        "data_type": "str",
                        "download_url": 'http://example.com/test/a"',
                    },
                    {
                        "export_name": "link_b",
                        "format": "dill",
                        "data_type": "str",
                        "download_url": 'http://example.com/test/b"',
                    },
                ],
                "errors": [],
            },
        )
        responses.add(
            responses.GET, 'http://example.com/test/a"', status=200, json="test-value-a"
        )
        responses.add(responses.GET, 'http://example.com/test/b"', status=404)

        scope = {}
        with self.assertRaises(FunctionExportFailedException):
            run_notebook_function(
                scope=scope,
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={
                    "link_a": {"variable_name": "var_a", "enabled": True},
                    "link_b": {"variable_name": "var_b", "enabled": True},
                },
            )

        self.assertEqual(scope["var_a"], "test-value-a")
        self.assertEqual(scope["var_b"], None)

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {
                        "link_a": {"variable_name": "var_a", "enabled": True},
                        "link_b": {"variable_name": "var_b", "enabled": True},
                    },
                    "executed_notebook_errors": [],
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_a",
                    "export_data_type": "str",
                    "export_table_state": None,
                    "variable_name": "var_a",
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_b",
                    "export_data_type": "str",
                    "export_table_state": None,
                    "variable_name": "var_b",
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_throw_general_FunctionRunFailedException_for_random_error_output(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "TestError",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        with self.assertRaises(FunctionRunFailedException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [
                        {
                            "error_output": {
                                "output_type": "error",
                                "ename": "TestError",
                                "evalue": "Test error",
                                "traceback": [],
                            },
                            "error_block_id": None,
                            "error_block_export_name": None,
                        }
                    ],
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_throw_FunctionNotAvailableException_for_FunctionNotAvailableException_error_output(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "FunctionNotAvailableException",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        with self.assertRaises(FunctionNotAvailableException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [
                        {
                            "error_output": {
                                "output_type": "error",
                                "ename": "FunctionNotAvailableException",
                                "evalue": "Test error",
                                "traceback": [],
                            },
                            "error_block_id": None,
                            "error_block_export_name": None,
                        }
                    ],
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_throw_FunctionNotebookNotModule_for_FunctionNotebookNotModule_error_output(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "FunctionNotebookNotModuleException",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        with self.assertRaises(FunctionNotebookNotModuleException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [
                        {
                            "error_output": {
                                "output_type": "error",
                                "ename": "FunctionNotebookNotModuleException",
                                "evalue": "Test error",
                                "traceback": [],
                            },
                            "error_block_id": None,
                            "error_block_export_name": None,
                        }
                    ],
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_throw_FunctionCyclicDependencyException_for_FunctionCyclicDependency_error_output(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "FunctionCyclicDependencyException",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        with self.assertRaises(FunctionCyclicDependencyException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [
                        {
                            "error_output": {
                                "output_type": "error",
                                "ename": "FunctionCyclicDependencyException",
                                "evalue": "Test error",
                                "traceback": [],
                            },
                            "error_block_id": None,
                            "error_block_export_name": None,
                        }
                    ],
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_throw_MissingInputVariableException_for_FunctionCyclicDependency_error_output(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "MissingInputVariableException",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        with self.assertRaises(MissingInputVariableException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [
                        {
                            "error_output": {
                                "output_type": "error",
                                "ename": "MissingInputVariableException",
                                "evalue": "Test error",
                                "traceback": [],
                            },
                            "error_block_id": None,
                            "error_block_export_name": None,
                        }
                    ],
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_throw_FunctionExportFailedException_for_FunctionCyclicDependency_error_output(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "FunctionExportFailedException",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        with self.assertRaises(FunctionExportFailedException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {},
                    "executed_notebook_errors": [
                        {
                            "error_output": {
                                "output_type": "error",
                                "ename": "FunctionExportFailedException",
                                "evalue": "Test error",
                                "traceback": [],
                            },
                            "error_block_id": None,
                            "error_block_export_name": None,
                        }
                    ],
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    def test_it_should_produce_partial_result_with_exports_before_error_output(
        self, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [
                    {
                        "export_name": "link_a",
                        "format": "json",
                        "data_type": "str",
                        "download_url": 'http://example.com/test/a"',
                    },
                    {
                        "export_name": "link_b",
                        "format": "json",
                        "data_type": "str",
                        "download_url": 'http://example.com/test/b"',
                    },
                ],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "TestError",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        responses.add(
            responses.GET, 'http://example.com/test/a"', status=200, json="test-value-a"
        )
        responses.add(
            responses.GET, 'http://example.com/test/b"', status=200, json="test-value-b"
        )

        scope = {}
        with self.assertRaises(FunctionRunFailedException):
            run_notebook_function(
                scope=scope,
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={
                    "link_a": {"variable_name": "var_a", "enabled": True},
                    "link_b": {"variable_name": "var_b", "enabled": True},
                    "link_c": {"variable_name": "var_c", "enabled": True},
                },
            )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_RUN_METADATA_MIME_TYPE: {
                    "notebook_function_run_id": "test-run-id",
                    "executed_notebook_id": "test-notebook-id",
                    "executed_notebook_name": "test-notebook-name",
                    "executed_notebook_inputs": {},
                    "executed_notebook_imports": {
                        "link_a": {"variable_name": "var_a", "enabled": True},
                        "link_b": {"variable_name": "var_b", "enabled": True},
                        "link_c": {"variable_name": "var_c", "enabled": True},
                    },
                    "executed_notebook_errors": [
                        {
                            "error_output": {
                                "output_type": "error",
                                "ename": "TestError",
                                "evalue": "Test error",
                                "traceback": [],
                            },
                            "error_block_id": None,
                            "error_block_export_name": None,
                        }
                    ],
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_a",
                    "export_data_type": "str",
                    "export_table_state": None,
                    "variable_name": "var_a",
                }
            }
        )
        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_b",
                    "export_data_type": "str",
                    "export_table_state": None,
                    "variable_name": "var_b",
                }
            }
        )

        self.assertEqual(scope["var_a"], "test-value-a")
        self.assertEqual(scope["var_b"], "test-value-b")
        self.assertEqual(scope["var_c"], None)

    @responses.activate
    def test_it_should_clear_import_variables_on_error_output(self):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [],
                "errors": [
                    {
                        "output": {
                            "output_type": "error",
                            "ename": "TestError",
                            "evalue": "Test error",
                            "traceback": [],
                        }
                    }
                ],
            },
        )

        scope = {
            "var_a": "stale-value-a",
            "var_b": "stale-value-b",
            "var_c": "stale-value-c",
        }
        with self.assertRaises(FunctionRunFailedException):
            run_notebook_function(
                scope=scope,
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={
                    "link_a": {"variable_name": "var_a", "enabled": True},
                    "link_b": {"variable_name": "var_b", "enabled": True},
                    "link_c": {"variable_name": "var_c", "enabled": False},
                },
            )

        self.assertEqual(scope["var_a"], None)
        self.assertEqual(scope["var_b"], None)
        self.assertEqual(scope["var_c"], "stale-value-c")

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    def test_it_should_apply_imported_dataframe_table_state(
        self, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [
                    {
                        "export_name": "link_a",
                        "format": "dill",
                        "data_type": "DataFrame",
                        "download_url": 'http://example.com/test/a"',
                        "table_state": {"pageSize": 30},
                    },
                ],
                "errors": [],
            },
        )

        df_a = pandas.DataFrame({"a": [1, 2, 3]})
        responses.add(
            responses.GET,
            'http://example.com/test/a"',
            status=200,
            body=dill.dumps(df_a),
        )

        scope = {}
        run_notebook_function(
            scope=scope,
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={
                "link_a": {"variable_name": "df_a", "enabled": True},
            },
        )
        self.assertEqual(
            json.loads(get_dataframe_browsing_spec(scope["df_a"])), {"pageSize": 30}
        )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_a",
                    "export_data_type": "DataFrame",
                    "export_table_state": {"pageSize": 30},
                    "variable_name": "df_a",
                }
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    def test_it_should_apply_dataframe_table_state_override(
        self, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [
                    {
                        "export_name": "link_a",
                        "format": "dill",
                        "data_type": "DataFrame",
                        "download_url": 'http://example.com/test/a"',
                        "table_state": {"pageSize": 30},
                    },
                ],
                "errors": [],
            },
        )

        df_a = pandas.DataFrame({"a": [1, 2, 3]})
        responses.add(
            responses.GET,
            'http://example.com/test/a"',
            status=200,
            body=dill.dumps(df_a),
        )

        scope = {}
        run_notebook_function(
            scope=scope,
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={
                "link_a": {"variable_name": "df_a", "enabled": True},
            },
            export_table_states_json=json.dumps({"link_a": {"pageSize": 50}}),
        )
        self.assertEqual(
            json.loads(get_dataframe_browsing_spec(scope["df_a"])), {"pageSize": 50}
        )

        mock_output_display_data.assert_any_call(
            {
                NOTEBOOK_FUNCTION_IMPORT_METADATA_MIME_TYPE: {
                    "export_name": "link_a",
                    "export_data_type": "DataFrame",
                    "export_table_state": {"pageSize": 50},
                    "variable_name": "df_a",
                }
            }
        )

    @responses.activate
    def test_it_should_throw_FunctionNotAvailableException_on_404_NotebookNotAvailable(
        self,
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=404,
            json={"error": "NotebookNotAvailable"},
        )

        with self.assertRaises(FunctionNotAvailableException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

    @responses.activate
    def test_it_should_throw_FunctionNotebookNotModuleException_on_405_NotebookNotModule(
        self,
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=405,
            json={"error": "NotebookNotModule"},
        )

        with self.assertRaises(FunctionNotebookNotModuleException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

    @responses.activate
    def test_it_should_throw_FunctionCyclicDependencyException_on_400_FunctionCyclicDependency(
        self,
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=400,
            json={"error": "FunctionCyclicDependency"},
        )

        with self.assertRaises(FunctionCyclicDependencyException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

    @responses.activate
    def test_it_should_throw_FunctionNotAvailableException_on_400_NestedFunctionNotAvailable(
        self,
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=400,
            json={"error": "NestedFunctionNotAvailable"},
        )

        with self.assertRaises(FunctionNotAvailableException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

    @responses.activate
    def test_it_should_throw_FunctionRunFailedException_on_400_InvaliParams(self):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=400,
            json={"error": "InvalidParams"},
        )

        with self.assertRaises(FunctionRunFailedException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

    @responses.activate
    def test_it_should_throw_FunctionRunFailedException_on_random_submit_error(self):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=500,
            json={"error": "TestError"},
        )

        with self.assertRaises(FunctionRunFailedException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    @patch("time.sleep", return_value=None)  # Patch time.sleep to be instant
    def test_it_should_keep_polling_status_on_various_errors(
        self, mock_sleep, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )

        # First try - 404 random not found error (server returns 401 when no such run exists)
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=404,
            json={"error": "NotFound"},
        )
        # Second try - 500 random error
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=500,
            json={"error": "TestError"},
        )
        # Third try - 200 waiting
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={"status": "waiting"},
        )
        # Fourth try - 200 done
        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=200,
            json={
                "status": "done",
                "exports": [
                    {
                        "export_name": "link_a",
                        "format": "json",
                        "data_type": "str",
                        "download_url": 'http://example.com/test/a"',
                    },
                ],
            },
        )

        responses.add(
            responses.GET, 'http://example.com/test/a"', status=200, json="test-value-a"
        )

        scope = {}
        run_notebook_function(
            scope=scope,
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
            inputs={},
            export_mappings={
                "link_a": {"variable_name": "var_a", "enabled": True},
            },
        )

        self.assertEqual(scope["var_a"], "test-value-a")

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    @patch("time.sleep", return_value=None)  # Patch time.sleep to be instant
    def test_it_throw_FunctionRunFailedException_on_run_status_polling_400_error(
        self, mock_sleep, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )

        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=400,
            json={"error": "InvalidParams"},
        )

        with self.assertRaises(FunctionRunFailedException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.output_display_data")
    @patch("deepnote_toolkit.notebook_functions.display")
    @patch("time.sleep", return_value=None)  # Patch time.sleep to be instant
    def test_it_throw_FunctionRunFailedException_on_run_status_polling_401_error(
        self, mock_sleep, mock_display, mock_output_display_data
    ):
        responses.add(
            responses.POST,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=202,
            json={
                "notebook_function_run_id": "test-run-id",
                "notebook_id": "test-notebook-id",
                "notebook_name": "test-notebook-name",
            },
        )

        responses.add(
            responses.GET,
            get_absolute_notebook_functions_api_url("test-notebook-id/test-run-id"),
            status=401,
            json={"error": "Unauthorized"},
        )

        with self.assertRaises(FunctionRunFailedException):
            run_notebook_function(
                scope={},
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
                inputs={},
                export_mappings={},
            )


class TestCancelNotebookFunction(TestCase):
    @responses.activate
    def test_it_should_call_notebook_functions_api_with_api_token_and_json_content_type(
        self,
    ):
        def request_callback(request):
            self.assertEqual(request.headers["Authorization"], "Bearer secret-token")
            self.assertEqual(request.headers["Content-Type"], "application/json")
            self.assertEqual(request.headers["Accept"], "application/json")
            request_callback.callCount += 1

            if (
                request.method == "DELETE"
                and request.url
                == get_absolute_notebook_functions_api_url("test-notebook-id")
            ):
                return (200, {}, json.dumps({}))

        request_callback.callCount = 0
        responses.add_callback(
            responses.DELETE,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            callback=request_callback,
            content_type="application/json",
        )

        cancel_notebook_function(
            notebook_function_api_token="secret-token",
            function_notebook_id="test-notebook-id",
        )

        self.assertEqual(
            request_callback.callCount, 1, "The request_callback was not called 1x"
        )

    @responses.activate
    def test_it_should_fail_on_random_error_response(
        self,
    ):
        responses.add(
            responses.DELETE,
            get_absolute_notebook_functions_api_url("test-notebook-id"),
            status=500,
        )

        with self.assertRaises(FunctionRunCancelFailedException):
            cancel_notebook_function(
                notebook_function_api_token="secret-token",
                function_notebook_id="test-notebook-id",
            )


class MockIPython:
    execution_count = 0
    _oh = {}


class TestExportLastBlockResult(TestCase):
    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    def test_it_should_upload_string_result_of_previous_execution_count_as_json(
        self, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        uploaded = None

        def upload_callback(request):
            nonlocal uploaded
            uploaded = {
                "content_type": request.headers["Content-Type"],
                "body": request.body,
            }
            return (200, {}, "")

        responses.add_callback(responses.PUT, upload_url, callback=upload_callback)

        Out = {2: {"test": "value"}}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="json")

        self.assertEqual(uploaded["content_type"], "application/json")
        self.assertEqual(
            parse_export_data(uploaded["body"], "json", "str"), {"test": "value"}
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    def test_it_should_upload_string_result_of_previous_execution_count_as_dill(
        self, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        uploaded = None

        def upload_callback(request):
            nonlocal uploaded
            uploaded = {
                "content_type": request.headers["Content-Type"],
                "body": request.body,
            }
            return (200, {}, "")

        responses.add_callback(responses.PUT, upload_url, callback=upload_callback)

        Out = {2: {"test": "value"}}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="dill")

        self.assertEqual(uploaded["content_type"], "application/octet-stream")
        self.assertEqual(
            parse_export_data(uploaded["body"], "dill", "str"), {"test": "value"}
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    def test_it_should_upload_dataframe_result_of_previous_execution_count_as_json(
        self, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        uploaded = None

        def upload_callback(request):
            nonlocal uploaded
            uploaded = {
                "content_type": request.headers["Content-Type"],
                "body": request.body,
            }
            return (200, {}, "")

        responses.add_callback(responses.PUT, upload_url, callback=upload_callback)

        Out = {2: pandas.DataFrame({"a": [1, 2, 3]})}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="json")

        self.assertEqual(uploaded["content_type"], "application/json")
        pandas.testing.assert_frame_equal(
            parse_export_data(uploaded["body"], "json", "DataFrame"),
            pandas.DataFrame({"a": [1, 2, 3]}),
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    def test_it_should_upload_dataframe_result_of_previous_execution_count_as_dill(
        self, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        uploaded = None

        def upload_callback(request):
            nonlocal uploaded
            uploaded = {
                "content_type": request.headers["Content-Type"],
                "body": request.body,
            }
            return (200, {}, "")

        responses.add_callback(responses.PUT, upload_url, callback=upload_callback)

        Out = {2: pandas.DataFrame({"a": [1, 2, 3]})}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="dill")

        self.assertEqual(uploaded["content_type"], "application/octet-stream")
        pandas.testing.assert_frame_equal(
            parse_export_data(uploaded["body"], "dill", "DataFrame"),
            pandas.DataFrame({"a": [1, 2, 3]}),
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    @patch("deepnote_toolkit.notebook_functions.JSON")
    def test_it_should_output_info_for_previous_execution_count_with_string_output(
        self, mock_JSON, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        responses.add(responses.PUT, upload_url, status=200)

        Out = {2: "test"}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="json")

        mock_JSON.assert_any_call(
            {
                "exported_data_type": "str",
                "exported_data_format": "json",
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    @patch("deepnote_toolkit.notebook_functions.JSON")
    def test_it_should_output_info_for_previous_execution_count_with_dataframe_output(
        self, mock_JSON, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        responses.add(responses.PUT, upload_url, status=200)

        Out = {2: pandas.DataFrame({"a": [1, 2, 3]})}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="json")

        mock_JSON.assert_any_call(
            {
                "exported_data_type": "DataFrame",
                "exported_data_format": "json",
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    def test_it_should_not_upload_empty_result_of_previous_execution_count(
        self, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        uploaded = False

        def upload_callback(request):
            nonlocal uploaded
            uploaded = True
            return (200, {}, "")

        responses.add_callback(responses.PUT, upload_url, callback=upload_callback)

        Out = {2: None}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="json")

        self.assertEqual(uploaded, False)

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    @patch("deepnote_toolkit.notebook_functions.JSON")
    def test_it_should_output_info_for_empty_result_of_previous_execution_count(
        self, mock_JSON, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        responses.add(responses.PUT, upload_url, status=200)

        Out = {2: None}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="json")

        mock_JSON.assert_any_call(
            {
                "exported_data_type": None,
                "exported_data_format": None,
            }
        )

    @responses.activate
    @patch("deepnote_toolkit.notebook_functions.get_ipython")
    @patch("deepnote_toolkit.notebook_functions.JSON")
    def test_it_should_output_info_for_missing_result_of_previous_execution_count(
        self, mock_JSON, mock_get_ipython
    ):
        upload_url = "http://example.com/test-upload-url"
        responses.add(responses.PUT, upload_url, status=200)

        Out = {}
        mock_ipython = MockIPython()
        mock_ipython.execution_count = 3
        mock_get_ipython.return_value = mock_ipython

        export_last_block_result(Out=Out, upload_url=upload_url, format="json")

        mock_JSON.assert_any_call(
            {
                "exported_data_type": None,
                "exported_data_format": None,
            }
        )
