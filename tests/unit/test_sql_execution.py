import base64
import copy
import datetime
import io
import json
import os
import secrets
import unittest
from unittest import TestCase, mock

import duckdb
import pandas as pd
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from parameterized import parameterized

from deepnote_toolkit.sql.sql_execution import (
    _sanitize_dataframe_for_parquet,
    execute_sql,
    execute_sql_with_connection_json,
)

from .helpers.testing_dataframes import testing_dataframes


class TestExecuteSql(TestCase):
    def test_duckdb_group_by_on_date(self):
        test_df = pd.DataFrame([{"d": datetime.date(2011, 1, 1)}])
        duckdb.register("test_df_view", test_df)

        os.environ["SQL_DEEPNOTE_DATAFRAME_SQL"] = (
            '{"url":"deepnote+duckdb:///:memory:","params":{},"param_style":"qmark"}'
        )

        result = execute_sql(
            """SELECT *
        FROM test_df group by d
        """,
            "SQL_DEEPNOTE_DATAFRAME_SQL",
        )

        assert result is not None
        self.assertEqual(len(result), 1, "Result should have exactly one row")

    def test_duckdb_concat_with_percentage_sign(self):
        from deepnote_toolkit.sql.duckdb_sql import _get_duckdb_connection

        test_df = pd.DataFrame([{"value": 25.5}])
        duckdb_conn = _get_duckdb_connection()
        duckdb_conn.register("test_df_concat", test_df)

        os.environ["SQL_DEEPNOTE_DATAFRAME_SQL"] = (
            '{"url":"deepnote+duckdb:///:memory:","params":{},"param_style":"qmark"}'
        )

        result = execute_sql(
            """SELECT
                concat(round(value, 1), '%') as percentage_string
            FROM test_df_concat
            """,
            "SQL_DEEPNOTE_DATAFRAME_SQL",
        )

        assert result is not None
        self.assertEqual(result.iloc[0]["percentage_string"], "25.5%")
        self.assertNotEqual(result.iloc[0]["percentage_string"], "25.5%%")

    def test_duckdb_defaults_to_qmark_param_style(self):
        os.environ["SQL_DEEPNOTE_DATAFRAME_SQL"] = (
            '{"url":"deepnote+duckdb:///:memory:","params":{},"param_style":null}'
        )

        result = execute_sql(
            "SELECT '%' as value",
            "SQL_DEEPNOTE_DATAFRAME_SQL",
        )

        assert result is not None
        self.assertEqual(result.iloc[0]["value"], "%")

    @mock.patch("deepnote_toolkit.sql.sql_execution._execute_sql_on_engine")
    @mock.patch("sqlalchemy.engine.create_engine")
    def test_delete_sql_that_doesnt_produce_a_dataframe(
        self, mocked_create_engine, execute_sql_on_engine
    ):
        mocked_create_engine.return_value = mock.Mock()
        execute_sql_on_engine.return_value = None

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        result = execute_sql(
            """DELETE FROM mock_table WHERE id = '1'""",
            "SQL_ENV_VAR",
        )

        self.assertIsNone(result)

    @mock.patch("deepnote_toolkit.sql.sql_caching._generate_cache_key")
    @mock.patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_sql_executed_with_audit_comment_but_hash_calculated_without_it(
        self,
        mocked_query_data_source,
        mocked_request_cache_info_from_webapp,
        mocked_generate_cache_key,
    ):
        mocked_request_cache_info_from_webapp.return_value = None

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        execute_sql(
            "SELECT * FROM users",
            "SQL_ENV_VAR",
            audit_sql_comment="/*audit_comment*/",
            sql_cache_mode="read_or_write",
        )

        # expect mocked_generate_cache_key to be called with a param that doesn't contain /*audit_comment*/
        mocked_generate_cache_key.assert_called_with("SELECT * FROM users", mock.ANY)

        # expect mocked_query_data_source to be called with param containing /*audit_comment*/
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM users/*audit_comment*/",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            mock.ANY,
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_return_variable_type_parameter(self, mocked_query_data_source):
        # Setup mock return value
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        # Test with default return_variable_type
        execute_sql(
            "SELECT * FROM test_table",
            "SQL_ENV_VAR",
        )
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM test_table",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "dataframe",
            mock.ANY,
        )

        # Test with explicit return_variable_type='query_preview'
        execute_sql(
            "SELECT * FROM test_table",
            "SQL_ENV_VAR",
            return_variable_type="query_preview",
        )
        # For query_preview, a LIMIT 100 clause is added to the query
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM test_table\nLIMIT 100",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "query_preview",
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_query_preview_preserves_trailing_inline_comment(
        self, mocked_query_data_source
    ):
        # Setup mock return value
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        # Test that trailing inline comment is preserved before LIMIT clause
        execute_sql(
            "SELECT * FROM test_table -- trailing",
            "SQL_ENV_VAR",
            return_variable_type="query_preview",
        )
        # For query_preview, a LIMIT 100 clause is added after the trailing comment
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM test_table -- trailing\nLIMIT 100",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "query_preview",
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_caching._generate_cache_key")
    @mock.patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_sql_executed_with_audit_comment_with_semicolon(
        self,
        mocked_query_data_source,
        mocked_request_cache_info_from_webapp,
        mocked_generate_cache_key,
    ):
        mocked_request_cache_info_from_webapp.return_value = None

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        execute_sql(
            "SELECT * FROM users;",  # Semicolon at the end of the query
            "SQL_ENV_VAR",
            audit_sql_comment="/*audit_comment*/",
            sql_cache_mode="read_or_write",
        )

        # expect mocked_generate_cache_key to be called with a param that doesn't contain /*audit_comment*/
        mocked_generate_cache_key.assert_called_with("SELECT * FROM users;", mock.ANY)

        # expect mocked_query_data_source to be called with param containing /*audit_comment*/
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM users/*audit_comment*/;",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "dataframe",
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_execute_sql_with_connection_json_with_snowflake_private_key(
        self, mock_execute_sql_with_caching
    ):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_key_b64 = base64.b64encode(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        ).decode("utf-8")

        template = "SELECT * FROM table"
        sql_alchemy_json = json.dumps(
            {
                "url": "snowflake://test@test?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {
                    "snowflake_private_key": private_key_b64,
                },
                "param_style": "pyformat",
            }
        )

        execute_sql_with_connection_json(template, sql_alchemy_json)

        args, _ = mock_execute_sql_with_caching.call_args

        # the private key is converted to DER format
        expected_private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        self.assertEqual(args[0], "SELECT * FROM table")
        self.assertEqual(
            args[2],
            {
                "url": "snowflake://test@test?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {"connect_args": {"private_key": expected_private_key_der}},
                "param_style": "pyformat",
            },
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_execute_sql_with_connection_json_with_snowflake_encrypted_private_key(
        self, mock_execute_sql_with_caching
    ):
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_key_passphrase = secrets.token_urlsafe(16)
        private_key_b64 = base64.b64encode(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.BestAvailableEncryption(
                    private_key_passphrase.encode("utf-8")
                ),
            )
        ).decode("utf-8")

        template = "SELECT * FROM table"
        sql_alchemy_json = json.dumps(
            {
                "url": "snowflake://test@test?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {
                    "snowflake_private_key": private_key_b64,
                    "snowflake_private_key_passphrase": private_key_passphrase,
                },
                "param_style": "pyformat",
            }
        )

        execute_sql_with_connection_json(template, sql_alchemy_json)

        args, _ = mock_execute_sql_with_caching.call_args

        # The private key is loaded with passphrase (decrypted) then converted to DER format without encryption
        expected_private_key_der = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        self.assertEqual(args[0], "SELECT * FROM table")
        self.assertEqual(
            args[2],
            {
                "url": "snowflake://test@test?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {"connect_args": {"private_key": expected_private_key_der}},
                "param_style": "pyformat",
            },
        )


class TestTrinoParamStyleAutoDetection(TestCase):
    """Tests for auto-detection of param_style for Trino connections"""

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_trino_url_auto_detects_qmark_param_style(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that Trino URLs automatically get 'qmark' param_style when not specified"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "trino://user@localhost:8080/catalog",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with 'qmark' param_style
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertEqual(call_args[2], "qmark")

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_non_trino_url_param_style_remains_none(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that non-Trino databases don't get auto-detected param_style"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "postgresql://user:pass@localhost:5432/mydb",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with None param_style
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertIsNone(call_args[2])

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_explicit_param_style_not_overridden(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that explicitly set param_style is preserved and not auto-detected"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        # Trino URL with explicit pyformat - should NOT be changed to qmark
        sql_alchemy_json = json.dumps(
            {
                "url": "trino://user@localhost:8080/catalog",
                "params": {},
                "param_style": "pyformat",
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with 'pyformat', NOT 'qmark'
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertEqual(call_args[2], "pyformat")

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_trino_url_with_protocol_suffix_not_matched(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that Trino URL variants like trino+rest:// don't match (drivername must be exactly 'trino')"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "trino+rest://user@localhost:8080/catalog",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with None param_style
        # because "trino+rest" doesn't match "trino" in the dictionary
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertIsNone(call_args[2])

    @mock.patch("deepnote_toolkit.sql.sql_execution.render_jinja_sql_template")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_trino_with_jinja_templates_uses_qmark(
        self, mocked_query_data_source, mocked_render_jinja
    ):
        """Test that Trino queries with Jinja templates correctly use qmark style"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_render_jinja.return_value = (
            "SELECT * FROM test_table WHERE id = ?",
            [123],
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "trino://user@localhost:8080/catalog",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json(
            "SELECT * FROM test_table WHERE id = {{ user_id }}",
            sql_alchemy_json,
        )

        # Verify render_jinja_sql_template was called with 'qmark' param_style
        mocked_render_jinja.assert_called()
        call_args = mocked_render_jinja.call_args[0]
        self.assertEqual(call_args[1], "qmark")

        # Verify bind_params is a list (qmark style) not dict (pyformat style)
        call_args = mocked_query_data_source.call_args[0]
        bind_params = call_args[1]
        self.assertIsInstance(bind_params, list)
        self.assertEqual(bind_params, [123])

    @mock.patch("pandas.read_sql_query")
    def test_list_bind_params_converted_to_tuple_for_pandas(self, mocked_read_sql):
        """Test that list bind_params are converted to tuple for pandas.read_sql_query"""
        from deepnote_toolkit.sql.sql_execution import _execute_sql_on_engine

        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_read_sql.return_value = mock_df

        # Mock engine and connection
        mock_engine = mock.Mock()
        mock_connection = mock.Mock()
        mock_engine.begin.return_value.__enter__ = mock.Mock(
            return_value=mock_connection
        )
        mock_engine.begin.return_value.__exit__ = mock.Mock(return_value=None)

        # Test with list bind_params (qmark style for Trino)
        list_params = [123, "test"]
        _execute_sql_on_engine(
            mock_engine,
            "SELECT * FROM test_table WHERE id = ? AND name = ?",
            list_params,
        )

        # Verify pandas.read_sql_query was called
        self.assertTrue(mocked_read_sql.called)

        # Get the params argument passed to pandas.read_sql_query
        call_kwargs = mocked_read_sql.call_args[1]
        params_arg = call_kwargs.get("params")

        # Verify that list was converted to tuple
        self.assertIsInstance(params_arg, tuple)
        self.assertEqual(params_arg, (123, "test"))

    @mock.patch("pandas.read_sql_query")
    def test_dict_bind_params_not_converted_for_pandas(self, mocked_read_sql):
        """Test that dict bind_params remain as dict for pandas.read_sql_query"""
        from deepnote_toolkit.sql.sql_execution import _execute_sql_on_engine

        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_read_sql.return_value = mock_df

        # Mock engine and connection
        mock_engine = mock.Mock()
        mock_connection = mock.Mock()
        mock_engine.begin.return_value.__enter__ = mock.Mock(
            return_value=mock_connection
        )
        mock_engine.begin.return_value.__exit__ = mock.Mock(return_value=None)

        # Test with dict bind_params (pyformat style)
        dict_params = {"id": 123, "name": "test"}
        _execute_sql_on_engine(
            mock_engine,
            "SELECT * FROM test_table WHERE id = %(id)s AND name = %(name)s",
            dict_params,
        )

        # Verify pandas.read_sql_query was called
        self.assertTrue(mocked_read_sql.called)

        # Get the params argument passed to pandas.read_sql_query
        call_kwargs = mocked_read_sql.call_args[1]
        params_arg = call_kwargs.get("params")

        # Verify that dict was NOT converted (remains as dict)
        self.assertIsInstance(params_arg, dict)
        self.assertEqual(params_arg, {"id": 123, "name": "test"})


class TestSanitizeDataframe(unittest.TestCase):
    @parameterized.expand([(key, df) for key, df in testing_dataframes.items()])
    def test_all_dataframes_serialize_to_parquet(self, key, df):
        # these are skipped because we are not expecting it's possible for dataframes like these
        # to come out of a SQL query
        skipped_dataframes = {
            "categorical_columns",
            "nested_list_column",
            "mixed_column_types",
            "multi_level_columns",
            "period_index",
            "non_serializable_values",
            "column_distinct_values",
        }

        if key in skipped_dataframes:
            return

        df_cleaned = df.copy()
        _sanitize_dataframe_for_parquet(df_cleaned)

        with io.BytesIO() as in_memory_file:
            try:
                df_cleaned.to_parquet(in_memory_file)
            except:  # noqa: E722
                self.fail(f"serializing to parquet failed for {key}")


class TestFederatedAuth(unittest.TestCase):
    @mock.patch("deepnote_toolkit.sql.sql_execution._get_federated_auth_credentials")
    def test_federated_auth_params_trino(self, mock_get_credentials):
        """Test that Trino federated auth updates the Authorization header with Bearer token."""
        from deepnote_toolkit.sql.sql_execution import (
            FederatedAuthResponseData,
            _handle_federated_auth_params,
        )

        # Setup mock to return Trino credentials
        mock_get_credentials.return_value = FederatedAuthResponseData(
            integrationType="trino",
            accessToken="test-trino-access-token",
        )

        # Create a sql_alchemy_dict with federatedAuthParams and the expected structure
        sql_alchemy_dict = {
            "url": "trino://user@localhost:8080/catalog",
            "params": {
                "connect_args": {
                    "http_headers": {
                        "Authorization": "Bearer old-token",
                    }
                }
            },
            "federatedAuthParams": {
                "integrationId": "test-integration-id",
                "authContextToken": "test-auth-context-token",
            },
        }

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify the API was called with correct params
        mock_get_credentials.assert_called_once_with(
            "test-integration-id", "test-auth-context-token"
        )

        # Verify the Authorization header was updated with the new token
        self.assertEqual(
            sql_alchemy_dict["params"]["connect_args"]["http_headers"]["Authorization"],
            "Bearer test-trino-access-token",
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._get_federated_auth_credentials")
    def test_federated_auth_params_bigquery(self, mock_get_credentials):
        """Test that BigQuery federated auth updates the access_token in params."""
        from deepnote_toolkit.sql.sql_execution import (
            FederatedAuthResponseData,
            _handle_federated_auth_params,
        )

        # Setup mock to return BigQuery credentials
        mock_get_credentials.return_value = FederatedAuthResponseData(
            integrationType="big-query",
            accessToken="test-bigquery-access-token",
        )

        # Create a sql_alchemy_dict with federatedAuthParams
        sql_alchemy_dict = {
            "url": "bigquery://?user_supplied_client=true",
            "params": {
                "access_token": "old-access-token",
                "project": "test-project",
            },
            "federatedAuthParams": {
                "integrationId": "test-bigquery-integration-id",
                "authContextToken": "test-bigquery-auth-context-token",
            },
        }

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify the API was called with correct params
        mock_get_credentials.assert_called_once_with(
            "test-bigquery-integration-id", "test-bigquery-auth-context-token"
        )

        # Verify the access_token was updated with the new token
        self.assertEqual(
            sql_alchemy_dict["params"]["access_token"],
            "test-bigquery-access-token",
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution.logger")
    @mock.patch("deepnote_toolkit.sql.sql_execution._get_federated_auth_credentials")
    def test_federated_auth_params_snowflake(self, mock_get_credentials, mock_logger):
        """Test that Snowflake federated auth logs a warning since it's not supported yet."""
        from deepnote_toolkit.sql.sql_execution import (
            FederatedAuthResponseData,
            _handle_federated_auth_params,
        )

        # Setup mock to return Snowflake credentials
        mock_get_credentials.return_value = FederatedAuthResponseData(
            integrationType="snowflake",
            accessToken="test-snowflake-access-token",
        )

        # Create a sql_alchemy_dict with federatedAuthParams
        sql_alchemy_dict = {
            "url": "snowflake://test@test?warehouse=&role=&application=Deepnote_Workspaces",
            "params": {},
            "federatedAuthParams": {
                "integrationId": "test-snowflake-integration-id",
                "authContextToken": "test-snowflake-auth-context-token",
            },
        }

        # Store original params to verify they remain unchanged
        original_params = copy.deepcopy(sql_alchemy_dict["params"])

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify the API was called with correct params
        mock_get_credentials.assert_called_once_with(
            "test-snowflake-integration-id", "test-snowflake-auth-context-token"
        )

        # Verify a warning was logged
        mock_logger.warning.assert_called_once_with(
            "Snowflake federated auth is not supported yet, using the original connection URL"
        )

        # Verify params were NOT modified (snowflake is not supported yet)
        self.assertEqual(sql_alchemy_dict["params"], original_params)

    def test_federated_auth_params_not_present(self):
        """Test that no action is taken when federatedAuthParams is not present."""
        from deepnote_toolkit.sql.sql_execution import _handle_federated_auth_params

        # Create a sql_alchemy_dict without federatedAuthParams
        sql_alchemy_dict = {
            "url": "trino://user@localhost:8080/catalog",
            "params": {
                "connect_args": {
                    "http_headers": {"Authorization": "Bearer original-token"}
                }
            },
        }

        original_dict = copy.deepcopy(sql_alchemy_dict)

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify the dict was not modified
        self.assertEqual(sql_alchemy_dict, original_dict)

    @mock.patch("deepnote_toolkit.sql.sql_execution.logger")
    def test_federated_auth_params_invalid_params(self, mock_logger):
        """Test that invalid federated auth params logs an error and returns early."""
        from deepnote_toolkit.sql.sql_execution import _handle_federated_auth_params

        # Create a sql_alchemy_dict with invalid federatedAuthParams (missing required fields)
        sql_alchemy_dict = {
            "url": "trino://user@localhost:8080/catalog",
            "params": {},
            "federatedAuthParams": {
                "invalidField": "value",
            },
        }

        original_dict = copy.deepcopy(sql_alchemy_dict)

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify an exception was logged
        mock_logger.exception.assert_called_once()
        call_args = mock_logger.exception.call_args
        self.assertIn("Invalid federated auth params", call_args[0][0])

        self.assertEqual(sql_alchemy_dict, original_dict)

    @mock.patch("deepnote_toolkit.sql.sql_execution.logger")
    @mock.patch("deepnote_toolkit.sql.sql_execution._get_federated_auth_credentials")
    def test_federated_auth_params_unsupported_integration_type(
        self, mock_get_credentials, mock_logger
    ):
        """Test that unsupported integration type logs an error."""
        from deepnote_toolkit.sql.sql_execution import (
            FederatedAuthResponseData,
            _handle_federated_auth_params,
        )

        # Setup mock to return unknown integration type
        mock_get_credentials.return_value = FederatedAuthResponseData(
            integrationType="unknown-database",
            accessToken="test-token",
        )

        # Create a sql_alchemy_dict with federatedAuthParams
        sql_alchemy_dict = {
            "url": "unknown://host/db",
            "params": {},
            "federatedAuthParams": {
                "integrationId": "test-integration-id",
                "authContextToken": "test-auth-context-token",
            },
        }

        original_dict = copy.deepcopy(sql_alchemy_dict)

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify an error was logged for unsupported integration type
        mock_logger.error.assert_called_once_with(
            "Unsupported integration type: %s, try updating toolkit version",
            "unknown-database",
        )

        self.assertEqual(sql_alchemy_dict, original_dict)

    @mock.patch("deepnote_toolkit.sql.sql_execution.logger")
    @mock.patch("deepnote_toolkit.sql.sql_execution._get_federated_auth_credentials")
    def test_federated_auth_params_trino_missing_http_headers(
        self, mock_get_credentials, mock_logger
    ):
        """Test that Trino federated auth logs exception when connect_args is missing http_headers."""
        from deepnote_toolkit.sql.sql_execution import (
            FederatedAuthResponseData,
            _handle_federated_auth_params,
        )

        # Setup mock to return Trino credentials
        mock_get_credentials.return_value = FederatedAuthResponseData(
            integrationType="trino",
            accessToken="test-trino-access-token",
        )

        # Create a sql_alchemy_dict with connect_args but missing http_headers
        sql_alchemy_dict = {
            "url": "trino://user@localhost:8080/catalog",
            "params": {
                "connect_args": {
                    # http_headers is missing
                }
            },
            "federatedAuthParams": {
                "integrationId": "test-integration-id",
                "authContextToken": "test-auth-context-token",
            },
        }

        original_dict = copy.deepcopy(sql_alchemy_dict)

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify the API was called with correct params
        mock_get_credentials.assert_called_once_with(
            "test-integration-id", "test-auth-context-token"
        )

        # Verify an exception was logged for missing http_headers
        mock_logger.exception.assert_called_once()
        call_args = mock_logger.exception.call_args
        self.assertIn("Invalid federated auth params", call_args[0][0])

        # Verify the dict was not modified
        self.assertEqual(sql_alchemy_dict, original_dict)

    @mock.patch("deepnote_toolkit.sql.sql_execution.logger")
    @mock.patch("deepnote_toolkit.sql.sql_execution._get_federated_auth_credentials")
    def test_federated_auth_params_bigquery_missing_params(
        self, mock_get_credentials, mock_logger
    ):
        """Test that BigQuery federated auth logs exception when params key is missing."""
        from deepnote_toolkit.sql.sql_execution import (
            FederatedAuthResponseData,
            _handle_federated_auth_params,
        )

        # Setup mock to return BigQuery credentials
        mock_get_credentials.return_value = FederatedAuthResponseData(
            integrationType="big-query",
            accessToken="test-bigquery-access-token",
        )

        # Create a sql_alchemy_dict without params key (will cause KeyError)
        sql_alchemy_dict = {
            "url": "bigquery://?user_supplied_client=true",
            # params key is missing entirely
            "federatedAuthParams": {
                "integrationId": "test-bigquery-integration-id",
                "authContextToken": "test-bigquery-auth-context-token",
            },
        }

        original_dict = copy.deepcopy(sql_alchemy_dict)

        # Call the function
        _handle_federated_auth_params(sql_alchemy_dict)

        # Verify the API was called with correct params
        mock_get_credentials.assert_called_once_with(
            "test-bigquery-integration-id", "test-bigquery-auth-context-token"
        )

        # Verify an exception was logged for missing params
        mock_logger.exception.assert_called_once()
        call_args = mock_logger.exception.call_args
        self.assertIn("Invalid federated auth params", call_args[0][0])

        # Verify the dict was not modified
        self.assertEqual(sql_alchemy_dict, original_dict)
