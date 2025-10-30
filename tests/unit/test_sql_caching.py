import unittest
from unittest.mock import patch

import pandas as pd
from parameterized import parameterized
from pyarrow import ArrowInvalid

from deepnote_toolkit.sql.sql_caching import _generate_cache_key, get_sql_cache
from deepnote_toolkit.sql.sql_utils import is_single_select_query


class TestGenerateCacheKey(unittest.TestCase):
    def test_empty_params_returns_valid_result(self):
        result = _generate_cache_key("SELECT * FROM users", {})

        # assert that the result contains only alphanumeric characters
        self.assertTrue(result.isalnum())

    def test_different_order_of_params_produces_same_result(self):
        result1 = _generate_cache_key("SELECT * FROM users", {"a": 1, "b": 2})
        result2 = _generate_cache_key("SELECT * FROM users", {"b": 2, "a": 1})

        self.assertEqual(result1, result2)


class TestIsSingleSelectQuery(unittest.TestCase):
    @parameterized.expand(
        [
            ("select_statement_only", "SELECT * FROM table", True),
            ("select_with_colon", "SELECT * FROM table;", True),
            ("select_with_newline", "SELECT * FROM table\n", True),
            ("select_with_colon_and_newline", "SELECT * FROM table;\n", True),
            (
                "select_statement_only_with_pyformat",
                "SELECT * FROM table WHERE id = %(id)s",
                True,
            ),
            (
                "multiple_select_queries",
                "SELECT * FROM table1; SELECT * FROM table2",
                False,
            ),
            (
                "multiple_statements_select_Firest",
                "SELECT * FROM table; UPDATE * FROM table",
                False,
            ),
            (
                "multiple_statements_update_first",
                "UPDATE * FROM table; SELECT * FROM table;",
                False,
            ),
            ("update_statement_first", "UPDATE table SET a = 1", False),
            ("update_statement_first", "DELETE FROM table", False),
            ("update_statement_first", "INSERT INTO table (a) VALUES (1)", False),
            ("with_cte", "WITH cte AS (SELECT * FROM table) SELECT * FROM cte", True),
        ]
    )
    def test_is_single_select_query(self, _, sql_string, expected):
        self.assertEqual(is_single_select_query(sql_string), expected)


class TestGetSqlCache(unittest.TestCase):
    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    def test_cache_not_supported_for_query(
        self,
        mock_output_sql_metadata,
        mock_request_cache_info_from_webapp,
        mock_is_single_select_query,
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = False

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        mock_output_sql_metadata.assert_called_with(
            {"status": "cache_not_supported_for_query"}
        )
        self.assertIsNone(result_df)
        self.assertIsNone(upload_url)

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    def test_failed_to_request_cache_info(
        self,
        mock_output_sql_metadata,
        mock_request_cache_info_from_webapp,
        mock_is_single_select_query,
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = True
        mock_request_cache_info_from_webapp.side_effect = Exception(
            "Failed to request cache info"
        )

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        mock_output_sql_metadata.assert_not_called()
        self.assertIsNone(result_df)
        self.assertIsNone(upload_url)

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("pandas.read_parquet")
    def test_read_from_cache_success(
        self,
        mock_read_parquet,
        mock_output_sql_metadata,
        mock_request_cache_info_from_webapp,
        mock_is_single_select_query,
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        cache_info = {
            "result": "cacheHit",
            "downloadUrl": "https://example.com/cache.parquet",
            "cacheCreatedAt": "2022-01-01 00:00:00",
        }
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = True
        mock_request_cache_info_from_webapp.return_value = cache_info
        mock_read_parquet.return_value = pd.DataFrame()

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        mock_output_sql_metadata.assert_called_with(
            {
                "status": "read_from_cache_success",
                "cache_created_at": cache_info["cacheCreatedAt"],
                "compiled_query": query,
                "variable_type": return_variable_type,
                "integration_id": integration_id,
            }
        )
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertIsNone(upload_url)

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("pandas.read_parquet")
    @patch("pandas.read_pickle")
    def test_fallback_to_pickle_format(
        self,
        mock_read_pickle,
        mock_read_parquet,
        mock_output_sql_metadata,
        mock_request_cache_info_from_webapp,
        mock_is_single_select_query,
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        cache_info = {
            "result": "cacheHit",
            "downloadUrl": "https://example.com/cache",
            "cacheCreatedAt": "2022-01-01 00:00:00",
        }
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = True
        mock_request_cache_info_from_webapp.return_value = cache_info
        mock_read_parquet.side_effect = ArrowInvalid
        mock_read_pickle.return_value = pd.DataFrame()

        result_df, upload_url = get_sql_cache(
            query,
            bind_params,
            integration_id,
            sql_cache_mode,
            return_variable_type,
        )

        mock_output_sql_metadata.assert_called_with(
            {
                "status": "read_from_cache_success",
                "cache_created_at": cache_info["cacheCreatedAt"],
                "compiled_query": query,
                "variable_type": return_variable_type,
                "integration_id": integration_id,
            }
        )
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertIsNone(upload_url)

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("pandas.read_parquet")
    def test_failed_to_download_from_cache(
        self,
        mock_read_parquet,
        mock_output_sql_metadata,
        mock_request_cache_info_from_webapp,
        mock_is_single_select_query,
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        cache_info = {
            "result": "cacheHit",
            "downloadUrl": "https://example.com/cache.parquet",
            "cacheCreatedAt": "2022-01-01 00:00:00",
        }
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = True
        mock_request_cache_info_from_webapp.return_value = cache_info
        mock_read_parquet.side_effect = Exception("Failed to download from cache")

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertIsNone(upload_url)

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    def test_cache_miss(
        self, mock_request_cache_info_from_webapp, mock_is_single_select_query
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        cache_info = {
            "result": "cacheMiss",
            "uploadUrl": "https://example.com/upload",
        }
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = True
        mock_request_cache_info_from_webapp.return_value = cache_info

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertEqual(upload_url, cache_info["uploadUrl"])

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    def test_always_write(
        self, mock_request_cache_info_from_webapp, mock_is_single_select_query
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        cache_info = {
            "result": "alwaysWrite",
            "uploadUrl": "https://example.com/upload",
        }
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = True
        mock_request_cache_info_from_webapp.return_value = cache_info

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertEqual(upload_url, cache_info["uploadUrl"])

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    def test_no_cache_info(
        self, mock_request_cache_info_from_webapp, mock_is_single_select_query
    ):
        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        return_variable_type = "dataframe"

        mock_is_single_select_query.return_value = True
        mock_request_cache_info_from_webapp.return_value = None

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertIsNone(upload_url)

    @patch("pandas.read_parquet")
    @patch("pandas.read_pickle")
    def test_read_from_cache_error_doesnt_raise(
        self, mock_read_pickle, mock_read_parquet
    ):
        mock_read_parquet.side_effect = ArrowInvalid
        mock_read_pickle.side_effect = Exception("Error reading pickle")

        query = "SELECT * FROM users"
        bind_params = {}
        integration_id = "123"
        sql_cache_mode = "read"
        return_variable_type = "dataframe"

        result_df, upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertIsNone(upload_url)
