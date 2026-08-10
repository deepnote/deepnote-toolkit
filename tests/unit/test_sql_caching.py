import json
import logging
import time
import unittest
from io import BytesIO
from unittest import mock
from unittest.mock import patch

import pandas as pd
import requests
from parameterized import parameterized
from pyarrow import ArrowInvalid

from deepnote_toolkit.sql.sql_caching import (
    SqlCacheUpload,
    _generate_cache_key,
    _request_cache_info_from_webapp,
    get_sql_cache,
    upload_sql_cache,
)
from deepnote_toolkit.sql.sql_utils import is_single_select_query

from .helpers.sql_cache_fixtures import (
    ACCESS_DENIED_EXPIRED_BODY,
    AWS_HEADERS,
    EXPIRED_TOKEN_BODY,
    PRESIGNED_URL,
    PROXY_ECHO_BODY,
    SECRETS,
    SIGNATURE_MISMATCH_BODY,
    URLLIB3_ERROR_MESSAGE,
    s3_response,
)

QUERY = "SELECT * FROM users"

RESERVED_LOGRECORD_ATTRS = set(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"message", "asctime"}


def _upload(url=PRESIGNED_URL, issued_at=None):
    return SqlCacheUpload(
        url=url, issued_at=time.monotonic() if issued_at is None else issued_at
    )


def _cache_hit(download_url=PRESIGNED_URL):
    return {
        "result": "cacheHit",
        "downloadUrl": download_url,
        "cacheCreatedAt": "2022-01-01 00:00:00",
    }


def _logged_strings(mock_logger):
    strings = []
    for call in mock_logger.error.call_args_list:
        strings.extend(str(arg) for arg in call.args)
        extra = call.kwargs.get("extra", {})
        strings.extend(str(key) for key in extra)
        strings.extend(str(value) for value in extra.values())
    return strings


def _collect_logged_extras():
    """Exercise every error path once for shared extra-dict guards."""
    dataframe = pd.DataFrame({"a": [1, 2, 3]})
    connection_error = requests.exceptions.ConnectionError(URLLIB3_ERROR_MESSAGE)

    with patch("deepnote_toolkit.sql.sql_caching.logger") as mock_logger:
        with patch("deepnote_toolkit.sql.sql_caching.requests.put") as mock_put:
            mock_put.return_value = s3_response(
                403, ACCESS_DENIED_EXPIRED_BODY, AWS_HEADERS
            )
            upload_sql_cache(dataframe, _upload())

            upload_sql_cache(dataframe, SqlCacheUpload(url=None, issued_at=0.0))

            mock_put.side_effect = connection_error
            upload_sql_cache(dataframe, _upload())

            unserializable = mock.Mock()
            unserializable.to_parquet.side_effect = ValueError("column customer_email")
            upload_sql_cache(unserializable, _upload())

        with patch(
            "deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp"
        ) as mock_cache_info:
            mock_cache_info.return_value = _cache_hit()
            with patch("deepnote_toolkit.sql.sql_caching.requests.get") as mock_get:
                mock_get.return_value = s3_response(
                    403, EXPIRED_TOKEN_BODY, AWS_HEADERS
                )
                get_sql_cache(QUERY, {}, "123", "read", "dataframe")

                mock_get.side_effect = connection_error
                get_sql_cache(QUERY, {}, "123", "read", "dataframe")

            mock_cache_info.side_effect = connection_error
            get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        with (
            patch("deepnote_toolkit.sql.sql_caching.requests.get") as mock_get,
            patch(
                "deepnote_toolkit.sql.sql_caching.get_absolute_userpod_api_url"
            ) as mock_url,
            patch(
                "deepnote_toolkit.sql.sql_caching.get_project_auth_headers"
            ) as mock_headers,
        ):
            mock_url.return_value = (
                "http://localhost:19456/userpod-api/p1/integrations/123/sql-cache"
                "?sqlCacheKey=abc&sqlCacheMode=read"
            )
            mock_headers.return_value = {}
            mock_get.return_value = s3_response(503, b"upstream unavailable")
            _request_cache_info_from_webapp("abc", "123", "read")

        return [call.kwargs["extra"] for call in mock_logger.error.call_args_list]


class TestGenerateCacheKey(unittest.TestCase):
    def test_empty_params_returns_valid_result(self):
        result = _generate_cache_key("SELECT * FROM users", {})

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

        result_df, upload = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        mock_output_sql_metadata.assert_called_with(
            {"status": "cache_not_supported_for_query"}
        )
        self.assertIsNone(result_df)
        self.assertIsNone(upload)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    def test_failed_to_request_cache_info(
        self,
        mock_output_sql_metadata,
        mock_request_cache_info_from_webapp,
        mock_is_single_select_query,
        mock_logger,
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

        result_df, upload = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        mock_output_sql_metadata.assert_not_called()
        self.assertIsNone(result_df)
        self.assertIsNone(upload)

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    @patch("pandas.read_parquet")
    def test_read_from_cache_success(
        self,
        mock_read_parquet,
        mock_get,
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
        mock_get.return_value = s3_response(200, b"parquet-bytes")
        mock_read_parquet.return_value = pd.DataFrame()

        result_df, upload = get_sql_cache(
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
        self.assertIsNone(upload)

    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    @patch("pandas.read_parquet")
    @patch("pandas.read_pickle")
    def test_fallback_to_pickle_format(
        self,
        mock_read_pickle,
        mock_read_parquet,
        mock_get,
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
        mock_get.return_value = s3_response(200, b"pickle-bytes")
        mock_read_parquet.side_effect = ArrowInvalid
        mock_read_pickle.return_value = pd.DataFrame()

        result_df, upload = get_sql_cache(
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
        self.assertIsNone(upload)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.is_single_select_query")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    @patch("pandas.read_parquet")
    def test_failed_to_download_from_cache(
        self,
        mock_read_parquet,
        mock_get,
        mock_output_sql_metadata,
        mock_request_cache_info_from_webapp,
        mock_is_single_select_query,
        mock_logger,
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
        mock_get.return_value = s3_response(200, b"parquet-bytes")
        mock_read_parquet.side_effect = Exception("Failed to download from cache")

        result_df, upload = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertIsNone(upload)

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

        result_df, upload = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertIsInstance(upload, SqlCacheUpload)
        self.assertEqual(upload.url, cache_info["uploadUrl"])
        self.assertIsInstance(upload.issued_at, float)

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

        result_df, upload = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertIsInstance(upload, SqlCacheUpload)
        self.assertEqual(upload.url, cache_info["uploadUrl"])
        self.assertIsInstance(upload.issued_at, float)

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

        result_df, upload = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )

        self.assertIsNone(result_df)
        self.assertIsNone(upload)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    @patch("pandas.read_parquet")
    @patch("pandas.read_pickle")
    def test_read_from_cache_error_doesnt_raise(
        self,
        mock_read_pickle,
        mock_read_parquet,
        mock_get,
        mock_cache_info,
        mock_logger,
    ):
        mock_cache_info.return_value = _cache_hit("https://example.com/cache")
        mock_get.return_value = s3_response(200, b"not-a-dataframe")
        mock_read_parquet.side_effect = ArrowInvalid
        mock_read_pickle.side_effect = Exception("Error reading pickle")

        result_df, upload = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        mock_read_parquet.assert_called_once()
        mock_read_pickle.assert_called_once()
        self.assertIsNone(result_df)
        self.assertIsNone(upload)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    def test_download_http_error_logs_s3_diagnostics(
        self, mock_get, mock_cache_info, mock_logger
    ):
        mock_cache_info.return_value = _cache_hit()
        mock_get.return_value = s3_response(403, EXPIRED_TOKEN_BODY, AWS_HEADERS)

        result_df, upload = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        self.assertIsNone(result_df)
        self.assertIsNone(upload)
        mock_logger.error.assert_called_once()
        self.assertEqual(
            mock_logger.error.call_args.args[0],
            "Failed to download dataframe from cache: %s",
        )
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["sql_caching_cause"], "failed_to_download_from_cache")
        self.assertEqual(extra["s3_error_code"], "ExpiredToken")
        self.assertEqual(extra["status_code"], 403)
        self.assertEqual(extra["aws_request_id"], "REQ123")
        self.assertEqual(extra["object_path"], "/ws/int/key")

    @parameterized.expand(
        [
            ("signature_mismatch_body", SIGNATURE_MISMATCH_BODY),
            ("proxy_echoing_request_url", PROXY_ECHO_BODY),
        ]
    )
    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    def test_download_http_error_logs_no_query_string(
        self, _name, body, mock_get, mock_cache_info, mock_logger
    ):
        mock_cache_info.return_value = _cache_hit()
        mock_get.return_value = s3_response(403, body, AWS_HEADERS)

        get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        mock_logger.error.assert_called_once()
        for logged in _logged_strings(mock_logger):
            for secret in SECRETS:
                self.assertNotIn(secret, logged)

    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    def test_download_reads_parquet_from_fetched_bytes(
        self, mock_get, mock_cache_info, mock_output_sql_metadata
    ):
        dataframe = pd.DataFrame({"a": [1, 2, 3]})
        buffer = BytesIO()
        dataframe.to_parquet(buffer)

        mock_cache_info.return_value = _cache_hit()
        mock_get.return_value = s3_response(200, buffer.getvalue())

        result_df, _ = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        pd.testing.assert_frame_equal(result_df, dataframe)
        mock_get.assert_called_once()
        self.assertEqual(mock_get.call_args.args[0], PRESIGNED_URL)

    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    def test_download_falls_back_to_pickle_from_same_bytes(
        self, mock_get, mock_cache_info, mock_output_sql_metadata
    ):
        dataframe = pd.DataFrame({"a": [1, 2, 3]})
        buffer = BytesIO()
        dataframe.to_pickle(buffer)

        mock_cache_info.return_value = _cache_hit()
        mock_get.return_value = s3_response(200, buffer.getvalue())

        result_df, _ = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        pd.testing.assert_frame_equal(result_df, dataframe)
        mock_get.assert_called_once()

    @patch("deepnote_toolkit.sql.sql_caching.output_sql_metadata")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    def test_download_request_is_bounded_and_streamed(
        self, mock_get, mock_cache_info, mock_output_sql_metadata
    ):
        dataframe = pd.DataFrame({"a": [1, 2, 3]})
        buffer = BytesIO()
        dataframe.to_parquet(buffer)

        mock_cache_info.return_value = _cache_hit()
        mock_get.return_value = s3_response(200, buffer.getvalue())

        get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        connect_timeout, read_timeout = mock_get.call_args.kwargs["timeout"]
        self.assertIsNotNone(connect_timeout)
        self.assertIsNotNone(read_timeout)
        self.assertTrue(mock_get.call_args.kwargs["stream"])

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    def test_download_network_error_is_swallowed(
        self, mock_get, mock_cache_info, mock_logger
    ):
        mock_cache_info.return_value = _cache_hit()
        mock_get.side_effect = requests.exceptions.ConnectionError(
            URLLIB3_ERROR_MESSAGE
        )

        result_df, upload = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        self.assertIsNone(result_df)
        self.assertIsNone(upload)
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["error_type"], "ConnectionError")
        for logged in _logged_strings(mock_logger):
            for secret in SECRETS:
                self.assertNotIn(secret, logged)


class TestRequestCacheInfoFromWebapp(unittest.TestCase):
    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.get_project_auth_headers")
    @patch("deepnote_toolkit.sql.sql_caching.get_absolute_userpod_api_url")
    @patch("deepnote_toolkit.sql.sql_caching.requests.get")
    def test_non_200_logs_constant_message_with_status_in_extra(
        self, mock_get, mock_url, mock_headers, mock_logger
    ):
        mock_url.return_value = (
            "http://localhost:19456/userpod-api/p1/integrations/123/sql-cache"
            "?sqlCacheKey=abc&sqlCacheMode=read"
        )
        mock_headers.return_value = {}
        mock_get.return_value = s3_response(503, b"upstream unavailable")

        self.assertIsNone(_request_cache_info_from_webapp("abc", "123", "read"))

        self.assertEqual(
            mock_logger.error.call_args.args[0], "Failed to request cache info: %s"
        )
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["status_code"], 503)
        self.assertEqual(extra["response_body_snippet"], "upstream unavailable")
        self.assertEqual(
            extra["cache_info_path"],
            "/userpod-api/p1/integrations/123/sql-cache",
        )
        self.assertNotIn("sqlCacheKey", " ".join(_logged_strings(mock_logger)))


class TestUploadSqlCache(unittest.TestCase):
    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_upload_parquet_success(self, mock_put, mock_logger):
        mock_put.return_value = mock.Mock(status_code=200)
        df = pd.DataFrame({"a": [1, 2, 3]})

        upload_sql_cache(df, _upload("https://example.com/upload"))

        mock_put.assert_called_once()
        args, _ = mock_put.call_args
        self.assertEqual(args[0], "https://example.com/upload")
        mock_logger.error.assert_not_called()

    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_overflow_error_falls_back_to_pickle(self, mock_put):
        """Large Python int triggers OverflowError in to_parquet, upload succeeds via pickle."""
        uploaded_bytes = None

        def capture_put(_url, data, **_kwargs):
            nonlocal uploaded_bytes
            uploaded_bytes = data.read()
            return mock.Mock(status_code=200)

        mock_put.side_effect = capture_put
        df = pd.DataFrame({"x": pd.array([2**100, 1], dtype=object)})

        upload_sql_cache(df, _upload("https://example.com/upload"))

        roundtripped = pd.read_pickle(pd.io.common.BytesIO(uploaded_bytes))
        pd.testing.assert_frame_equal(roundtripped, df)

    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_arrow_failure_still_falls_back_to_pickle_and_uploads(self, mock_put):
        """The narrowed serialization handler must not swallow the pickle fallback."""
        uploaded_bytes = None

        def capture_put(_url, data, **_kwargs):
            nonlocal uploaded_bytes
            uploaded_bytes = data.read()
            return mock.Mock(status_code=200)

        mock_put.side_effect = capture_put
        # nested dicts of mixed shape are not representable in parquet
        df = pd.DataFrame({"x": [{"a": 1}, {"a": "two"}]})

        upload_sql_cache(df, _upload("https://example.com/upload"))

        mock_put.assert_called_once()
        roundtripped = pd.read_pickle(BytesIO(uploaded_bytes))
        pd.testing.assert_frame_equal(roundtripped, df)

    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_pickle_fallback_truncates_partial_parquet_bytes(self, mock_put):
        """When to_parquet writes partial bytes before failing, truncate clears them."""
        mock_put.return_value = mock.Mock(status_code=200)

        def write_garbage_then_overflow(f, **_kwargs):
            f.write(b"partial parquet data")
            raise OverflowError("Python int too large")

        pickle_pos = None
        pickle_size = None

        def capture_file_state(f, **_kwargs):
            nonlocal pickle_pos, pickle_size
            pickle_pos = f.tell()
            pickle_size = f.seek(0, 2)
            f.seek(0)

        df = mock.Mock()
        df.to_parquet.side_effect = write_garbage_then_overflow
        df.to_pickle.side_effect = capture_file_state

        upload_sql_cache(df, _upload("https://example.com/upload"))

        self.assertEqual(pickle_pos, 0, "file should be at position 0")
        self.assertEqual(pickle_size, 0, "file should be empty after truncate")

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_http_error_logs_s3_diagnostics(self, mock_put, mock_logger):
        mock_put.return_value = s3_response(
            403, ACCESS_DENIED_EXPIRED_BODY, AWS_HEADERS
        )

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        mock_logger.error.assert_called_once()
        self.assertEqual(
            mock_logger.error.call_args.args[0], "Failed to upload SQL cache: %s"
        )
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["sql_caching_cause"], "failed_to_upload_to_cache")
        self.assertEqual(extra["s3_error_code"], "AccessDenied")
        self.assertEqual(extra["s3_error_message"], "Request has expired")
        self.assertEqual(extra["status_code"], 403)
        self.assertEqual(extra["aws_request_id"], "REQ123")
        self.assertEqual(extra["aws_host_id"], "HOSTID456")
        self.assertEqual(extra["object_path"], "/ws/int/key")
        self.assertEqual(extra["url_expires_in"], 900)
        self.assertGreaterEqual(extra["seconds_since_url_issued"], 0)

    @parameterized.expand(
        [
            ("signature_mismatch_body", SIGNATURE_MISMATCH_BODY),
            ("proxy_echoing_request_url", PROXY_ECHO_BODY),
        ]
    )
    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_http_error_logs_no_presigned_query_string(
        self, _name, body, mock_put, mock_logger
    ):
        mock_put.return_value = s3_response(403, body, AWS_HEADERS)

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        mock_logger.error.assert_called_once()
        for logged in _logged_strings(mock_logger):
            for secret in SECRETS:
                self.assertNotIn(secret, logged)

    @parameterized.expand(
        [
            ("connection", requests.exceptions.ConnectionError, "ConnectionError"),
            ("timeout", requests.exceptions.Timeout, "Timeout"),
        ]
    )
    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_network_error_logs_no_presigned_url(
        self, _name, error, expected_type, mock_put, mock_logger
    ):
        mock_put.side_effect = error(URLLIB3_ERROR_MESSAGE)

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["sql_caching_cause"], "failed_to_upload_to_cache")
        self.assertEqual(extra["error_type"], expected_type)
        for logged in _logged_strings(mock_logger):
            for secret in SECRETS:
                self.assertNotIn(secret, logged)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_message_is_constant_across_different_failures(self, mock_put, mock_logger):
        df = pd.DataFrame({"a": [1, 2, 3]})
        other_url = PRESIGNED_URL.replace("/ws/int/key", "/other/int/key2")

        mock_put.return_value = s3_response(403, ACCESS_DENIED_EXPIRED_BODY)
        upload_sql_cache(df, _upload())
        mock_put.return_value = s3_response(500, b"<Error><Code>Slow</Code></Error>")
        upload_sql_cache(df, _upload(other_url))
        mock_put.side_effect = requests.exceptions.ConnectionError(
            URLLIB3_ERROR_MESSAGE
        )
        upload_sql_cache(df, _upload())
        upload_sql_cache(df, _upload(other_url))

        templates = {call.args[0] for call in mock_logger.error.call_args_list}
        self.assertEqual(templates, {"Failed to upload SQL cache: %s"})

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_serialization_failure_uses_distinct_cause(self, mock_put, mock_logger):
        df = mock.Mock()
        df.to_parquet.side_effect = ValueError("Conversion failed for column secret")
        df.to_pickle.side_effect = ValueError("Conversion failed for column secret")

        upload_sql_cache(df, _upload())

        mock_put.assert_not_called()
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["sql_caching_cause"], "failed_to_serialize_cache")
        self.assertEqual(extra["error_type"], "ValueError")
        self.assertNotIn("error_message", extra)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.tempfile.TemporaryFile")
    def test_failure_before_the_request_leaves_both_times_unset(
        self, mock_temp_file, mock_logger
    ):
        mock_temp_file.side_effect = OSError("no space left on device")

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertIsNone(extra["seconds_since_url_issued"])
        self.assertIsNone(extra["upload_duration_seconds"])

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    @patch("deepnote_toolkit.sql.sql_caching.time.monotonic")
    def test_transfer_time_is_not_counted_as_url_age(
        self, mock_monotonic, mock_put, mock_logger
    ):
        clock = [1000.0]
        mock_monotonic.side_effect = lambda: clock[0]

        def slow_put(*args, **kwargs):
            clock[0] += 1000.0
            return s3_response(500, b"<Error><Code>InternalError</Code></Error>")

        mock_put.side_effect = slow_put

        upload_sql_cache(
            pd.DataFrame({"a": [1, 2, 3]}),
            SqlCacheUpload(url=PRESIGNED_URL, issued_at=100.5),
        )

        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["seconds_since_url_issued"], 899.5)
        self.assertEqual(extra["upload_duration_seconds"], 1000.0)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_upload_request_bounds_connect_and_read_phases(self, mock_put, mock_logger):
        mock_put.return_value = s3_response(200)

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        connect_timeout, read_timeout = mock_put.call_args.kwargs["timeout"]
        self.assertIsNotNone(connect_timeout)
        self.assertIsNotNone(read_timeout)


class TestLoggedExtras(unittest.TestCase):
    def setUp(self):
        self.extras = _collect_logged_extras()

    def test_every_failure_path_logs_a_cause(self):
        self.assertEqual(
            {extra["sql_caching_cause"] for extra in self.extras},
            {
                "failed_to_upload_to_cache",
                "failed_to_serialize_cache",
                "failed_to_download_from_cache",
                "failed_to_request_cache_info",
                "http_error",
            },
        )

    def test_extra_keys_avoid_reserved_logrecord_attributes(self):
        """Reserved extra keys make logger.error raise into the user's cell."""
        for extra in self.extras:
            self.assertEqual(set(extra) & RESERVED_LOGRECORD_ATTRS, set())

    def test_extra_is_json_serializable(self):
        """Non-JSON-serializable extras drop the whole error report."""
        for extra in self.extras:
            for value in extra.values():
                self.assertIsInstance(value, (str, int, float, bool, type(None)))
            json.dumps(extra)
