import unittest
from typing import Any
from unittest import mock
from unittest.mock import patch
from xml.sax.saxutils import escape

import pandas as pd
import requests
from parameterized import parameterized
from pyarrow import ArrowInvalid

from deepnote_toolkit.sql.sql_caching import (
    _describe_presigned_url,
    _describe_s3_response,
    _generate_cache_key,
    _redact_presigned_query,
    get_sql_cache,
    upload_sql_cache,
)
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


class TestUploadSqlCache(unittest.TestCase):
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_upload_parquet_success(self, mock_put):
        mock_put.return_value = mock.Mock(raise_for_status=mock.Mock())
        df = pd.DataFrame({"a": [1, 2, 3]})

        upload_sql_cache(df, "https://example.com/upload")

        mock_put.assert_called_once()
        args, _ = mock_put.call_args
        self.assertEqual(args[0], "https://example.com/upload")

    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_overflow_error_falls_back_to_pickle(self, mock_put):
        """Large Python int triggers OverflowError in to_parquet, upload succeeds via pickle."""
        uploaded_bytes = None

        def capture_put(_url, data):
            nonlocal uploaded_bytes
            uploaded_bytes = data.read()
            return mock.Mock(raise_for_status=mock.Mock())

        mock_put.side_effect = capture_put
        df = pd.DataFrame({"x": pd.array([2**100, 1], dtype=object)})

        upload_sql_cache(df, "https://example.com/upload")

        roundtripped = pd.read_pickle(pd.io.common.BytesIO(uploaded_bytes))
        pd.testing.assert_frame_equal(roundtripped, df)

    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_pickle_fallback_truncates_partial_parquet_bytes(self, mock_put):
        """When to_parquet writes partial bytes before failing, truncate clears them."""
        mock_put.return_value = mock.Mock(raise_for_status=mock.Mock())

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

        upload_sql_cache(df, "https://example.com/upload")

        self.assertEqual(pickle_pos, 0, "file should be at position 0")
        self.assertEqual(pickle_size, 0, "file should be empty after truncate")

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_http_error_logs_s3_diagnostics_without_presigned_url(
        self, mock_put: mock.MagicMock, mock_logger: mock.MagicMock
    ) -> None:
        """A 403 logs S3's Code/Message, request id and URL window, never the URL."""
        response = requests.Response()
        response.status_code = 403
        response.reason = "Forbidden"
        response.url = PRESIGNED_URL
        response.headers["x-amz-request-id"] = "REQ123"
        response._content = (
            b'<?xml version="1.0" encoding="UTF-8"?><Error>'
            b"<Code>SignatureDoesNotMatch</Code>"
            b"<Message>The request signature does not match</Message>"
            b"<AWSAccessKeyId>AKIAEXAMPLEKEY</AWSAccessKeyId>"
            b"<StringToSign>AWS4-HMAC-SHA256 20260729T120000Z</StringToSign>"
            b"</Error>"
        )
        mock_put.return_value = response

        upload_sql_cache(pd.DataFrame({"a": [1]}), PRESIGNED_URL)

        message = mock_logger.error.call_args.args[0]
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(message, "Failed to upload SQL cache")
        self.assertEqual(
            extra,
            {
                "sql_caching_cause": "failed_to_upload_to_cache",
                "error_type": "HTTPError",
                "error": "403 Client Error: Forbidden for url: "
                "https://bucket.s3.amazonaws.com/workspace/integration/cache-key?<redacted>",
                "status_code": 403,
                "s3_error_code": "SignatureDoesNotMatch",
                "s3_error_message": "The request signature does not match",
                "aws_request_id": "REQ123",
                "aws_host_id": None,
                "object_path": "/workspace/integration/cache-key",
                "url_expires_in": 900,
                "seconds_since_url_issued": mock.ANY,
            },
        )
        self.assertGreater(extra["seconds_since_url_issued"], 86400)
        self.assertNotIn(PRESIGNED_SECRET, repr(mock_logger.error.call_args))

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_http_error_with_non_xml_body_logs_status_only(
        self, mock_put: mock.MagicMock, mock_logger: mock.MagicMock
    ) -> None:
        """A non-XML body yields no S3 fields."""
        response = requests.Response()
        response.status_code = 502
        response._content = b"<!DOCTYPE html><html><body><hr>502</body></html>"
        mock_put.return_value = response

        upload_sql_cache(pd.DataFrame({"a": [1]}), PRESIGNED_URL)

        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["status_code"], 502)
        self.assertIsNone(extra["s3_error_code"])
        self.assertIsNone(extra["s3_error_message"])

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_connection_error_logs_redacted_message(
        self, mock_put: mock.MagicMock, mock_logger: mock.MagicMock
    ) -> None:
        """Non-HTTP failures keep the exception text minus the presigned query."""
        mock_put.side_effect = requests.ConnectionError(
            "HTTPSConnectionPool(host='bucket.s3.amazonaws.com', port=443): "
            f"Max retries exceeded with url: {PRESIGNED_PATH_AND_QUERY} "
            "(Caused by ConnectTimeoutError)"
        )

        upload_sql_cache(pd.DataFrame({"a": [1]}), PRESIGNED_URL)

        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["error_type"], "ConnectionError")
        self.assertEqual(
            extra["error"],
            "HTTPSConnectionPool(host='bucket.s3.amazonaws.com', port=443): "
            "Max retries exceeded with url: /workspace/integration/cache-key?<redacted> "
            "(Caused by ConnectTimeoutError)",
        )
        self.assertNotIn("status_code", extra)

    @parameterized.expand([("garbage_string", "not a url at all"), ("not_a_string", 1)])
    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_upload_failure_never_raises(
        self,
        _: str,
        upload_url: Any,
        mock_put: mock.MagicMock,
        mock_logger: mock.MagicMock,
    ) -> None:
        """A bad upload URL does not raise."""
        mock_put.side_effect = requests.ConnectionError("boom")

        upload_sql_cache(pd.DataFrame({"a": [1]}), upload_url)

        mock_logger.error.assert_called_once()


PRESIGNED_SECRET = "SECRETTOKEN"
PRESIGNED_PATH_AND_QUERY = (
    "/workspace/integration/cache-key"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=AKIA%2F20260729%2Fus-east-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260729T120000Z"
    "&X-Amz-Expires=900"
    f"&X-Amz-Security-Token={PRESIGNED_SECRET}"
    "&X-Amz-Signature=abc123"
)
PRESIGNED_URL = f"https://bucket.s3.amazonaws.com{PRESIGNED_PATH_AND_QUERY}"


class TestDescribeS3Response(unittest.TestCase):
    """Tests for _describe_s3_response."""

    def test_error_fields_are_redacted_and_capped(self) -> None:
        """Code and Message are redacted and capped at 200 characters."""
        response = requests.Response()
        response.status_code = 400
        response._content = (
            f"<Error><Code>{'X' * 300}</Code>"
            f"<Message>Rejected {escape(PRESIGNED_URL)}</Message></Error>"
        ).encode()

        described = _describe_s3_response(response)

        self.assertEqual(described["s3_error_code"], "X" * 200)
        self.assertEqual(
            described["s3_error_message"],
            "Rejected https://bucket.s3.amazonaws.com/workspace/integration/cache-key"
            "?<redacted>",
        )
        self.assertNotIn(PRESIGNED_SECRET, repr(described))


class TestDescribePresignedUrl(unittest.TestCase):
    """Tests for _describe_presigned_url."""

    @parameterized.expand(
        [
            ("no_query_string", "https://bucket.s3.amazonaws.com/path"),
            ("malformed_values", "https://x/path?X-Amz-Expires=soon&X-Amz-Date=today"),
        ]
    )
    def test_unusable_values_become_none(self, _: str, url: str) -> None:
        """Missing or malformed SigV4 params yield None."""
        self.assertEqual(
            _describe_presigned_url(url),
            {
                "object_path": "/path",
                "url_expires_in": None,
                "seconds_since_url_issued": None,
            },
        )

    def test_invalid_url_does_not_raise(self) -> None:
        """A URL urlsplit rejects yields an empty dict."""
        self.assertEqual(_describe_presigned_url("https://[bad"), {})


class TestRedactPresignedQuery(unittest.TestCase):
    """Tests for _redact_presigned_query."""

    def test_leaves_ordinary_text_alone(self) -> None:
        """Question marks and non-SigV4 query strings are not redacted."""
        self.assertEqual(
            _redact_presigned_query("Is the file valid? Yes: /path?a=1"),
            "Is the file valid? Yes: /path?a=1",
        )
