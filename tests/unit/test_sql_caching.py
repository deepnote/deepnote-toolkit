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
    _URL_QUERY_PATTERN,
    SqlCacheUpload,
    _describe_exception,
    _describe_presigned_url,
    _describe_s3_error,
    _generate_cache_key,
    _redact_sensitive,
    _request_cache_info_from_webapp,
    _safe_url_path,
    get_sql_cache,
    upload_sql_cache,
)
from deepnote_toolkit.sql.sql_utils import is_single_select_query

QUERY = "SELECT * FROM users"

# Signing parameters given values that are easy to search log output for
PRESIGNED_URL = (
    "https://bucket.s3.eu-west-1.amazonaws.com/ws/int/key"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=CREDVALUE%2F20260729%2Feu-west-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260729T120000Z&X-Amz-Expires=900"
    "&X-Amz-Security-Token=TOKENVALUE&X-Amz-Signature=SIGVALUE"
)

# What requests raises when it cannot reach the object store. The URL urllib3
# embeds is path-only - there is no scheme and no host in front of it.
URLLIB3_ERROR_MESSAGE = (
    "HTTPSConnectionPool(host='bucket.s3.eu-west-1.amazonaws.com', port=443): "
    "Max retries exceeded with url: /ws/int/key"
    "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    "&X-Amz-Credential=CREDVALUE%2F20260729%2Feu-west-1%2Fs3%2Faws4_request"
    "&X-Amz-Date=20260729T120000Z&X-Amz-Expires=900"
    "&X-Amz-Security-Token=TOKENVALUE&X-Amz-Signature=SIGVALUE "
    "(Caused by NameResolutionError('Failed to resolve host'))"
)

SECRETS = ("CREDVALUE", "TOKENVALUE", "SIGVALUE", "X-Amz-")

AWS_HEADERS = {
    "x-amz-request-id": "REQ123",
    "x-amz-id-2": "HOSTID456",
    "Date": "Wed, 29 Jul 2026 12:31:07 GMT",
}

ACCESS_DENIED_EXPIRED_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>AccessDenied</Code><Message>Request has expired</Message>"
    b"<X-Amz-Expires>900</X-Amz-Expires>"
    b"<Expires>2026-07-29T12:15:00Z</Expires>"
    b"<ServerTime>2026-07-29T12:31:07Z</ServerTime>"
    b"<RequestId>REQ123</RequestId><HostId>HOSTID456</HostId></Error>"
)

# S3 rejecting credentials that died before the URL's own expiry
EXPIRED_TOKEN_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>ExpiredToken</Code>"
    b"<Message>The provided token has expired.</Message>"
    b"<RequestId>REQ123</RequestId><HostId>HOSTID456</HostId></Error>"
)

# S3 echoes the canonical request - and with it the signed query string - when
# the signature does not match
SIGNATURE_MISMATCH_BODY = (
    b'<?xml version="1.0" encoding="UTF-8"?>\n'
    b"<Error><Code>SignatureDoesNotMatch</Code>"
    b"<Message>The request signature we calculated does not match the signature "
    b"you provided. Check your key and signing method.</Message>"
    b"<AWSAccessKeyId>CREDVALUE</AWSAccessKeyId>"
    b"<StringToSign>AWS4-HMAC-SHA256\n20260729T120000Z\n</StringToSign>"
    b"<CanonicalRequest>PUT\n/ws/int/key\n"
    b"X-Amz-Credential=CREDVALUE&amp;X-Amz-Security-Token=TOKENVALUE"
    b"&amp;X-Amz-Signature=SIGVALUE\nhost:bucket.s3.eu-west-1.amazonaws.com\n"
    b"</CanonicalRequest>"
    b"<RequestId>REQ123</RequestId><HostId>HOSTID456</HostId></Error>"
)

# A gateway that answered instead of S3 and echoed the request line back
PROXY_ECHO_BODY = (
    b"<html><head><title>502 Bad Gateway</title></head><body>\n"
    b"<h1>502 Bad Gateway</h1>\n"
    b"<p>Upstream failed for request: PUT "
    b"https://bucket.s3.eu-west-1.amazonaws.com/ws/int/key"
    b"?X-Amz-Algorithm=AWS4-HMAC-SHA256"
    b"&X-Amz-Credential=CREDVALUE%2F20260729%2Feu-west-1%2Fs3%2Faws4_request"
    b"&X-Amz-Date=20260729T120000Z&X-Amz-Expires=900"
    b"&X-Amz-Security-Token=TOKENVALUE&X-Amz-Signature=SIGVALUE</p>\n"
    b"</body></html>"
)

RESERVED_LOGRECORD_ATTRS = set(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"message", "asctime"}


def _s3_response(status_code, body=b"", headers=None):
    """Build a stand-in for a requests.Response from the object store."""
    response = mock.MagicMock(
        status_code=status_code, content=body, headers=headers or {}
    )
    response.__enter__.return_value = response
    # a real streamed body reads back empty once the block exits, silently
    response.__exit__.side_effect = lambda *_: setattr(response, "content", b"")
    # a chunked response yields one HTTP chunk per read however large a size is
    # asked for
    response.iter_content.side_effect = lambda size: iter(
        [body[i : i + 20] for i in range(0, len(body), 20)]
    )
    return response


def _upload(url=PRESIGNED_URL, issued_at=None):
    """Build an upload handle, issued now unless told otherwise."""
    return SqlCacheUpload(
        url=url, issued_at=time.monotonic() if issued_at is None else issued_at
    )


def _cache_hit(download_url=PRESIGNED_URL):
    """The webapp's answer when the query has a cached result waiting."""
    return {
        "result": "cacheHit",
        "downloadUrl": download_url,
        "cacheCreatedAt": "2022-01-01 00:00:00",
    }


def _logged_strings(mock_logger):
    """Every string a logger.error call would hand to the error pipeline."""
    strings = []
    for call in mock_logger.error.call_args_list:
        strings.extend(str(arg) for arg in call.args)
        extra = call.kwargs.get("extra", {})
        strings.extend(str(key) for key in extra)
        strings.extend(str(value) for value in extra.values())
    return strings


def _collect_logged_extras():
    """Run every failure path in the module and collect the extras it logs.

    Gathered in one place so the reserved-key and serializability guards cover all
    of them, rather than whichever path a single test happened to exercise.
    """
    dataframe = pd.DataFrame({"a": [1, 2, 3]})
    connection_error = requests.exceptions.ConnectionError(URLLIB3_ERROR_MESSAGE)

    with patch("deepnote_toolkit.sql.sql_caching.logger") as mock_logger:
        with patch("deepnote_toolkit.sql.sql_caching.requests.put") as mock_put:
            mock_put.return_value = _s3_response(
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
                mock_get.return_value = _s3_response(
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
            mock_get.return_value = _s3_response(503, b"upstream unavailable")
            _request_cache_info_from_webapp("abc", "123", "read")

        return [call.kwargs["extra"] for call in mock_logger.error.call_args_list]


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
        mock_get.return_value = _s3_response(200, b"parquet-bytes")
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
        mock_get.return_value = _s3_response(200, b"pickle-bytes")
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
        mock_get.return_value = _s3_response(200, b"parquet-bytes")
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
        mock_get.return_value = _s3_response(200, b"not-a-dataframe")
        mock_read_parquet.side_effect = ArrowInvalid
        mock_read_pickle.side_effect = Exception("Error reading pickle")

        result_df, upload = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        # both read attempts ran, and the failure of the second was swallowed
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
        mock_get.return_value = _s3_response(403, EXPIRED_TOKEN_BODY, AWS_HEADERS)

        result_df, upload = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        self.assertIsNone(result_df)
        self.assertIsNone(upload)
        mock_logger.error.assert_called_once()
        self.assertEqual(
            mock_logger.error.call_args.args,
            ("Failed to download dataframe from cache",),
        )
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["sql_caching_cause"], "failed_to_download_from_cache")
        self.assertEqual(extra["s3_error_code"], "ExpiredToken")
        self.assertEqual(extra["status_code"], 403)
        self.assertEqual(extra["aws_request_id"], "REQ123")
        self.assertEqual(extra["object_path"], "/ws/int/key")

    @parameterized.expand(
        [
            # the field allowlist is what keeps this one clean
            ("signature_mismatch_body", SIGNATURE_MISMATCH_BODY),
            # no allowlist applies here, so only redaction keeps it clean
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
        mock_get.return_value = _s3_response(403, body, AWS_HEADERS)

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
        mock_get.return_value = _s3_response(200, buffer.getvalue())

        result_df, _ = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        pd.testing.assert_frame_equal(result_df, dataframe)
        # the object was fetched here rather than by handing the URL to pandas
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
        mock_get.return_value = _s3_response(200, buffer.getvalue())

        result_df, _ = get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        # pins the seek(0) that the failed parquet read makes necessary
        pd.testing.assert_frame_equal(result_df, dataframe)
        # and the single fetch that replaced one download per format attempt
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
        mock_get.return_value = _s3_response(200, buffer.getvalue())

        get_sql_cache(QUERY, {}, "123", "read", "dataframe")

        # an unbounded read phase hangs the user's cell on a socket gone silent
        connect_timeout, read_timeout = mock_get.call_args.kwargs["timeout"]
        self.assertIsNotNone(connect_timeout)
        self.assertIsNotNone(read_timeout)
        # and an unstreamed error body costs the size of that body
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
        mock_get.return_value = _s3_response(503, b"upstream unavailable")

        self.assertIsNone(_request_cache_info_from_webapp("abc", "123", "read"))

        self.assertEqual(
            mock_logger.error.call_args.args, ("Failed to request cache info",)
        )
        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["status_code"], 503)
        self.assertEqual(extra["response_body_snippet"], "upstream unavailable")
        self.assertEqual(
            extra["cache_info_path"],
            "/userpod-api/p1/integrations/123/sql-cache",
        )
        self.assertNotIn("sqlCacheKey", " ".join(_logged_strings(mock_logger)))


class TestRedactSensitive(unittest.TestCase):
    def test_strips_query_string_from_urlopen_style_message(self):
        # the shape urllib3 actually produces: the URL is path-only
        redacted = _redact_sensitive(URLLIB3_ERROR_MESSAGE)

        self.assertIn("bucket.s3.eu-west-1.amazonaws.com", redacted)
        self.assertIn("/ws/int/key?<redacted>", redacted)
        for secret in SECRETS:
            self.assertNotIn(secret, redacted)

    def test_query_string_strip_alone_redacts_urllib3_message(self):
        """The primary defence must hold without help from the parameter backstop."""
        stripped = _URL_QUERY_PATTERN.sub(r"\1?<redacted>", URLLIB3_ERROR_MESSAGE)

        for secret in SECRETS:
            self.assertNotIn(secret, stripped)

    def test_blanks_aws_params_outside_a_url(self):
        redacted = _redact_sensitive(
            "X-Amz-Credential=CREDVALUE&X-Amz-Security-Token=TOKENVALUE"
            "&X-Amz-Signature=SIGVALUE"
        )

        self.assertEqual(
            redacted,
            "X-Amz-Credential=<redacted>&X-Amz-Security-Token=<redacted>"
            "&X-Amz-Signature=<redacted>",
        )

    @parameterized.expand(
        [
            ("question_in_prose", "Is this ok? Yes it is."),
            ("bare_question", "what? nothing"),
            ("plain_sentence", "The provided token has expired."),
        ]
    )
    def test_leaves_ordinary_text_unchanged(self, _, text):
        self.assertEqual(_redact_sensitive(text), text)


class TestDescribeException(unittest.TestCase):
    def test_large_message_is_bounded_and_redacted_in_bounded_time(self):
        """The message is cut before redacting, not after.

        _URL_QUERY_PATTERN backtracks over every start position in a long run of
        non-separator characters, so redacting an untruncated exception message is
        quadratic - 64k characters took 8.7s, charged to the user's cell.
        """
        message = URLLIB3_ERROR_MESSAGE + "&padding=" + "A" * 64_000

        started = time.monotonic()
        described = _describe_exception(ValueError(message))
        elapsed = time.monotonic() - started

        self.assertEqual(described["error_type"], "ValueError")
        self.assertLessEqual(len(described["error_message"]), 500)
        # cutting the query string short is still safe: the pattern runs to the
        # end of the string, so the remainder is stripped either way
        self.assertIn("/ws/int/key?<redacted>", described["error_message"])
        for secret in SECRETS:
            self.assertNotIn(secret, described["error_message"])
        self.assertLess(elapsed, 2.0)


class TestDescribeS3Error(unittest.TestCase):
    def test_extracts_code_and_message_from_xml(self):
        diagnostics = _describe_s3_error(
            _s3_response(403, ACCESS_DENIED_EXPIRED_BODY, AWS_HEADERS)
        )

        self.assertEqual(diagnostics["status_code"], 403)
        self.assertEqual(diagnostics["s3_error_code"], "AccessDenied")
        self.assertIn("Request has expired", diagnostics["s3_error_message"])
        # AWS's own account of when the URL died and what time it was
        self.assertEqual(diagnostics["s3_expires"], "2026-07-29T12:15:00Z")
        self.assertEqual(diagnostics["s3_server_time"], "2026-07-29T12:31:07Z")

    def test_extracts_expired_token_body(self):
        diagnostics = _describe_s3_error(_s3_response(403, EXPIRED_TOKEN_BODY))

        self.assertEqual(diagnostics["s3_error_code"], "ExpiredToken")
        self.assertEqual(
            diagnostics["s3_error_message"], "The provided token has expired."
        )

    def test_captures_aws_request_headers(self):
        diagnostics = _describe_s3_error(
            _s3_response(403, EXPIRED_TOKEN_BODY, AWS_HEADERS)
        )

        self.assertEqual(diagnostics["aws_request_id"], "REQ123")
        self.assertEqual(diagnostics["aws_host_id"], "HOSTID456")
        self.assertEqual(diagnostics["aws_date"], "Wed, 29 Jul 2026 12:31:07 GMT")

    def test_signature_mismatch_body_surfaces_only_code_and_message(self):
        """Pins the field allowlist: <CanonicalRequest> and friends are never read.

        Redaction is not what protects this body - the elements carrying the signed
        query string are simply not among the four that get extracted.
        """
        diagnostics = _describe_s3_error(
            _s3_response(403, SIGNATURE_MISMATCH_BODY, AWS_HEADERS)
        )

        self.assertEqual(diagnostics["s3_error_code"], "SignatureDoesNotMatch")
        self.assertNotIn("response_body_snippet", diagnostics)
        for value in diagnostics.values():
            for secret in SECRETS:
                self.assertNotIn(secret, str(value))

    def test_proxy_body_echoing_request_url_is_redacted(self):
        """Pins redaction: the allowlist cannot help once the body is not S3's."""
        diagnostics = _describe_s3_error(_s3_response(502, PROXY_ECHO_BODY))

        self.assertIsNone(diagnostics["s3_error_code"])
        snippet = diagnostics["response_body_snippet"]
        # the request line survives, the credentials on it do not
        self.assertIn("502 Bad Gateway", snippet)
        self.assertIn("/ws/int/key?<redacted>", snippet)
        for secret in SECRETS:
            self.assertNotIn(secret, snippet)

    def test_non_xml_body_yields_snippet_without_code(self):
        diagnostics = _describe_s3_error(
            _s3_response(502, b"<html><body>502 Bad Gateway</body></html>")
        )

        self.assertIsNone(diagnostics["s3_error_code"])
        self.assertIsNone(diagnostics["aws_request_id"])
        self.assertIn("502 Bad Gateway", diagnostics["response_body_snippet"])
        self.assertLessEqual(len(diagnostics["response_body_snippet"]), 500)

    def test_oversized_body_is_bounded(self):
        body = (
            b"<Error><Code>AccessDenied</Code><Message>"
            + b"x" * 1000
            + b"</Message>"
            + b"y" * 10_000
            + b"</Error>"
        )

        diagnostics = _describe_s3_error(_s3_response(403, body))

        self.assertEqual(diagnostics["s3_error_code"], "AccessDenied")
        for value in diagnostics.values():
            if isinstance(value, str):
                self.assertLessEqual(len(value), 500)

    def test_body_prefix_is_streamed_rather_than_buffered(self):
        """Reading .content downloads the whole body just to keep 4 KB of it."""
        buffered = []
        response = mock.MagicMock(status_code=403, headers={})
        type(response).content = mock.PropertyMock(
            side_effect=lambda: buffered.append("content")
        )
        response.iter_content.side_effect = lambda size: iter(
            [ACCESS_DENIED_EXPIRED_BODY[:size]]
        )

        diagnostics = _describe_s3_error(response)

        self.assertEqual(buffered, [])
        self.assertEqual(diagnostics["s3_error_code"], "AccessDenied")


class TestDescribePresignedUrl(unittest.TestCase):
    def test_returns_path_and_expiry(self):
        described = _describe_presigned_url(PRESIGNED_URL)

        self.assertEqual(described["object_host"], "bucket.s3.eu-west-1.amazonaws.com")
        self.assertEqual(described["object_path"], "/ws/int/key")
        self.assertEqual(described["url_expires_in"], 900)

    def test_missing_expires_yields_none(self):
        described = _describe_presigned_url("https://example.com/x")

        self.assertEqual(described["object_path"], "/x")
        self.assertIsNone(described["url_expires_in"])

    def test_non_numeric_expires_yields_none(self):
        described = _describe_presigned_url("https://example.com/x?X-Amz-Expires=abc")

        self.assertIsNone(described["url_expires_in"])

    @parameterized.expand(
        [
            ("unterminated_ipv6", "https://[::1"),
            ("empty", ""),
            ("not_a_url", "not a url"),
            ("none", None),
            ("bytes", b"/ws/int/key"),
            ("dict", {"url": "https://example.com/x"}),
        ]
    )
    def test_malformed_url_does_not_raise_or_leak(self, _, url):
        described = _describe_presigned_url(url)

        self.assertIsNone(described["url_expires_in"])
        for value in described.values():
            for secret in SECRETS:
                self.assertNotIn(secret, str(value))
        # a bytes value in either would silently discard the entire error report
        json.dumps(described)
        json.dumps(_safe_url_path(url))

    @parameterized.expand(
        [
            (
                "separator_encoded",
                "%3FX-Amz-Credential=CREDVALUE&X-Amz-Security-Token=TOKENVALUE"
                "&X-Amz-Signature=SIGVALUE",
            ),
            (
                "separator_and_equals_encoded",
                "%3FX-Amz-Credential%3DCREDVALUE&X-Amz-Security-Token%3DTOKENVALUE"
                "&X-Amz-Signature%3DSIGVALUE",
            ),
            (
                "whole_query_encoded",
                "%3FX-Amz-Credential%3DCREDVALUE%26X-Amz-Security-Token%3DTOKENVALUE"
                "%26X-Amz-Signature%3DSIGVALUE",
            ),
            ("separator_is_a_semicolon", ";X-Amz-Credential=CREDVALUE"),
            ("no_separator_at_all", "X-Amz-Signature=SIGVALUE"),
        ]
    )
    def test_over_encoded_url_does_not_leak_signing_params_via_path(self, _, suffix):
        """urlsplit only splits on a literal '?', so the path carries the rest."""
        described = _describe_presigned_url(
            "https://bucket.s3.eu-west-1.amazonaws.com/ws/int/key" + suffix
        )

        # the parameter names may survive redaction, their values must not
        for value in described.values():
            for secret in ("CREDVALUE", "TOKENVALUE", "SIGVALUE"):
                self.assertNotIn(secret, str(value))


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
        mock_put.return_value = _s3_response(
            403, ACCESS_DENIED_EXPIRED_BODY, AWS_HEADERS
        )

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        mock_logger.error.assert_called_once()
        self.assertEqual(
            mock_logger.error.call_args.args, ("Failed to upload SQL cache",)
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
            # the field allowlist is what keeps this one clean
            ("signature_mismatch_body", SIGNATURE_MISMATCH_BODY),
            # no allowlist applies here, so only redaction keeps it clean
            ("proxy_echoing_request_url", PROXY_ECHO_BODY),
        ]
    )
    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_http_error_logs_no_presigned_query_string(
        self, _name, body, mock_put, mock_logger
    ):
        mock_put.return_value = _s3_response(403, body, AWS_HEADERS)

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        mock_logger.error.assert_called_once()
        for logged in _logged_strings(mock_logger):
            for secret in SECRETS:
                self.assertNotIn(secret, logged)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_network_error_logs_no_presigned_url(self, mock_put, mock_logger):
        mock_put.side_effect = requests.exceptions.ConnectionError(
            URLLIB3_ERROR_MESSAGE
        )

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["sql_caching_cause"], "failed_to_upload_to_cache")
        self.assertEqual(extra["error_type"], "ConnectionError")
        for logged in _logged_strings(mock_logger):
            for secret in SECRETS:
                self.assertNotIn(secret, logged)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_message_is_constant_across_different_failures(self, mock_put, mock_logger):
        df = pd.DataFrame({"a": [1, 2, 3]})
        other_url = PRESIGNED_URL.replace("/ws/int/key", "/other/int/key2")

        mock_put.return_value = _s3_response(403, ACCESS_DENIED_EXPIRED_BODY)
        upload_sql_cache(df, _upload())
        mock_put.return_value = _s3_response(500, b"<Error><Code>Slow</Code></Error>")
        upload_sql_cache(df, _upload(other_url))
        mock_put.side_effect = requests.exceptions.ConnectionError(
            URLLIB3_ERROR_MESSAGE
        )
        upload_sql_cache(df, _upload())
        upload_sql_cache(df, _upload(other_url))

        messages = {call.args for call in mock_logger.error.call_args_list}
        self.assertEqual(messages, {("Failed to upload SQL cache",)})

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
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_seconds_since_url_issued_reflects_elapsed_time(
        self, mock_put, mock_logger
    ):
        mock_put.return_value = _s3_response(403, ACCESS_DENIED_EXPIRED_BODY)

        upload_sql_cache(
            pd.DataFrame({"a": [1, 2, 3]}),
            _upload(issued_at=time.monotonic() - 930),
        )

        extra = mock_logger.error.call_args.kwargs["extra"]
        # the entry alone shows the URL was used after its window closed
        self.assertGreaterEqual(extra["seconds_since_url_issued"], 930)
        self.assertEqual(extra["url_expires_in"], 900)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.tempfile.TemporaryFile")
    def test_failure_before_the_request_leaves_both_times_unset(
        self, mock_temp_file, mock_logger
    ):
        """Nothing was timed because nothing was sent, and neither may raise."""
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
            return _s3_response(500, b"<Error><Code>InternalError</Code></Error>")

        mock_put.side_effect = slow_put

        upload_sql_cache(
            pd.DataFrame({"a": [1, 2, 3]}),
            SqlCacheUpload(url=PRESIGNED_URL, issued_at=100.5),
        )

        extra = mock_logger.error.call_args.kwargs["extra"]
        # the request left inside the 900s window, so the slow transfer that
        # followed must not make the entry read as an expired URL
        self.assertEqual(extra["seconds_since_url_issued"], 899.5)
        self.assertEqual(extra["upload_duration_seconds"], 1000.0)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_upload_request_bounds_connect_and_read_phases(self, mock_put, mock_logger):
        mock_put.return_value = _s3_response(200)

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        connect_timeout, read_timeout = mock_put.call_args.kwargs["timeout"]
        self.assertIsNotNone(connect_timeout)
        self.assertIsNotNone(read_timeout)

    @patch("deepnote_toolkit.sql.sql_caching.logger")
    @patch("deepnote_toolkit.sql.sql_caching.requests.put")
    def test_upload_timeout_never_raises(self, mock_put, mock_logger):
        mock_put.side_effect = requests.exceptions.Timeout(URLLIB3_ERROR_MESSAGE)

        upload_sql_cache(pd.DataFrame({"a": [1, 2, 3]}), _upload())

        extra = mock_logger.error.call_args.kwargs["extra"]
        self.assertEqual(extra["error_type"], "Timeout")


class TestLoggedExtras(unittest.TestCase):
    """Guards that apply to every extra dict this module logs."""

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
        """A reserved key makes logger.error raise into the user's query."""
        for extra in self.extras:
            self.assertEqual(set(extra) & RESERVED_LOGRECORD_ATTRS, set())

    def test_extra_is_json_serializable(self):
        """A non-serializable value silently discards the whole error report."""
        for extra in self.extras:
            for value in extra.values():
                self.assertIsInstance(value, (str, int, float, bool, type(None)))
            json.dumps(extra)
