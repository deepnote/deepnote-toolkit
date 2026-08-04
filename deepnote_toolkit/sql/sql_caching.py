import hashlib
import json
import re
import tempfile
import time
from io import BytesIO
from typing import IO, Any, NamedTuple, Optional
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import requests
from pyarrow import ArrowInvalid, ArrowNotImplementedError

from deepnote_toolkit.sql.sql_utils import is_single_select_query

from ..get_webapp_url import get_absolute_userpod_api_url, get_project_auth_headers
from ..ipython_utils import output_sql_metadata
from ..logging import get_logger

# Initialize logger
logger = get_logger()

# The read timeout bounds the gap between two received chunks, not the transfer as
# a whole, so it cannot abort a large but healthy download.
_OBJECT_STORE_TIMEOUT: tuple[int, int] = (5, 60)

_MAX_ERROR_BODY_BYTES = 4096
_MAX_ERROR_FIELD_CHARS = 500
_MAX_OBJECT_PATH_CHARS = 200
_MAX_RAW_EXCEPTION_CHARS = _MAX_ERROR_FIELD_CHARS * 4

# Matches the scheme-less, path-only form that urllib3 puts in its connection
# error messages as well as a whole URL
_URL_QUERY_PATTERN = re.compile(
    r"((?:https?://)?[^\s?\"'<>]*/[^\s?\"'<>]*)\?[^\s\"'<>]*"
)
# Backstop for signing parameters that appear without a URL in front of them, as
# they do in the canonical request echoed by an S3 SignatureDoesNotMatch body
_AWS_CREDENTIAL_PARAM_PATTERN = re.compile(
    r"(X-(?:Amz|Goog)-(?:Credential|Security-Token|Signature)=)[^&\s\"'<>]*",
    re.IGNORECASE,
)
# urlsplit only splits on a literal '?', so an over-encoded URL keeps its signed
# query string in the path
_ENCODED_QUERY_SEPARATOR = re.compile("%3F", re.IGNORECASE)
# <Expires> and <ServerTime> are AWS's own answer to whether the URL ran out of
# time, independent of this pod's clock
_S3_ERROR_FIELD_PATTERN = re.compile(
    r"<(Code|Message|Expires|ServerTime)>(.*?)</\1>", re.DOTALL | re.IGNORECASE
)


class SqlCacheUpload(NamedTuple):
    """A presigned upload URL together with the moment it was issued."""

    url: str
    issued_at: float


class _SqlCacheHttpError(Exception):
    """A non-2xx response from the cache object store.

    Its own string representation deliberately contains no URL.
    """

    def __init__(self, diagnostics: dict[str, Any]) -> None:
        """Store log-safe diagnostics already taken from the failed response."""
        super().__init__("SQL cache object store returned an error response")
        self.diagnostics = diagnostics


def _redact_sensitive(text: str) -> str:
    """Strip credential-bearing material from text destined for a log."""
    redacted = _URL_QUERY_PATTERN.sub(r"\1?<redacted>", text)
    return _AWS_CREDENTIAL_PARAM_PATTERN.sub(r"\1<redacted>", redacted)


def _to_int_or_none(value: Optional[str]) -> Optional[int]:
    """Coerce a query string value to an int, or None when it is not numeric."""
    if value is None:
        return None

    try:
        return int(value)
    except ValueError:
        return None


def _seconds_between(start: Optional[float], end: Optional[float]) -> Optional[float]:
    """Monotonic seconds from start to end, or None when either was not taken."""
    if start is None or end is None:
        return None

    return round(end - start, 1)


def _safe_url_path(url: str) -> Optional[str]:
    """The bounded path of a URL, or None when the URL cannot be parsed."""
    if not isinstance(url, str):
        return None

    try:
        return urlsplit(url).path[:_MAX_OBJECT_PATH_CHARS]
    except Exception:
        return None


def _describe_presigned_url(url: str) -> dict[str, Any]:
    """Object path and declared expiry, without the credential-bearing query string."""
    if not isinstance(url, str):
        # urlsplit(None) answers with bytes-valued fields instead of raising, and a
        # bytes value in extra silently discards the whole error report
        return {"object_host": None, "object_path": None, "url_expires_in": None}

    try:
        parts = urlsplit(url)
        expires_values = parse_qs(parts.query).get("X-Amz-Expires")
        path = _ENCODED_QUERY_SEPARATOR.split(parts.path, maxsplit=1)[0]
        return {
            # hostname rather than netloc, which would carry any user:pass@ userinfo
            "object_host": parts.hostname,
            # cut for an encoded '?', redacted for separators urlsplit ignores
            "object_path": _redact_sensitive(path[:_MAX_OBJECT_PATH_CHARS]),
            "url_expires_in": _to_int_or_none(
                expires_values[0] if expires_values else None
            ),
        }
    except Exception:
        return {"object_host": None, "object_path": None, "url_expires_in": None}


def _read_response_body_prefix(response: requests.Response) -> Optional[str]:
    """Decode a bounded prefix of a response body, or None when it is unreadable.

    Taken off the wire rather than sliced off response.content, which would
    download the whole body first, and accumulated rather than read once, because
    a chunked response answers with one HTTP chunk per read however large a size
    is asked for.
    """
    try:
        chunks = []
        length = 0
        for chunk in response.iter_content(_MAX_ERROR_BODY_BYTES):
            chunks.append(chunk)
            length += len(chunk)
            if length >= _MAX_ERROR_BODY_BYTES:
                break

        prefix = b"".join(chunks)[:_MAX_ERROR_BODY_BYTES]
        return prefix.decode("utf-8", errors="replace")
    except Exception:
        return None


def _describe_s3_error(response: requests.Response) -> dict[str, Any]:
    """Non-sensitive diagnostics from a failed response from the object store.

    Only the allowlisted fields are taken from the body, because the other
    elements of an S3 error document (<CanonicalRequest>, <StringToSign>,
    <AWSAccessKeyId>) echo the signed query string and with it the credentials.
    """
    diagnostics: dict[str, Any] = {
        "status_code": response.status_code,
        # Needed if we ever have to ask AWS Support about a specific request
        "aws_request_id": response.headers.get("x-amz-request-id"),
        "aws_host_id": response.headers.get("x-amz-id-2"),
        # AWS's clock at the moment it rejected us
        "aws_date": response.headers.get("Date"),
        "s3_error_code": None,
        "s3_error_message": None,
        "s3_expires": None,
        "s3_server_time": None,
    }

    body = _read_response_body_prefix(response)
    if body is None:
        return diagnostics

    fields = {
        name.lower(): value for name, value in _S3_ERROR_FIELD_PATTERN.findall(body)
    }
    for key, name in (
        ("s3_error_code", "code"),
        ("s3_error_message", "message"),
        ("s3_expires", "expires"),
        ("s3_server_time", "servertime"),
    ):
        value = fields.get(name)
        if value is not None:
            diagnostics[key] = _redact_sensitive(value)[:_MAX_ERROR_FIELD_CHARS]

    if diagnostics["s3_error_code"] is None:
        # Not an S3 error document - a proxy or gateway answered instead
        diagnostics["response_body_snippet"] = _redact_sensitive(body)[
            :_MAX_ERROR_FIELD_CHARS
        ]

    return diagnostics


def _describe_exception(exc: BaseException) -> dict[str, Any]:
    """Non-sensitive diagnostics from an exception raised while using the cache."""
    if isinstance(exc, _SqlCacheHttpError):
        return dict(exc.diagnostics)

    # Cut before redacting rather than after: the URL pattern backtracks over every
    # start position, and an exception message has no bound of its own
    message = _redact_sensitive(str(exc)[:_MAX_RAW_EXCEPTION_CHARS])
    return {
        "error_type": type(exc).__name__,
        "error_message": message[:_MAX_ERROR_FIELD_CHARS],
    }


def get_sql_cache(
    query: str,
    bind_params: dict,
    integration_id: str,
    sql_cache_mode: str,
    return_variable_type: str,
) -> tuple[Optional[pd.DataFrame], Optional[SqlCacheUpload]]:
    """
    Retrieves the SQL cache from webapp for a given query.

    Args:
        query (str): The SQL query to retrieve the cache for.
        bind_params (dict): The bind parameters for the SQL query.
        integration_id (str): The integration ID associated with the cache.
        sql_cache_mode (str): The mode of the SQL cache.
        return_variable_type (str): The type of variable the result is bound to.

    Returns:
        tuple: A tuple containing the cached dataframe (if available) and the
            pending upload (if applicable).
    """

    if not is_single_select_query(query):
        # we only cache single select queries
        output_sql_metadata(
            {
                "status": "cache_not_supported_for_query",
                # We don't include the additional metadata as the query hasn't been executed/read from cache
            }
        )
        return None, None

    query_hash = _generate_cache_key(query, bind_params)

    # Taken before the request because the webapp signs the upload URL somewhere
    # inside this round trip, which makes it a conservative upper bound
    requested_at = time.monotonic()

    cache_info = None
    try:
        cache_info = _request_cache_info_from_webapp(
            query_hash, integration_id, sql_cache_mode
        )
    except Exception as exc:
        # we failed to request the cache info from the webapp
        logger.error(
            "Failed to request SQL cache info",
            extra={
                "sql_caching_cause": "failed_to_request_cache_info",
                **_describe_exception(exc),
            },
        )
        return None, None

    if cache_info is not None:
        if cache_info["result"] == "cacheHit":
            download_url = cache_info["downloadUrl"]
            dataframe_from_cache = None
            try:
                dataframe_from_cache = _try_read_cache(download_url)
            except Exception as exc:
                # we failed to download the dataframe from the cache
                logger.error(
                    "Failed to download dataframe from cache",
                    extra={
                        "sql_caching_cause": "failed_to_download_from_cache",
                        **_describe_exception(exc),
                        **_describe_presigned_url(download_url),
                    },
                )
                return None, None

            if dataframe_from_cache is not None:
                output_sql_metadata(
                    {
                        "status": "read_from_cache_success",
                        "cache_created_at": cache_info["cacheCreatedAt"],
                        "compiled_query": query,
                        "variable_type": return_variable_type,
                        "integration_id": integration_id,
                    }
                )
                return dataframe_from_cache, None

        if cache_info["result"] == "cacheMiss" or cache_info["result"] == "alwaysWrite":
            return None, SqlCacheUpload(
                url=cache_info["uploadUrl"], issued_at=requested_at
            )

    return None, None


def _serialize_dataframe_for_cache(
    dataframe: pd.DataFrame, file_obj: IO[bytes]
) -> None:
    """Write the dataframe to file_obj as parquet, falling back to pickle."""
    try:
        dataframe.to_parquet(file_obj)
    except (ArrowNotImplementedError, ArrowInvalid, OverflowError):
        # see NB-1684
        # we fallback to pickle if parquet serialization fails (which will throw either of first 2 errors)
        # OverflowError: PyArrow raises this for Python int / Decimal values exceeding int64 range
        file_obj.seek(0)
        file_obj.truncate()
        dataframe.to_pickle(file_obj)


def upload_sql_cache(dataframe: pd.DataFrame, upload: SqlCacheUpload) -> None:
    """Upload the result to the cache as a parquet file.

    Caching is best effort: every failure is logged under a constant message, with
    all variable data in extra so occurrences group, and then swallowed so that it
    can never fail the user's query.
    """

    put_started_at: Optional[float] = None
    try:
        with tempfile.TemporaryFile() as temp_file:
            try:
                _serialize_dataframe_for_cache(dataframe, temp_file)
            except Exception as exc:
                # Only the type is logged: serialization errors quote the user's
                # column names
                logger.error(
                    "Failed to upload SQL cache",
                    extra={
                        "sql_caching_cause": "failed_to_serialize_cache",
                        "error_type": type(exc).__name__,
                    },
                )
                return

            temp_file.seek(0)
            # S3 checks a presigned signature when the request arrives rather than
            # when it finishes, so this is the age that decided whether it was valid
            put_started_at = time.monotonic()
            response = requests.put(
                upload.url, data=temp_file, timeout=_OBJECT_STORE_TIMEOUT
            )

        if response.status_code < 400:
            return

        # Checked explicitly rather than with raise_for_status(), whose message
        # interpolates the whole presigned URL and discards S3's error body
        failure = _describe_s3_error(response)
    except Exception as exc:
        failure = _describe_exception(exc)

    logger.error(
        "Failed to upload SQL cache",
        extra={
            "sql_caching_cause": "failed_to_upload_to_cache",
            **failure,
            **_describe_presigned_url(upload.url),
            "seconds_since_url_issued": _seconds_between(
                upload.issued_at, put_started_at
            ),
            # Separate field so "the URL was already old" stays distinguishable
            # from "the transfer was slow"
            "upload_duration_seconds": _seconds_between(
                put_started_at, time.monotonic()
            ),
        },
    )


def _try_read_cache(download_url: str) -> pd.DataFrame:
    """Download the cached object and read it as a dataframe.

    The object is fetched explicitly instead of handing the URL to pandas, which
    fetches it with urllib and so raises before S3's error body is ever read.
    """
    # Streamed so that a failed download costs a bounded prefix rather than the
    # whole error body, and so the rest of it is dropped with the connection
    with requests.get(
        download_url, timeout=_OBJECT_STORE_TIMEOUT, stream=True
    ) as response:
        if response.status_code >= 400:
            raise _SqlCacheHttpError(_describe_s3_error(response))

        # Read inside the block: once it exits, the body reads back as empty
        # rather than raising, which would cache-miss every hit in silence
        buffer = BytesIO(response.content)

    try:
        # Attempt to read as a parquet file
        return pd.read_parquet(buffer)
    except ArrowInvalid:
        # ArrowInvalid means that the file at download_url is not a parquet file.
        # We fallback to the pickle format if that happens, because the cache should either be in parquet or
        # pickle format and we don't know which one it is, the file has no extension.
        # (see .to_pickle fallback in upload_sql_cache)
        pass

    buffer.seek(0)
    return pd.read_pickle(buffer)


def _generate_cache_key(query, bind_params):
    return hashlib.sha256(
        (query + json.dumps(bind_params, sort_keys=True, default=str)).encode("utf-8")
    ).hexdigest()


def _request_cache_info_from_webapp(
    query_hash: str, integration_id: str, sql_cache_mode: str
) -> Optional[dict[str, Any]]:
    """The cache info for this query, or None when caching is off or unavailable."""
    # calls https://github.com/deepnote/deepnote/blob/eb96467937de12db8b588e5aa0a80244cec7eae7/apps/webapp/server/api/userpod-api.ts#L133
    sql_cache_url = get_absolute_userpod_api_url(
        f"integrations/{integration_id}/sql-cache?sqlCacheKey={query_hash}&sqlCacheMode={sql_cache_mode}"
    )

    # Add project credentials in detached mode
    headers = get_project_auth_headers()

    timeout_in_seconds = 5
    sql_cache_response = requests.get(
        sql_cache_url, timeout=timeout_in_seconds, headers=headers
    )
    if sql_cache_response.status_code != 200:
        # the caching endpoint is not available, we can't use it. We'll skip the caching logic
        body = _read_response_body_prefix(sql_cache_response)
        snippet = (
            None if body is None else _redact_sensitive(body)[:_MAX_ERROR_FIELD_CHARS]
        )
        logger.error(
            "Failed to request cache info",
            extra={
                "sql_caching_cause": "http_error",
                "status_code": sql_cache_response.status_code,
                "cache_info_path": _safe_url_path(sql_cache_url),
                "response_body_snippet": snippet,
            },
        )
        return None

    result_dict = sql_cache_response.json()
    if result_dict["result"] == "sqlCachingDisabled":
        return None

    return result_dict
