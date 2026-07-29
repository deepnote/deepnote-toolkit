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

# Only the connect phase is bounded. A read timeout would abort slow but healthy
# transfers of large cache objects, which would be a regression.
_OBJECT_STORE_TIMEOUT: tuple[int, None] = (5, None)

# Bounds on every piece of third-party text that can reach a log entry
_MAX_ERROR_BODY_BYTES = 4096
_MAX_ERROR_FIELD_CHARS = 500
_MAX_OBJECT_PATH_CHARS = 200
# An exception message is the one input that is not already bounded by the time it
# is redacted, so it is cut first. The margin over _MAX_ERROR_FIELD_CHARS leaves
# room for the text to grow as placeholders replace what they redact.
_MAX_RAW_EXCEPTION_CHARS = _MAX_ERROR_FIELD_CHARS * 4

# Strips the query string off anything URL-shaped, including the scheme-less,
# path-only form that urllib3 puts in its connection error messages. The character
# classes exclude whitespace and angle brackets, so a match can never span an XML
# tag boundary or swallow ordinary prose containing a question mark.
_URL_QUERY_PATTERN = re.compile(
    r"((?:https?://)?[^\s?\"'<>]*/[^\s?\"'<>]*)\?[^\s\"'<>]*"
)
# Backstop for signing parameters that appear without a URL in front of them, as
# they do in the canonical request echoed by an S3 SignatureDoesNotMatch body
_AWS_CREDENTIAL_PARAM_PATTERN = re.compile(
    r"(X-(?:Amz|Goog)-(?:Credential|Security-Token|Signature)=)[^&\s\"'<>]*",
    re.IGNORECASE,
)
# S3 states the real cause of a failure in an XML body. <Expires> and <ServerTime>
# accompany a rejection for an elapsed signature and are AWS's own answer to
# whether the URL ran out of time, independent of this pod's clock.
_S3_ERROR_FIELD_PATTERN = re.compile(
    r"<(Code|Message|Expires|ServerTime)>(.*?)</\1>", re.DOTALL | re.IGNORECASE
)


class SqlCacheUpload(NamedTuple):
    """A presigned upload URL together with the moment it was issued.

    The URL is obtained before the query runs and used only after the query
    completes and its result is serialized, so all of that has to fit inside the
    URL's signed lifetime. Carrying issued_at is what makes that measurable.
    """

    url: str
    # time.monotonic() taken just before the URL was requested from the webapp
    issued_at: float


class _SqlCacheHttpError(Exception):
    """A non-2xx response from the cache object store.

    Carries pre-computed, non-sensitive diagnostics for the caller to log. Its own
    string representation deliberately contains no URL.
    """

    def __init__(self, diagnostics: dict[str, Any]) -> None:
        """Store diagnostics already extracted from the failed response.

        Args:
            diagnostics (dict): Log-safe fields describing the response.
        """
        super().__init__("SQL cache object store returned an error response")
        self.diagnostics = diagnostics


def _redact_sensitive(text: str) -> str:
    """Strip credential-bearing material from text destined for a log.

    Dropping the query string from anything URL-shaped is the primary defence:
    presigned URLs carry X-Amz-Credential and X-Amz-Security-Token there, and
    urllib3 embeds the requested path and its query in every connection error.
    Blanking named signing parameters is a backstop for the same values appearing
    without a URL in front of them.
    """
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


def _seconds_since(issued_at: float) -> Optional[float]:
    """Monotonic seconds elapsed since issued_at, or None if it is not a number.

    Guarded because it is evaluated while building a log entry, in a function
    documented as never failing the user's query.
    """
    try:
        return round(time.monotonic() - issued_at, 1)
    except Exception:
        return None


def _safe_url_path(url: str) -> Optional[str]:
    """The bounded path of a URL, or None when the URL cannot be parsed.

    The query string is never returned, and neither is the input itself when
    urlsplit rejects it.
    """
    if not isinstance(url, str):
        # urlsplit(None) answers with bytes-valued fields instead of raising, and a
        # bytes value in extra silently discards the whole error report
        return None

    try:
        return urlsplit(url).path[:_MAX_OBJECT_PATH_CHARS]
    except Exception:
        return None


def _describe_presigned_url(url: str) -> dict[str, Any]:
    """Object path and declared expiry, without the credential-bearing query string.

    The query string of a presigned URL carries X-Amz-Credential and
    X-Amz-Security-Token and must never reach a log, so it is never returned - not
    even when the URL cannot be parsed.
    """
    if not isinstance(url, str):
        # urlsplit(None) answers with bytes-valued fields instead of raising, and a
        # bytes value in extra silently discards the whole error report
        return {"object_host": None, "object_path": None, "url_expires_in": None}

    try:
        parts = urlsplit(url)
        expires_values = parse_qs(parts.query).get("X-Amz-Expires")
        return {
            # hostname rather than netloc, which would carry any user:pass@ userinfo
            "object_host": parts.hostname,
            "object_path": parts.path[:_MAX_OBJECT_PATH_CHARS],
            "url_expires_in": _to_int_or_none(
                expires_values[0] if expires_values else None
            ),
        }
    except Exception:
        # Built purely from literals, so an unparseable URL cannot leak through
        # the failure path either
        return {"object_host": None, "object_path": None, "url_expires_in": None}


def _read_response_body_prefix(response: requests.Response) -> Optional[str]:
    """Decode a bounded prefix of a response body, or None when it is unreadable.

    The prefix is sliced off the raw bytes before decoding, so an oversized or
    mis-declared body costs nothing to inspect and binary content cannot raise.
    """
    try:
        return response.content[:_MAX_ERROR_BODY_BYTES].decode(
            "utf-8", errors="replace"
        )
    except Exception:
        # A truncated or badly encoded transfer. The caller still has the status
        # code, which is worth logging on its own.
        return None


def _describe_s3_error(response: requests.Response) -> dict[str, Any]:
    """Non-sensitive diagnostics from a failed response from the object store.

    Only <Code>, <Message>, <Expires> and <ServerTime> are taken from the body,
    because the other elements of an S3 error document (<CanonicalRequest>,
    <StringToSign>, <AWSAccessKeyId>) echo the signed query string and with it the
    credentials this module must not log.
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
        # Not an S3 error document - a proxy or gateway answered instead. Keep a
        # short redacted snippet so that case stays diagnosable.
        diagnostics["response_body_snippet"] = _redact_sensitive(body)[
            :_MAX_ERROR_FIELD_CHARS
        ]

    return diagnostics


def _describe_exception(exc: BaseException) -> dict[str, Any]:
    """Non-sensitive diagnostics from an exception raised while using the cache."""
    if isinstance(exc, _SqlCacheHttpError):
        return dict(exc.diagnostics)

    # Cut before redacting rather than after: the URL pattern backtracks over every
    # start position in a long run of non-separator characters, and an exception
    # message has no bound of its own. Cutting a query string short is safe, since
    # the pattern consumes to the end of the string either way.
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
    # inside this round trip, which makes it a conservative upper bound on how much
    # of the URL's lifetime has been spent by the time we come to use it
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

    Args:
        dataframe (pd.DataFrame): The query result to cache.
        upload (SqlCacheUpload): The presigned upload URL and when it was issued.
    """

    try:
        with tempfile.TemporaryFile() as temp_file:
            try:
                _serialize_dataframe_for_cache(dataframe, temp_file)
            except Exception as exc:
                # Nothing was sent, so this is not an upload failure and gets its own
                # cause. Only the type is logged: serialization errors quote the
                # user's column names.
                logger.error(
                    "Failed to upload SQL cache",
                    extra={
                        "sql_caching_cause": "failed_to_serialize_cache",
                        "error_type": type(exc).__name__,
                    },
                )
                return

            temp_file.seek(0)
            # PUT the file to the pre-signed s3 url
            response = requests.put(
                upload.url, data=temp_file, timeout=_OBJECT_STORE_TIMEOUT
            )

        if response.status_code < 400:
            return

        # Checked explicitly rather than with raise_for_status(), whose message
        # interpolates the whole presigned URL and discards S3's error body
        failure = _describe_s3_error(response)
    except Exception as exc:
        # The request never completed, so there is no response to describe
        failure = _describe_exception(exc)

    logger.error(
        "Failed to upload SQL cache",
        extra={
            "sql_caching_cause": "failed_to_upload_to_cache",
            **failure,
            **_describe_presigned_url(upload.url),
            "seconds_since_url_issued": _seconds_since(upload.issued_at),
        },
    )


def _try_read_cache(download_url: str) -> pd.DataFrame:
    """Download the cached object and read it as a dataframe.

    The object is fetched explicitly instead of handing the URL to pandas, which
    fetches it with urllib and so raises before S3's error body is ever read.
    """
    response = requests.get(download_url, timeout=_OBJECT_STORE_TIMEOUT)
    if response.status_code >= 400:
        raise _SqlCacheHttpError(_describe_s3_error(response))

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

    # The failed parquet read left the cursor part-way through the buffer. Reading
    # from the URL used to start a fresh download for each attempt.
    buffer.seek(0)
    return pd.read_pickle(buffer)


def _generate_cache_key(query, bind_params):
    return hashlib.sha256(
        (query + json.dumps(bind_params, sort_keys=True, default=str)).encode("utf-8")
    ).hexdigest()


def _request_cache_info_from_webapp(
    query_hash: str, integration_id: str, sql_cache_mode: str
) -> Optional[dict[str, Any]]:
    """Ask the webapp what the cache holds for this query.

    Args:
        query_hash (str): The cache key derived from the query and its params.
        integration_id (str): The integration ID associated with the cache.
        sql_cache_mode (str): The mode of the SQL cache.

    Returns:
        The cache info, or None when caching is disabled or unavailable.
    """
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
