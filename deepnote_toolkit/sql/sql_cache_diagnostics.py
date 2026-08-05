"""Log-safe descriptions of SQL cache failures.

Everything here exists so a failed cache request can be explained in a log entry
without that entry carrying the presigned URL's signing parameters with it.
"""

import re
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

import requests

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
# The only elements ever read out of an S3 error document, mapped to the log
# field each becomes. The rest of the document (<CanonicalRequest>,
# <StringToSign>, <AWSAccessKeyId>) echoes the signed query string and with it
# the credentials, so nothing outside this table is touched.
# <Expires> and <ServerTime> are AWS's own answer to whether the URL ran out of
# time, independent of this pod's clock.
_S3_ERROR_FIELDS = {
    "Code": "s3_error_code",
    "Message": "s3_error_message",
    "Expires": "s3_expires",
    "ServerTime": "s3_server_time",
}

_S3_ERROR_FIELD_PATTERN = re.compile(
    rf"<({'|'.join(_S3_ERROR_FIELDS)})>(.*?)</\1>", re.DOTALL | re.IGNORECASE
)


class SqlCacheHttpError(Exception):
    """A non-2xx response from the cache object store.

    Its own string representation deliberately contains no URL.
    """

    def __init__(self, diagnostics: dict[str, Any]) -> None:
        """Store log-safe diagnostics already taken from the failed response."""
        super().__init__("SQL cache object store returned an error response")
        self.diagnostics = diagnostics


def redact_sensitive(text: str) -> str:
    """Strip credential-bearing material from text destined for a log."""
    redacted = _URL_QUERY_PATTERN.sub(r"\1?<redacted>", text)
    return _AWS_CREDENTIAL_PARAM_PATTERN.sub(r"\1<redacted>", redacted)


def _redacted_snippet(text: str) -> str:
    """Redact text and cut it to what a single log field may carry."""
    return redact_sensitive(text)[:_MAX_ERROR_FIELD_CHARS]


def _to_int_or_none(value: Optional[str]) -> Optional[int]:
    """Coerce a query string value to an int, or None when it is not numeric."""
    if value is None:
        return None

    try:
        return int(value)
    except ValueError:
        return None


def seconds_between(start: Optional[float], end: Optional[float]) -> Optional[float]:
    """Monotonic seconds from start to end, or None when either was not taken."""
    if start is None or end is None:
        return None

    return round(end - start, 1)


def safe_url_path(url: object) -> Optional[str]:
    """The bounded path of a URL, or None when the URL cannot be parsed."""
    if not isinstance(url, str):
        return None

    try:
        return urlsplit(url).path[:_MAX_OBJECT_PATH_CHARS]
    except Exception:
        return None


def describe_presigned_url(url: object) -> dict[str, Any]:
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
            "object_path": redact_sensitive(path[:_MAX_OBJECT_PATH_CHARS]),
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


def read_body_snippet(response: requests.Response) -> Optional[str]:
    """A log-safe prefix of a response body, or None when it is unreadable."""
    body = _read_response_body_prefix(response)
    return None if body is None else _redacted_snippet(body)


def describe_s3_error(response: requests.Response) -> dict[str, Any]:
    """Non-sensitive diagnostics from a failed response from the object store.

    Only the elements in _S3_ERROR_FIELDS are read out of the body.
    """
    diagnostics: dict[str, Any] = {
        "status_code": response.status_code,
        # Needed if we ever have to ask AWS Support about a specific request
        "aws_request_id": response.headers.get("x-amz-request-id"),
        "aws_host_id": response.headers.get("x-amz-id-2"),
        # AWS's clock at the moment it rejected us
        "aws_date": response.headers.get("Date"),
        **{field: None for field in _S3_ERROR_FIELDS.values()},
    }

    body = _read_response_body_prefix(response)
    if body is None:
        return diagnostics

    found = {
        element.lower(): value
        for element, value in _S3_ERROR_FIELD_PATTERN.findall(body)
    }
    for element, field in _S3_ERROR_FIELDS.items():
        value = found.get(element.lower())
        if value is not None:
            diagnostics[field] = _redacted_snippet(value)

    if diagnostics["s3_error_code"] is None:
        # Not an S3 error document - a proxy or gateway answered instead
        diagnostics["response_body_snippet"] = _redacted_snippet(body)

    return diagnostics


def describe_exception(exc: BaseException) -> dict[str, Any]:
    """Non-sensitive diagnostics from an exception raised while using the cache."""
    if isinstance(exc, SqlCacheHttpError):
        return dict(exc.diagnostics)

    # raise_for_status() attaches the response, so the error document is still
    # there to be read - unlike its own message, which is just the status line
    # and the whole presigned URL
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return describe_s3_error(exc.response)

    # Cut before redacting rather than after: the URL pattern backtracks over every
    # start position, and an exception message has no bound of its own
    return {
        "error_type": type(exc).__name__,
        "error_message": _redacted_snippet(str(exc)[:_MAX_RAW_EXCEPTION_CHARS]),
    }
