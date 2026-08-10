"""Log-safe descriptions of SQL cache failures.

Failed cache requests are described without presigned URL signing parameters.
"""

import re
import xml.etree.ElementTree as ET
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit

import requests

_MAX_ERROR_BODY_BYTES = 4096
_MAX_ERROR_FIELD_CHARS = 500
_MAX_OBJECT_PATH_CHARS = 500
_MAX_RAW_EXCEPTION_CHARS = _MAX_ERROR_FIELD_CHARS * 4

# urllib3 connection errors use path-only URLs as well as full URLs
_URL_QUERY_PATTERN = re.compile(
    r"((?:https?://)?[^\s?\"'<>]*/[^\s?\"'<>]*)\?[^\s\"'<>]*"
)
# SignatureDoesNotMatch bodies echo params outside any URL prefix
_AWS_CREDENTIAL_PARAM_PATTERN = re.compile(
    r"(X-(?:Amz|Goog)-(?:Credential|Security-Token|Signature)=)[^&\s\"'<>]*",
    re.IGNORECASE,
)
# urlsplit only splits on a literal '?'
_ENCODED_QUERY_SEPARATOR = re.compile("%3F", re.IGNORECASE)
# Parsing stops at this allowlist; other S3 error elements embed the signed query.
_S3_ERROR_FIELDS = {
    "Code": "s3_error_code",
    "Message": "s3_error_message",
    "Expires": "s3_expires",
    "ServerTime": "s3_server_time",
}


class SqlCacheHttpError(Exception):
    """Non-2xx from the cache object store; str() carries no URL."""

    def __init__(self, diagnostics: dict[str, Any]) -> None:
        super().__init__("SQL cache object store returned an error response")
        self.diagnostics = diagnostics


def redact_sensitive(text: str) -> str:
    """Remove credential-bearing material from text destined for logs."""
    redacted = _URL_QUERY_PATTERN.sub(r"\1?<redacted>", text)
    return _AWS_CREDENTIAL_PARAM_PATTERN.sub(r"\1<redacted>", redacted)


def _redacted_snippet(text: str) -> str:
    return redact_sensitive(text)[:_MAX_ERROR_FIELD_CHARS]


def seconds_between(start: Optional[float], end: Optional[float]) -> Optional[float]:
    """Elapsed monotonic seconds, or None if either timestamp is missing."""
    if start is None or end is None:
        return None

    return round(end - start, 1)


def safe_int(value: Optional[str]) -> Optional[int]:
    """Convert a string to an integer, or return None if conversion fails."""
    if value is None:
        return None

    try:
        return int(value)
    except ValueError:
        return None


def safe_url_path(url: object) -> Optional[str]:
    """Bounded URL path, or None when parsing fails."""
    if not isinstance(url, str):
        return None

    try:
        return urlsplit(url).path[:_MAX_OBJECT_PATH_CHARS]
    except Exception:
        return None


def describe_presigned_url(url: object) -> dict[str, Any]:
    """Object path and declared expiry without the signing query string."""
    if not isinstance(url, str):
        # urlsplit(None) returns bytes fields; bytes in logging extras drop the report
        return {"object_host": None, "object_path": None, "url_expires_in": None}

    try:
        url_parts = urlsplit(url)
        expires_values = parse_qs(url_parts.query).get("X-Amz-Expires")
        path = _ENCODED_QUERY_SEPARATOR.split(url_parts.path, maxsplit=1)[0]
        return {
            # netloc would include user:pass@ userinfo
            "object_host": url_parts.hostname,
            "object_path": redact_sensitive(path[:_MAX_OBJECT_PATH_CHARS]),
            "url_expires_in": safe_int(expires_values[0] if expires_values else None),
        }
    except Exception:
        return {"object_host": None, "object_path": None, "url_expires_in": None}


def _read_response_body_prefix(response: requests.Response) -> Optional[bytes]:
    try:
        # The cut keeps redaction cheap: its URL pattern backtracks over every
        # start position, so an unbounded body costs seconds of the user's cell
        return response.content[:_MAX_ERROR_BODY_BYTES]
    except Exception:
        return None


def _parse_s3_error_fields(body: bytes) -> dict[str, str]:
    """Allowlisted elements of an S3 error document, best-effort.

    Bodies arrive cut at _MAX_ERROR_BODY_BYTES, so the document usually has no
    closing tag. A pull parser that is never closed still reports every element
    that ended inside the prefix, and read_events() re-raises a malformed-XML
    failure only after yielding the events that preceded it.
    """
    parser = ET.XMLPullParser(["end"])
    fields: dict[str, str] = {}
    try:
        parser.feed(body)
        for _, element in parser.read_events():
            # An empty element carries nothing, so leave it out and let the
            # caller fall back to the body snippet
            if element.tag in _S3_ERROR_FIELDS and element.text:
                fields.setdefault(element.tag, element.text)
    except Exception:
        # Bogus encoding declarations raise LookupError rather than ParseError;
        # nothing here may mask the failure these diagnostics describe
        pass

    return fields


def read_body_snippet(response: requests.Response) -> Optional[str]:
    """Redacted response body prefix, or None when unreadable."""
    body = _read_response_body_prefix(response)
    if body is None:
        return None

    return _redacted_snippet(body.decode("utf-8", errors="replace"))


def describe_s3_error(response: requests.Response) -> dict[str, Any]:
    """Log-safe fields from a failed object-store HTTP response."""
    diagnostics: dict[str, Any] = {
        "status_code": response.status_code,
        "aws_request_id": response.headers.get("x-amz-request-id"),
        "aws_host_id": response.headers.get("x-amz-id-2"),
        "aws_date": response.headers.get("Date"),
        **{field: None for field in _S3_ERROR_FIELDS.values()},
    }

    body = _read_response_body_prefix(response)
    if body is None:
        return diagnostics

    found = _parse_s3_error_fields(body)
    for element, field in _S3_ERROR_FIELDS.items():
        value = found.get(element)
        if value is not None:
            diagnostics[field] = _redacted_snippet(value)

    if diagnostics["s3_error_code"] is None:
        # Non-S3 body (proxy/gateway)
        diagnostics["response_body_snippet"] = _redacted_snippet(
            body.decode("utf-8", errors="replace")
        )

    return diagnostics


def describe_exception(exc: BaseException) -> dict[str, Any]:
    """Log-safe fields from a cache-related exception."""
    if isinstance(exc, SqlCacheHttpError):
        return dict(exc.diagnostics)

    # HTTPError.message is status + URL; the response body is still available
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return describe_s3_error(exc.response)

    # Truncate before redact: _URL_QUERY_PATTERN is O(n²) on long unbounded text
    return {
        "error_type": type(exc).__name__,
        "error_message": _redacted_snippet(str(exc)[:_MAX_RAW_EXCEPTION_CHARS]),
    }


def diagnostics_summary(diag: dict[str, Any]) -> str:
    """One-line summary from a diagnostics dict, for the log message."""
    # S3 HTTP error: "403 AccessDenied" or just "403"
    status = diag.get("status_code")
    if status is not None:
        code = diag.get("s3_error_code")
        return f"{status} {code}" if code else str(status)

    # Non-HTTP exception: "ConnectionError: timed out"
    error_type = diag.get("error_type", "")
    error_msg = diag.get("error_message", "")
    if error_type and error_msg:
        return f"{error_type}: {error_msg}"

    return error_type or error_msg or "unknown error"
