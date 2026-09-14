import hashlib
import json
import re
import tempfile
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, urlsplit
from xml.etree import ElementTree

import pandas as pd
import requests
from pyarrow import ArrowInvalid, ArrowNotImplementedError

from deepnote_toolkit.sql.sql_utils import is_single_select_query

from ..get_webapp_url import get_absolute_userpod_api_url, get_project_auth_headers
from ..ipython_utils import output_sql_metadata
from ..logging import get_logger

# Initialize logger
logger = get_logger()


def get_sql_cache(
    query, bind_params, integration_id, sql_cache_mode, return_variable_type
):
    """
    Retrieves the SQL cache from webapp for a given query.

    Args:
        query (str): The SQL query to retrieve the cache for.
        bind_params (dict): The bind parameters for the SQL query.
        integration_id (str): The integration ID associated with the cache.
        sql_cache_mode (str): The mode of the SQL cache.

    Returns:
        tuple: A tuple containing the cached dataframe (if available) and the upload URL (if applicable).
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

    cache_info = None
    try:
        cache_info = _request_cache_info_from_webapp(
            query_hash, integration_id, sql_cache_mode
        )
    except Exception as exc:
        # we failed to request the cache info from the webapp
        logger.error(
            "Failed to request SQL cache info: %s",
            exc,
            extra={"sql_caching_cause": "failed_to_request_cache_info"},
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
                    "Failed to download dataframe from cache: %s",
                    exc,
                    extra={"sql_caching_cause": "failed_to_download_from_cache"},
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
            return None, cache_info["uploadUrl"]

    return None, None


def upload_sql_cache(dataframe, upload_url):
    """upload the result to the cache as a parquet file"""

    try:
        with tempfile.TemporaryFile() as temp_file:
            try:
                dataframe.to_parquet(temp_file)
            except (ArrowNotImplementedError, ArrowInvalid, OverflowError):
                # see NB-1684
                # we fallback to pickle if parquet serialization fails (which will throw either of first 2 errors)
                # OverflowError: PyArrow raises this for Python int / Decimal values exceeding int64 range
                temp_file.seek(0)
                temp_file.truncate()
                dataframe.to_pickle(temp_file)

            temp_file.seek(0)
            # PUT the file to cache_upload_url pre-signed s3 url
            response = requests.put(upload_url, data=temp_file)
            response.raise_for_status()
    except Exception as exc:
        # Constant message so occurrences group in error tracking; the variable parts
        # go in `extra`. str(exc) and the URL are never logged as-is: both can carry
        # the presigned query string, which holds AWS credentials.
        logger.error(
            "Failed to upload SQL cache",
            extra={
                "sql_caching_cause": "failed_to_upload_to_cache",
                "error_type": type(exc).__name__,
                "error": _redact_presigned_query(str(exc)),
                **_describe_s3_response(getattr(exc, "response", None)),
                **_describe_presigned_url(upload_url),
            },
        )


def _describe_s3_response(response: Optional[requests.Response]) -> dict[str, Any]:
    """HTTP status, S3's <Code>/<Message> and the request ids AWS Support asks for.

    A bare 403 can mean an expired URL, expired credentials, a policy change or a
    signature mismatch; only <Code>/<Message> say which. Nothing else is taken from
    the body: a SignatureDoesNotMatch body also echoes the access key id and the
    canonical request.
    """
    if response is None:
        return {}
    try:
        error = ElementTree.fromstring(response.text)
    except ElementTree.ParseError:  # not an S3 error document, e.g. HTML from a proxy
        error = ElementTree.Element("Error")
    return {
        "status_code": response.status_code,
        "s3_error_code": _s3_field(error, "Code"),
        "s3_error_message": _s3_field(error, "Message"),
        "aws_request_id": response.headers.get("x-amz-request-id"),
        "aws_host_id": response.headers.get("x-amz-id-2"),
    }


def _s3_field(error: ElementTree.Element, tag: str) -> Optional[str]:
    """Redacted, length-capped text of an error field; the body is remote input."""
    text = error.findtext(tag)
    return _redact_presigned_query(text)[:200] if text else None


def _describe_presigned_url(url: str) -> dict[str, Any]:
    """Object path and validity window of a presigned URL, never its query string.

    The URL is valid from X-Amz-Date for X-Amz-Expires seconds, so comparing
    seconds_since_url_issued with url_expires_in shows whether it had expired.
    Missing or malformed values become None.
    """
    try:
        parts = urlsplit(url)
    except (AttributeError, TypeError, ValueError):  # untyped JSON from the webapp
        return {}
    query = parse_qs(parts.query)
    expires_in = query.get("X-Amz-Expires", [""])[0]
    return {
        "object_path": parts.path,
        "url_expires_in": int(expires_in) if expires_in.isdigit() else None,
        "seconds_since_url_issued": _seconds_since(query.get("X-Amz-Date", [""])[0]),
    }


def _seconds_since(amz_date: str) -> Optional[int]:
    """Seconds since a SigV4 timestamp such as 20260729T120000Z, or None if malformed."""
    try:
        issued_at = datetime.strptime(amz_date, "%Y%m%dT%H%M%S%z")
    except ValueError:
        return None
    return int((datetime.now(timezone.utc) - issued_at).total_seconds())


def _redact_presigned_query(text: str) -> str:
    """Strip query strings carrying SigV4 parameters: credentials, signature, token."""
    return re.sub(r"""\?[^\s'"]*X-Amz-[^\s'"]*""", "?<redacted>", text)


def _try_read_cache(download_url):
    try:
        # Attempt to read as a parquet file
        return pd.read_parquet(download_url)
    except ArrowInvalid:
        # ArrowInvalid means that the file at download_url is not a parquet file.
        # We fallback to the pickle format if that happens, because the cache should either be in parquet or
        # pickle format and we don't know which one it is, the file has no extension.
        # (see .to_pickle fallback in upload_sql_cache)
        pass

    try:
        # Attempt to read as a pickle file
        return pd.read_pickle(download_url)
    except Exception:
        # If reading as pickle also fails, re-raise this exception to be caught by the caller
        raise


def _generate_cache_key(query, bind_params):
    return hashlib.sha256(
        (query + json.dumps(bind_params, sort_keys=True, default=str)).encode("utf-8")
    ).hexdigest()


def _request_cache_info_from_webapp(query_hash, integration_id, sql_cache_mode):
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
        error_msg = f"Failed to request cache info from {sql_cache_url}, status code {sql_cache_response.status_code}, response {sql_cache_response.text}"
        logger.error(error_msg, extra={"sql_caching_cause": "http_error"})
        return None

    result_dict = sql_cache_response.json()
    if result_dict["result"] == "sqlCachingDisabled":
        return None

    return result_dict
