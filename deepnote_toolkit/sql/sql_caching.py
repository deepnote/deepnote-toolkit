import hashlib
import json
import tempfile
import time
from io import BytesIO
from typing import IO, Any, NamedTuple, Optional

import pandas as pd
import requests
from pyarrow import ArrowInvalid, ArrowNotImplementedError

from deepnote_toolkit.sql.sql_cache_diagnostics import (
    SqlCacheHttpError,
    describe_exception,
    describe_presigned_url,
    read_body_snippet,
    safe_url_path,
    seconds_between,
)
from deepnote_toolkit.sql.sql_utils import is_single_select_query

from ..get_webapp_url import get_absolute_userpod_api_url, get_project_auth_headers
from ..ipython_utils import output_sql_metadata
from ..logging import get_logger

# Initialize logger
logger = get_logger()

# The read timeout bounds the gap between two received chunks, not the transfer as
# a whole, so it cannot abort a large but healthy download.
_OBJECT_STORE_TIMEOUT: tuple[int, int] = (5, 60)


class SqlCacheUpload(NamedTuple):
    """A presigned upload URL together with the moment it was issued."""

    url: str
    issued_at: float


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
                **describe_exception(exc),
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
                        **describe_exception(exc),
                        **describe_presigned_url(download_url),
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

        response.raise_for_status()
        return
    except Exception as exc:
        failure = describe_exception(exc)

    logger.error(
        "Failed to upload SQL cache",
        extra={
            "sql_caching_cause": "failed_to_upload_to_cache",
            **failure,
            **describe_presigned_url(upload.url),
            "seconds_since_url_issued": seconds_between(
                upload.issued_at, put_started_at
            ),
            # Separate field so "the URL was already old" stays distinguishable
            # from "the transfer was slow"
            "upload_duration_seconds": seconds_between(
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
    # whole error body, and so the rest of it is dropped with the connection.
    # Both branches must read the body inside the block: once it exits the body
    # reads back as empty rather than raising, which would silently cache-miss
    # every hit and drop S3's error document on the way out.
    with requests.get(
        download_url, timeout=_OBJECT_STORE_TIMEOUT, stream=True
    ) as response:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise SqlCacheHttpError(describe_exception(exc)) from exc

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
        logger.error(
            "Failed to request cache info",
            extra={
                "sql_caching_cause": "http_error",
                "status_code": sql_cache_response.status_code,
                "cache_info_path": safe_url_path(sql_cache_url),
                "response_body_snippet": read_body_snippet(sql_cache_response),
            },
        )
        return None

    result_dict = sql_cache_response.json()
    if result_dict["result"] == "sqlCachingDisabled":
        return None

    return result_dict
