import hashlib
import json
import tempfile

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
            except (ArrowNotImplementedError, ArrowInvalid):
                # see NB-1684
                # we fallback to pickle if parquet serialization fails (which will throw either of these 2 errors)
                dataframe.to_pickle(temp_file)

            temp_file.seek(0)
            # PUT the file to cache_upload_url pre-signed s3 url
            response = requests.put(upload_url, data=temp_file)
            response.raise_for_status()
    except Exception as exc:
        logger.error(
            "Failed to upload SQL cache: %s",
            exc,
            extra={"sql_caching_cause": "failed_to_upload_to_cache"},
        )


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
