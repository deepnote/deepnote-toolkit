import base64
import contextlib
import json
import re
import uuid
import warnings
import weakref
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import quote

import google.oauth2.credentials
import numpy as np
import requests
import wrapt
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from google.api_core.client_info import ClientInfo
from google.cloud import bigquery
from packaging.version import parse as parse_version
from pydantic import BaseModel
from sqlalchemy.engine import URL, Connection, create_engine, make_url
from sqlalchemy.exc import ResourceClosedError

from deepnote_core.pydantic_compat_helpers import model_validate_compat
from deepnote_toolkit import env as dnenv
from deepnote_toolkit.create_ssh_tunnel import create_ssh_tunnel
from deepnote_toolkit.get_webapp_url import (
    get_absolute_userpod_api_url,
    get_project_auth_headers,
)
from deepnote_toolkit.ipython_utils import output_sql_metadata
from deepnote_toolkit.logging import LoggerManager
from deepnote_toolkit.ocelots.pandas.utils import deduplicate_columns
from deepnote_toolkit.sql.duckdb_sql import execute_duckdb_sql
from deepnote_toolkit.sql.jinjasql_utils import render_jinja_sql_template
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview
from deepnote_toolkit.sql.sql_caching import get_sql_cache, upload_sql_cache
from deepnote_toolkit.sql.sql_query_chaining import add_limit_clause, unchain_sql_query
from deepnote_toolkit.sql.sql_utils import is_single_select_query
from deepnote_toolkit.sql.url_utils import replace_user_pass_in_pg_url

if TYPE_CHECKING:
    try:
        from sqlalchemy.engine.interfaces import DBAPIConnection, DBAPICursor
    except ImportError:
        # Not available in SQLAlchemy < 2.0. We use them only for typing, so replace with Any
        DBAPIConnection = Any
        DBAPICursor = Any

logger = LoggerManager().get_logger()


class IntegrationFederatedAuthParams(BaseModel):
    integrationId: str
    authContextToken: str


class FederatedAuthResponseData(BaseModel):
    integrationType: str
    accessToken: str


def compile_sql_query(
    skip_jinja_template_render,
    template,
    param_style,
    return_variable_type,
):
    """
    Compiles a SQL query by un-chaining it and filling the Jinja template if needed.
    :param skip_jinja_template_render: Boolean indicating whether to skip Jinja template rendering
    :param template: Templated SQL query
    :param param_style: Parameter style for the SQL query
    :return: Tuple of (compiled_query, bind_params, query_preview_source (the original query before adding a LIMIT clause))
    """
    # We need to unchain the query first as the referenced queries can contain Jinja templates as well
    unchained_query = unchain_sql_query(template)

    # Store the original query before adding a LIMIT clause
    query_preview_source = unchained_query

    # If we're creating a preview, we additionally need to add a limit clause
    compiled_query = (
        add_limit_clause(unchained_query)
        if return_variable_type == "query_preview"
        else unchained_query
    )

    # Now that we have the whole query, we can render the Jinja template
    bind_params = {}
    if not skip_jinja_template_render:
        compiled_query, bind_params = render_jinja_sql_template(
            compiled_query, param_style
        )

    return compiled_query, bind_params, query_preview_source


def execute_sql_with_connection_json(
    template,
    sql_alchemy_json,
    audit_sql_comment="",
    sql_cache_mode="cache_disabled",
    return_variable_type="dataframe",
):
    """
    Executes a SQL query using the given connection JSON (string).
    This is called by collab for SQL cells which don't have environment variable with the full
    connection details, namely the federated auth connections.
    :param template: Templated SQL
    :param sql_alchemy_json: String containing JSON with the connection details.
                             Mandatory fields: url, params, param_style
    :param sql_cache_mode: SQL caching setting for the query. Possible values: "cache_disabled", "always_write", "read_or_write"
    :return: Pandas dataframe with the result
    """

    class ExecuteSqlError(Exception):
        pass

    # let's ignore warnings here because otherwise they clutter the output
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")

        sql_alchemy_dict = json.loads(sql_alchemy_json)

        requires_duckdb = sql_alchemy_dict["url"] == "deepnote+duckdb:///:memory:"

        _handle_iam_params(sql_alchemy_dict)
        _handle_federated_auth_params(sql_alchemy_dict)

        requires_bigquery_oauth = (
            sql_alchemy_dict["url"] == "bigquery://?user_supplied_client=true"
        )

        if requires_bigquery_oauth:
            params = sql_alchemy_dict.get("params")
            sql_alchemy_dict["params"] = _build_params_for_bigquery_oauth(params)

        # When using key-pair authentication with Snowflake, the private key will be
        # passed as a base64 encoded string as 'snowflake_private_key'.
        #
        # If it's encrypted, the pass_phrase will be passed as 'snowflake_private_key_passphrase'.
        snowflake_private_key_64 = sql_alchemy_dict.get("params", {}).get(
            "snowflake_private_key"
        )

        if snowflake_private_key_64:
            pass_phrase = sql_alchemy_dict.get("params", {}).get(
                "snowflake_private_key_passphrase", None
            )

            snowflake_private_key = base64.b64decode(snowflake_private_key_64)

            ## Encode the password as bytes
            if pass_phrase is not None and isinstance(pass_phrase, str):
                pass_phrase = pass_phrase.encode("utf-8")

            private_key = serialization.load_pem_private_key(
                snowflake_private_key,
                password=pass_phrase,
                backend=default_backend(),
            )
            private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

            sql_alchemy_dict["params"]["connect_args"] = {
                "private_key": private_key_bytes
            }

            del sql_alchemy_dict["params"]["snowflake_private_key"]

            if (
                "params" in sql_alchemy_dict
                and "snowflake_private_key_passphrase" in sql_alchemy_dict["params"]
            ):
                del sql_alchemy_dict["params"]["snowflake_private_key_passphrase"]

        param_style = sql_alchemy_dict.get("param_style")

        # Auto-detect param_style for databases that don't support pyformat default
        if param_style is None:
            url_obj = make_url(sql_alchemy_dict["url"])
            # Mapping of SQLAlchemy dialect names to their required param_style
            dialect_param_styles = {
                "trino": "qmark",  # Trino only supports qmark style
                "deepnote+duckdb": "qmark",  # DuckDB officially recommends qmark style (doesn't support pyformat)
            }
            param_style = dialect_param_styles.get(url_obj.drivername)

        skip_template_render = re.search(
            "^snowflake.*host=.*.proxy.cloud.getdbt.com", sql_alchemy_dict["url"]
        )

        compiled_query, bind_params, query_preview_source = compile_sql_query(
            skip_template_render,
            template,
            param_style,
            return_variable_type,
        )

        if not compiled_query.strip():
            return

        if (
            not is_single_select_query(compiled_query)
            and return_variable_type == "query_preview"
        ):
            raise ExecuteSqlError(
                "Invalid query type: Query Preview supports only a single SELECT statement"
            )

        return _execute_sql_with_caching(
            compiled_query,
            bind_params,
            audit_sql_comment,
            sql_alchemy_dict,
            requires_duckdb,
            sql_cache_mode,
            return_variable_type,
            query_preview_source,
        )


def execute_sql(
    template,
    sql_alchemy_json_env_var,
    audit_sql_comment="",
    sql_cache_mode="cache_disabled",
    return_variable_type="dataframe",
):
    """
    Wrapper around execute_sql_with_connection_json which reads the connection JSON from
    environment variable.
    :param template: Templated SQL
    :param sql_alchemy_json_env_var: Name of the environment variable containing the connection JSON
    :param sql_cache_mode: SQL caching setting for the query. Possible values: "cache_disabled", "always_write", "read_or_write"
    :return: Pandas dataframe with the result
    """

    class ExecuteSqlError(Exception):
        pass

    if not sql_alchemy_json_env_var:
        raise ExecuteSqlError(
            "This SQL cell is not linked with a connected integration"
        )

    sql_alchemy_json = dnenv.get_env(sql_alchemy_json_env_var)
    if not sql_alchemy_json:
        raise ExecuteSqlError(
            "This SQL cell is not linked with a connected integration"
        )

    return execute_sql_with_connection_json(
        template,
        sql_alchemy_json,
        audit_sql_comment=audit_sql_comment,
        sql_cache_mode=sql_cache_mode,
        return_variable_type=return_variable_type,
    )


def _generate_temporary_credentials(integration_id):
    url = get_absolute_userpod_api_url(f"integrations/credentials/{integration_id}")

    # Add project credentials in detached mode
    headers = get_project_auth_headers()

    response = requests.post(url, timeout=10, headers=headers)

    response.raise_for_status()

    data = response.json()

    return quote(data["username"]), quote(data["password"])


def _get_federated_auth_credentials(
    integration_id: str, user_pod_auth_context_token: str
) -> FederatedAuthResponseData:
    """Get federated auth credentials for the given integration ID and user pod auth context token."""

    url = get_absolute_userpod_api_url(
        f"integrations/federated-auth-token/{integration_id}"
    )

    # Add project credentials in detached mode
    headers = get_project_auth_headers()
    headers["UserPodAuthContextToken"] = user_pod_auth_context_token

    response = requests.post(url, timeout=10, headers=headers)

    response.raise_for_status()

    data = model_validate_compat(FederatedAuthResponseData, response.json())

    return data


def _handle_iam_params(sql_alchemy_dict: dict[str, Any]) -> None:
    """Apply IAM credentials to the connection URL in-place."""

    if "iamParams" not in sql_alchemy_dict:
        return

    integration_id = sql_alchemy_dict["iamParams"]["integrationId"]

    temporary_username, temporary_password = _generate_temporary_credentials(
        integration_id
    )

    sql_alchemy_dict["url"] = replace_user_pass_in_pg_url(
        sql_alchemy_dict["url"], temporary_username, temporary_password
    )


def _handle_federated_auth_params(sql_alchemy_dict: dict[str, Any]) -> None:
    """Fetch and apply federated auth credentials to connection params in-place."""

    if "federatedAuthParams" not in sql_alchemy_dict:
        return

    try:
        federated_auth_params = model_validate_compat(
            IntegrationFederatedAuthParams, sql_alchemy_dict["federatedAuthParams"]
        )
    except Exception:
        logger.exception("Invalid federated auth params, try updating toolkit version")
        return

    federated_auth = _get_federated_auth_credentials(
        federated_auth_params.integrationId, federated_auth_params.authContextToken
    )

    if federated_auth.integrationType == "trino":
        try:
            sql_alchemy_dict["params"]["connect_args"]["http_headers"][
                "Authorization"
            ] = f"Bearer {federated_auth.accessToken}"
        except KeyError:
            logger.exception(
                "Invalid federated auth params, try updating toolkit version"
            )
    elif federated_auth.integrationType == "big-query":
        try:
            sql_alchemy_dict["params"]["access_token"] = federated_auth.accessToken
        except KeyError:
            logger.exception(
                "Invalid federated auth params, try updating toolkit version"
            )
    elif federated_auth.integrationType == "snowflake":
        # Snowflake federated auth is not supported yet, using the original connection URL
        pass
    else:
        logger.error(
            "Unsupported integration type: %s, try updating toolkit version",
            federated_auth.integrationType,
        )


@contextlib.contextmanager
def _create_sql_ssh_uri(ssh_enabled, sql_alchemy_dict):
    server = None
    if ssh_enabled:
        base64_encoded_key = dnenv.get_env("PRIVATE_SSH_KEY_BLOB")
        if not base64_encoded_key:
            raise Exception(
                "The private key needed to establish the SSH connection is missing. Please try again or contact support."
            )
        original_url = make_url(sql_alchemy_dict["url"])
        try:
            server = create_ssh_tunnel(
                ssh_host=sql_alchemy_dict["ssh_options"]["host"],
                ssh_port=int(sql_alchemy_dict["ssh_options"]["port"]),
                ssh_user=sql_alchemy_dict["ssh_options"]["user"],
                remote_host=original_url.host,
                remote_port=int(original_url.port),
                private_key=base64.b64decode(base64_encoded_key).decode("utf-8"),
            )
            url = URL.create(
                drivername=original_url.drivername,
                username=original_url.username,
                password=original_url.password,
                host=server.local_bind_host,
                port=server.local_bind_port,
                database=original_url.database,
                query=original_url.query,
            )
            yield url
        finally:
            if server is not None and server.is_active:
                server.close()
    else:
        yield None


def _execute_sql_with_caching(
    query,
    bind_params,
    audit_sql_comment,
    sql_alchemy_dict,
    requires_duckdb,
    sql_cache_mode,
    return_variable_type,
    query_preview_source,
):
    # duckdb SQL is not cached, so we can skip the logic below for duckdb
    if requires_duckdb:
        dataframe = execute_duckdb_sql(query, bind_params)
        # for Chained SQL we return the dataframe with the SQL source attached as DeepnoteQueryPreview object
        if return_variable_type == "query_preview":
            return _convert_dataframe_to_query_preview(dataframe, query_preview_source)

        return dataframe

    sql_caching_enabled = (
        sql_cache_mode != "cache_disabled" and return_variable_type == "dataframe"
    )
    integration_id = sql_alchemy_dict.get("integration_id")
    can_get_sql_cache = integration_id is not None and sql_caching_enabled

    cache_upload_url = None

    if can_get_sql_cache:
        dataframe_from_cache, cache_upload_url = get_sql_cache(
            query, bind_params, integration_id, sql_cache_mode, return_variable_type
        )
        if dataframe_from_cache is not None:
            return dataframe_from_cache

    # The comment must be appended AFTER the query statement because of Snowflake.
    # Comments at the beginning of a query are, for some reason, ignored in Snowflake Activity UI
    # If the query ends with a semicolon, the audit comment needs to be added before it, to not confuse Athena that the query contains multiple statements (only one statement is allowed in Athena queries)
    query_with_audit_comment = (
        query[:-1] + audit_sql_comment + ";"
        if query.strip().endswith(";")
        else query + audit_sql_comment
    )

    return _query_data_source(
        query_with_audit_comment,
        bind_params,
        sql_alchemy_dict,
        cache_upload_url,
        return_variable_type,
        query_preview_source,  # The original query before any transformations such as appending a LIMIT clause
    )


@contextlib.contextmanager
def suppress_third_party_deprecation_warnings():
    """Suppress known deprecation warnings from third-party SQL packages.

    These warnings are caused by internal implementation details of upstream packages
    and cannot be fixed in deepnote-toolkit. We suppress them to avoid cluttering
    user output with warnings they cannot act upon.

    Suppressed warnings:
    - databricks-sqlalchemy: '_user_agent_entry' parameter deprecated
      https://github.com/databricks/databricks-sqlalchemy/issues/36
    """
    with warnings.catch_warnings():
        # databricks-sqlalchemy uses deprecated '_user_agent_entry' parameter
        warnings.filterwarnings(
            "ignore",
            message=r"Parameter '_user_agent_entry' is deprecated",
        )
        yield


def _query_data_source(
    query,
    bind_params,
    sql_alchemy_dict,
    cache_upload_url,
    return_variable_type,
    query_preview_source,
):
    sshEnabled = sql_alchemy_dict.get("ssh_options", {}).get("enabled", False)

    with _create_sql_ssh_uri(sshEnabled, sql_alchemy_dict) as url:
        if url is None:
            url = sql_alchemy_dict["url"]

        with suppress_third_party_deprecation_warnings():
            engine = create_engine(
                url, **sql_alchemy_dict["params"], pool_pre_ping=True
            )

        try:
            dataframe = _execute_sql_on_engine(engine, query, bind_params)

            if dataframe is None:
                return None

            # sanitize dataframe so that we can safely call .to_parquet on it
            _sanitize_dataframe_for_parquet(dataframe)

            dataframe_size_in_bytes = int(dataframe.memory_usage(deep=True).sum())
            output_sql_metadata(
                {
                    "status": "success_no_cache",
                    "size_in_bytes": dataframe_size_in_bytes,
                    "compiled_query": query,
                    "variable_type": return_variable_type,
                    "integration_id": sql_alchemy_dict.get("integration_id"),
                }
            )

            # for Chained SQL we return the dataframe with the SQL source attached as DeepnoteQueryPreview object
            if return_variable_type == "query_preview":
                return _convert_dataframe_to_query_preview(
                    dataframe, query_preview_source
                )

            # if df is larger than 5GB, don't upload it. See NB-988
            dataframe_is_cacheable = dataframe_size_in_bytes < 5 * 1024 * 1024 * 1024

            if cache_upload_url is not None and dataframe_is_cacheable:
                upload_sql_cache(dataframe, cache_upload_url)

            return dataframe
        finally:
            engine.dispose()


class CursorTrackingDBAPIConnection(wrapt.ObjectProxy):
    """Wraps DBAPI connection to track cursors as they're created."""

    def __init__(
        self,
        wrapped: "DBAPIConnection",
        cursor_registry: Optional[weakref.WeakSet["DBAPICursor"]] = None,
    ) -> None:
        super().__init__(wrapped)
        # Use provided registry or create our own
        self._self_cursor_registry = (
            cursor_registry if cursor_registry is not None else weakref.WeakSet()
        )

    def cursor(self, *args, **kwargs):
        cursor = self.__wrapped__.cursor(*args, **kwargs)
        try:
            self._self_cursor_registry.add(cursor)
        except TypeError:
            logger.warning(
                f"DBAPI Cursor of type {type(cursor)} can't be added to weakset and thus can't be tracked."
            )
        return cursor

    def cancel_all_cursors(self):
        """Cancel all tracked cursors. Best-effort, ignores errors."""
        for cursor in self._self_cursor_registry:
            _cancel_cursor(cursor)


class CursorTrackingSQLAlchemyConnection(wrapt.ObjectProxy):
    """A SQLAlchemy connection wrapper that tracks cursors for cancellation.

    This wrapper replaces the internal _dbapi_connection with a tracking proxy,
    so all cursors created (including by exec_driver_sql) are tracked.
    """

    def __init__(self, wrapped: Connection) -> None:
        super().__init__(wrapped)
        self._self_cursors: weakref.WeakSet[DBAPICursor] = weakref.WeakSet()
        self._install_dbapi_wrapper()

    def _install_dbapi_wrapper(self):
        """Replace SQLAlchemy's internal DBAPI connection with our tracking wrapper."""
        try:
            # Access the internal DBAPI connection
            if hasattr(self.__wrapped__.connection, "dbapi_connection"):
                dbapi_conn = self.__wrapped__.connection.dbapi_connection
                dbapi_connection_attr_name = "dbapi_connection"
            else:
                # SQLAlchemy pre v1.4
                dbapi_conn = self.__wrapped__.connection.connection
                dbapi_connection_attr_name = "connection"
            if dbapi_conn is None:
                logger.warning(
                    f"DBAPI connection is None (connection type {type(self.__wrapped__)}), cannot install tracking"
                )
                return

            setattr(
                self.__wrapped__.connection,
                dbapi_connection_attr_name,
                CursorTrackingDBAPIConnection(dbapi_conn, self._self_cursors),
            )
        except Exception as e:
            logger.warning(f"Could not install DBAPI wrapper: {e}")

    def cancel_all_cursors(self):
        """Cancel all tracked cursors. Best-effort, ignores errors."""
        for cursor in self._self_cursors:
            _cancel_cursor(cursor)


def _cancel_cursor(cursor: "DBAPICursor") -> None:
    """Best-effort cancel a cursor using available methods."""
    try:
        if hasattr(cursor, "cancel") and callable(cursor.cancel):
            cursor.cancel()
    except (Exception, KeyboardInterrupt):
        pass  # Best effort, ignore all errors


def _execute_sql_on_engine(engine, query, bind_params):
    """Run *query* on *engine* and return a DataFrame.

    Uses pandas.read_sql_query to execute the query with a SQLAlchemy connection.
    For pandas 2.2+ and SQLAlchemy < 2.0, which requires a raw DB-API connection with a `.cursor()` attribute,
    we use the underlying connection.

    On exceptions (including KeyboardInterrupt from cell cancellation), all cursors
    created during execution are cancelled to stop running queries on the server.
    """

    import pandas as pd
    from sqlalchemy import __version__ as sqlalchemy_version

    from deepnote_toolkit.config import get_config

    try:
        cfg_val = get_config().runtime.coerce_float
        # Treat None as unspecified → default True
        coerce_float = True if (cfg_val is None or bool(cfg_val)) else False
    except (ImportError, AttributeError, TypeError, ValueError):
        coerce_float = True

    # Check pandas version to determine if we need raw connection
    p_ver, sa_ver = parse_version(pd.__version__), parse_version(sqlalchemy_version)
    needs_raw_connection = p_ver >= parse_version("2.2") and sa_ver < parse_version(
        "2.0"
    )

    with engine.begin() as connection:
        # For pandas 2.2+ with SQLAlchemy < 2.0, use raw DBAPI connection
        if needs_raw_connection:
            tracking_connection = CursorTrackingDBAPIConnection(connection.connection)
        else:
            tracking_connection = CursorTrackingSQLAlchemyConnection(connection)

        try:
            # pandas.read_sql_query expects params as tuple (not list) for qmark/format style
            params_for_pandas = (
                tuple(bind_params) if isinstance(bind_params, list) else bind_params
            )

            return pd.read_sql_query(
                query,
                con=tracking_connection,
                params=params_for_pandas,
                coerce_float=coerce_float,
            )
        except ResourceClosedError:
            # this happens if the query is e.g. UPDATE and pandas tries to create a dataframe from its result
            return None
        except KeyboardInterrupt:
            tracking_connection.cancel_all_cursors()
            raise


def _build_params_for_bigquery_oauth(params):
    class BigQueryCredentialsError(Exception):
        pass

    # we need to manually create BigQuery client with OAuth credentials
    access_token = params["access_token"]
    project = params["project"]
    if (not access_token) or (not project):
        raise BigQueryCredentialsError("This BigQuery cell is missing credentials.")

    credentials = google.oauth2.credentials.Credentials(access_token)
    # Add UserAgent for Google Cloud partnership tracking (MAR-237)
    # This enables Google's partnership team to track Deepnote queries in their dashboard
    client_info = ClientInfo(user_agent="Deepnote/1.0.0 (GPN:Deepnote;production)")
    client = bigquery.Client(
        project=project, credentials=credentials, client_info=client_info
    )

    return {"connect_args": {"client": client}}


def _sanitize_dataframe_for_parquet(dataframe):
    """Sanitizes the dataframe so that we can safely call .to_parquet on it"""

    deduplicate_columns(dataframe)

    # Convert columns with UUIDs to strings
    for column in dataframe.columns:
        if dataframe[column].apply(lambda x: isinstance(x, uuid.UUID)).any():
            dataframe[column] = dataframe[column].astype(str)

    # Convert columns with complex numbers to their real part
    for column in dataframe.columns:
        if (
            dataframe[column]
            .apply(lambda x: isinstance(x, (complex, np.complex64, np.complex128)))
            .any()
        ):
            dataframe[column] = dataframe[column].astype(str)

    # Convert columns with large numbers to strings
    for column in dataframe.columns:
        if (
            dataframe[column]
            .apply(lambda x: isinstance(x, (int, float)) and abs(x) > 2**63 - 1)
            .any()
        ):
            dataframe[column] = dataframe[column].astype(str)


def _convert_dataframe_to_query_preview(dataframe, query):
    """Converts a dataframe to a DeepnoteQueryPreview and stores the source query as a deepnote_query property"""
    return DeepnoteQueryPreview(
        dataframe.values, columns=dataframe.columns, deepnote_query=query
    )
