"""Helpers for federated authentication inside Streamlit data apps.

Streamlit data apps run as a long-lived process inside the project pod, separate from the
notebook kernel. The viewer is identified by the ``streamlit-token`` cookie set when the
data app SSR page is rendered. To obtain database credentials scoped to the viewer (rather
than to the project owner), apps call this helper, which forwards the cookie to the webapp's
userpod-api as a ``StreamlitToken`` header.

Usage inside a Streamlit data app::

    import deepnote_toolkit.streamlit_data_apps as dn

    creds = dn.get_federated_auth_token("<integration-id>")
    # creds = {
    #   "integrationType": "big-query" | "snowflake" | "trino",
    #   "accessToken": "<oauth-access-token>",
    #   "connectionParams": {"type": "big-query" | "snowflake" | "trino", ...},
    # }

Convenience wrappers for the most common clients are also provided::

    client = dn.get_bigquery_client("<integration-id>")
    conn = dn.get_snowflake_connection("<integration-id>")
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from .get_webapp_url import (
    get_absolute_userpod_api_url,
    get_project_auth_headers,
)

STREAMLIT_TOKEN_COOKIE_NAME = "streamlit-token"


class StreamlitFederatedAuthError(Exception):
    """Raised when the federated auth token cannot be obtained for a Streamlit viewer."""


class FederatedAuthRequired(StreamlitFederatedAuthError):
    """Raised when the viewer has not yet authenticated the federated integration.

    Carries ``auth_url`` (the Deepnote OAuth start URL for this integration) and
    ``integration_name`` so callers can render an authentication prompt.
    """

    def __init__(
        self,
        message: str,
        *,
        auth_url: str,
        integration_name: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.auth_url = auth_url
        self.integration_name = integration_name


def _read_streamlit_token_from_context() -> Optional[str]:
    """Read the ``streamlit-token`` cookie from the active Streamlit context.

    Returns ``None`` if Streamlit is not installed, no script run is active, or the cookie
    is missing.
    """

    try:
        import streamlit as st
    except ImportError:
        return None

    try:
        cookies = st.context.cookies
    except Exception:
        return None

    if not cookies:
        return None

    token = cookies.get(STREAMLIT_TOKEN_COOKIE_NAME)
    if not isinstance(token, str) or not token:
        return None
    return token


def get_federated_auth_token(
    integration_id: str,
    *,
    streamlit_token: Optional[str] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """Fetch a federated-auth access token for the current Streamlit viewer.

    Parameters
    ----------
    integration_id:
        The Deepnote integration UUID.
    streamlit_token:
        Optional override for the ``streamlit-token`` cookie value. If not provided, the
        token is read from ``st.context.cookies``.
    timeout:
        Timeout in seconds for the HTTP request.

    Returns
    -------
    dict
        A dict with ``integrationType``, ``accessToken``, and ``connectionParams`` keys.
        ``connectionParams`` carries non-secret integration metadata useful for building
        a database client (e.g. ``project`` for BigQuery, ``accountName`` for Snowflake).

    Raises
    ------
    StreamlitFederatedAuthError
        If the token cannot be resolved or the webapp returns a non-2xx response.
    """

    if not integration_id:
        raise StreamlitFederatedAuthError("integration_id is required.")

    token = streamlit_token or _read_streamlit_token_from_context()
    if not token:
        raise StreamlitFederatedAuthError(
            "Could not read the `streamlit-token` cookie from the Streamlit context. "
            "This helper is intended to run inside a Streamlit data app served via "
            "Deepnote, where the cookie is forwarded by the proxy."
        )

    # ``get_absolute_userpod_api_url`` resolves the project ID from the runtime config
    # / DEEPNOTE_PROJECT_ID env var. Inside the project pod the userpod-proxy sidecar
    # already prepends the project ID before forwarding to the webapp, so the relative
    # URL passed in here must NOT include it.
    url = get_absolute_userpod_api_url(
        f"integrations/federated-auth-token-streamlit/{integration_id}"
    )

    headers: Dict[str, str] = {
        "Content-Type": "application/json",
        "StreamlitToken": token,
        **get_project_auth_headers(),
    }

    request = urllib.request.Request(
        url,
        data=b"",
        method="POST",
        headers=headers,
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        auth_required = _parse_auth_required(error_body)
        if auth_required is not None:
            raise auth_required from error
        raise StreamlitFederatedAuthError(
            f"Federated auth request failed with HTTP {error.code}: {error_body}"
        ) from error
    except urllib.error.URLError as error:
        raise StreamlitFederatedAuthError(
            f"Federated auth request failed: {error}"
        ) from error

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as error:
        raise StreamlitFederatedAuthError(
            f"Federated auth response was not valid JSON: {body!r}"
        ) from error

    if "accessToken" not in payload or "integrationType" not in payload:
        raise StreamlitFederatedAuthError(
            f"Federated auth response is missing required fields: {payload!r}"
        )

    payload.setdefault("connectionParams", {})
    return payload


def _parse_auth_required(error_body: str) -> Optional["FederatedAuthRequired"]:
    """Return a ``FederatedAuthRequired`` if the error body advertises an auth URL."""

    try:
        body = json.loads(error_body)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(body, dict):
        return None

    auth_required = body.get("authRequired")
    if not isinstance(auth_required, dict):
        return None

    auth_url = auth_required.get("authUrl")
    integration_name = auth_required.get("integrationName")
    if not isinstance(auth_url, str) or not auth_url:
        return None

    message = body.get("error")
    if not isinstance(message, str) or not message:
        message = (
            f"Sign in to {integration_name} to use this integration."
            if integration_name
            else "Sign in to this integration before using it."
        )

    return FederatedAuthRequired(
        message,
        auth_url=auth_url,
        integration_name=(
            integration_name if isinstance(integration_name, str) else None
        ),
    )


def prompt_federated_auth(
    integration_id: str,
    *,
    streamlit_token: Optional[str] = None,
    stop: bool = True,
) -> None:
    """Render a Streamlit prompt asking the viewer to authenticate the integration.

    Calls :func:`get_federated_auth_token` to discover the OAuth start URL for the
    integration. If the viewer has already authenticated, this is a no-op. Otherwise
    it renders ``st.error`` with a link button that opens the same OAuth flow used by
    notebooks and published apps. By default the script is then halted via
    :func:`streamlit.stop` so the rest of the data app does not run with missing
    credentials.
    """

    import streamlit as st

    try:
        get_federated_auth_token(
            integration_id,
            streamlit_token=streamlit_token,
        )
        return
    except FederatedAuthRequired as auth_required:
        label = (
            f"Authenticate {auth_required.integration_name}"
            if auth_required.integration_name
            else "Authenticate integration"
        )
        st.error(str(auth_required))
        try:
            st.link_button(label, auth_required.auth_url, type="primary")
        except TypeError:
            # Older Streamlit versions don't accept ``type``.
            st.link_button(label, auth_required.auth_url)
        if stop:
            st.stop()


def get_bigquery_client(
    integration_id: str,
    *,
    project: Optional[str] = None,
    streamlit_token: Optional[str] = None,
    **client_kwargs: Any,
) -> Any:
    """Build a ``google.cloud.bigquery.Client`` for the current Streamlit viewer.

    The viewer's OAuth access token is obtained from Deepnote and used as the credential.
    """

    from google.api_core.client_info import ClientInfo
    from google.cloud import bigquery
    from google.oauth2.credentials import Credentials

    try:
        payload = get_federated_auth_token(
            integration_id,
            streamlit_token=streamlit_token,
        )
    except FederatedAuthRequired:
        prompt_federated_auth(
            integration_id,
            streamlit_token=streamlit_token,
        )
        raise

    params = payload.get("connectionParams", {})
    if params.get("type") != "big-query":
        raise StreamlitFederatedAuthError(
            f"Integration {integration_id} is not a BigQuery integration "
            f"(got {params.get('type')!r})."
        )

    resolved_project = project or params.get("project")
    if not resolved_project:
        raise StreamlitFederatedAuthError(
            "BigQuery integration metadata did not include a project. "
            "Pass `project=` explicitly."
        )

    credentials = Credentials(payload["accessToken"])
    # Match the User-Agent used by the notebook flow so Google's partnership
    # dashboard correctly attributes traffic to Deepnote (MAR-237).
    client_info = client_kwargs.pop(
        "client_info",
        ClientInfo(user_agent="Deepnote/1.0.0 (GPN:Deepnote;production)"),
    )

    return bigquery.Client(
        project=resolved_project,
        credentials=credentials,
        client_info=client_info,
        **client_kwargs,
    )


def get_snowflake_connection(
    integration_id: str,
    *,
    account: Optional[str] = None,
    warehouse: Optional[str] = None,
    database: Optional[str] = None,
    role: Optional[str] = None,
    user: Optional[str] = None,
    streamlit_token: Optional[str] = None,
    **connect_kwargs: Any,
) -> Any:
    """Open a ``snowflake.connector`` connection for the current Streamlit viewer.

    The viewer's OAuth access token is used as the Snowflake authenticator token.
    """

    import snowflake.connector  # type: ignore[import-not-found]

    try:
        payload = get_federated_auth_token(
            integration_id,
            streamlit_token=streamlit_token,
        )
    except FederatedAuthRequired:
        prompt_federated_auth(
            integration_id,
            streamlit_token=streamlit_token,
        )
        raise

    params = payload.get("connectionParams", {})
    if params.get("type") != "snowflake":
        raise StreamlitFederatedAuthError(
            f"Integration {integration_id} is not a Snowflake integration "
            f"(got {params.get('type')!r})."
        )

    resolved_account = account or params.get("accountName")
    if not resolved_account:
        raise StreamlitFederatedAuthError(
            "Snowflake integration metadata did not include an account name. "
            "Pass `account=` explicitly."
        )

    kwargs: Dict[str, Any] = dict(connect_kwargs)
    kwargs.setdefault("account", resolved_account)
    kwargs.setdefault("authenticator", "oauth")
    kwargs.setdefault("token", payload["accessToken"])

    resolved_warehouse = warehouse or params.get("warehouse")
    if resolved_warehouse:
        kwargs.setdefault("warehouse", resolved_warehouse)

    resolved_database = database or params.get("database")
    if resolved_database:
        kwargs.setdefault("database", resolved_database)

    resolved_role = role or params.get("role")
    if resolved_role:
        kwargs.setdefault("role", resolved_role)

    resolved_user = user or params.get("user")
    if resolved_user:
        kwargs.setdefault("user", resolved_user)

    return snowflake.connector.connect(**kwargs)
