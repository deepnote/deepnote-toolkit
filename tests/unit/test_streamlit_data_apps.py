import io
import json
import textwrap
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from deepnote_toolkit.streamlit_data_apps import (
    FederatedAuthRequired,
    StreamlitFederatedAuthError,
    get_federated_auth_token,
    get_snowflake_connection,
    prompt_federated_auth,
)


def _setup_attached_config(tmp_path, monkeypatch):
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(textwrap.dedent("""
        [runtime]
        running_in_detached_mode = false
        dev_mode = false
    """).strip())
    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))
    monkeypatch.setenv("DEEPNOTE_PROJECT_ID", "test-project")


def _build_response(payload: Dict[str, Any]) -> MagicMock:
    response = MagicMock()
    response.read.return_value = json.dumps(payload).encode("utf-8")
    response.__enter__.return_value = response
    response.__exit__.return_value = False
    return response


def test_get_federated_auth_token_calls_local_endpoint(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    captured = {}

    def fake_urlopen(request, timeout):  # noqa: ARG001
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["method"] = request.get_method()
        return _build_response(
            {
                "integrationType": "big-query",
                "accessToken": "token-abc",
                "connectionParams": {"type": "big-query", "project": "my-project"},
            }
        )

    with patch(
        "deepnote_toolkit.streamlit_data_apps.urllib.request.urlopen",
        side_effect=fake_urlopen,
    ):
        result = get_federated_auth_token(
            "integration-1", streamlit_token="cookie-value"
        )

    assert result == {
        "integrationType": "big-query",
        "accessToken": "token-abc",
        "connectionParams": {"type": "big-query", "project": "my-project"},
    }
    # The userpod-proxy sidecar at localhost:19456 prepends the project ID
    # before forwarding to the webapp, so the URL we build does NOT include it.
    assert captured["url"] == (
        "http://localhost:19456/userpod-api/integrations/"
        "federated-auth-token-streamlit/integration-1"
    )
    assert captured["method"] == "POST"
    # urllib lowercases header keys when iterating with header_items().
    lower_headers = {k.lower(): v for k, v in captured["headers"].items()}
    assert lower_headers["streamlittoken"] == "cookie-value"
    assert lower_headers["content-type"] == "application/json"


def test_get_federated_auth_token_raises_when_token_missing(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    with patch(
        "deepnote_toolkit.streamlit_data_apps._read_streamlit_token_from_context",
        return_value=None,
    ):
        with pytest.raises(StreamlitFederatedAuthError) as excinfo:
            get_federated_auth_token("integration-1")

    assert "streamlit-token" in str(excinfo.value)


def test_get_federated_auth_token_requires_integration_id(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    with pytest.raises(StreamlitFederatedAuthError):
        get_federated_auth_token("", streamlit_token="cookie-value")


def test_get_federated_auth_token_wraps_http_errors(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    import urllib.error

    def fake_urlopen(request, timeout):  # noqa: ARG001
        raise urllib.error.HTTPError(
            request.full_url, 401, "Unauthorized", {}, io.BytesIO(b'{"error":"nope"}')
        )

    with patch(
        "deepnote_toolkit.streamlit_data_apps.urllib.request.urlopen",
        side_effect=fake_urlopen,
    ):
        with pytest.raises(StreamlitFederatedAuthError) as excinfo:
            get_federated_auth_token("integration-1", streamlit_token="cookie-value")

    assert "401" in str(excinfo.value)


def test_get_federated_auth_token_validates_response_shape(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    def fake_urlopen(request, timeout):  # noqa: ARG001
        return _build_response({"integrationType": "big-query"})

    with patch(
        "deepnote_toolkit.streamlit_data_apps.urllib.request.urlopen",
        side_effect=fake_urlopen,
    ):
        with pytest.raises(StreamlitFederatedAuthError):
            get_federated_auth_token("integration-1", streamlit_token="cookie-value")


def test_get_federated_auth_token_raises_federated_auth_required(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    import urllib.error

    error_payload = json.dumps(
        {
            "error": "Sign in to Snowflake DWH to use this integration.",
            "authRequired": {
                "integrationName": "Snowflake DWH",
                "authUrl": "https://deepnote.test/auth/snowflake/okta/abc?source=app",
            },
        }
    ).encode("utf-8")

    def fake_urlopen(request, timeout):  # noqa: ARG001
        raise urllib.error.HTTPError(
            request.full_url, 401, "Unauthorized", {}, io.BytesIO(error_payload)
        )

    with patch(
        "deepnote_toolkit.streamlit_data_apps.urllib.request.urlopen",
        side_effect=fake_urlopen,
    ):
        with pytest.raises(FederatedAuthRequired) as excinfo:
            get_federated_auth_token("integration-1", streamlit_token="cookie-value")

    assert excinfo.value.auth_url == (
        "https://deepnote.test/auth/snowflake/okta/abc?source=app"
    )
    assert excinfo.value.integration_name == "Snowflake DWH"
    assert "Snowflake DWH" in str(excinfo.value)


def test_prompt_federated_auth_renders_streamlit_prompt(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    fake_streamlit = MagicMock()
    fake_streamlit.stop.side_effect = RuntimeError("st.stop called")

    auth_required = FederatedAuthRequired(
        "Sign in to Snowflake DWH to use this integration.",
        auth_url="https://deepnote.test/auth/snowflake/okta/abc?source=app",
        integration_name="Snowflake DWH",
    )

    with patch.dict("sys.modules", {"streamlit": fake_streamlit}):
        with patch(
            "deepnote_toolkit.streamlit_data_apps.get_federated_auth_token",
            side_effect=auth_required,
        ):
            with pytest.raises(RuntimeError, match="st.stop called"):
                prompt_federated_auth("integration-1", streamlit_token="cookie-value")

    fake_streamlit.error.assert_called_once_with(
        "Sign in to Snowflake DWH to use this integration."
    )
    fake_streamlit.link_button.assert_called_once_with(
        "Authenticate Snowflake DWH",
        "https://deepnote.test/auth/snowflake/okta/abc?source=app",
        type="primary",
    )
    fake_streamlit.stop.assert_called_once()


def test_prompt_federated_auth_noop_when_already_authenticated(tmp_path, monkeypatch):
    _setup_attached_config(tmp_path, monkeypatch)

    fake_streamlit = MagicMock()

    with patch.dict("sys.modules", {"streamlit": fake_streamlit}):
        with patch(
            "deepnote_toolkit.streamlit_data_apps.get_federated_auth_token",
            return_value={
                "integrationType": "snowflake",
                "accessToken": "tok",
                "connectionParams": {},
            },
        ):
            prompt_federated_auth("integration-1", streamlit_token="cookie-value")

    fake_streamlit.error.assert_not_called()
    fake_streamlit.link_button.assert_not_called()
    fake_streamlit.stop.assert_not_called()


def test_get_snowflake_connection_passes_user_from_connection_params(
    tmp_path, monkeypatch
):
    _setup_attached_config(tmp_path, monkeypatch)

    fake_connector = MagicMock()
    fake_connector.connect.return_value = MagicMock(name="snowflake-connection")
    fake_snowflake = MagicMock()
    fake_snowflake.connector = fake_connector

    with patch.dict(
        "sys.modules",
        {"snowflake": fake_snowflake, "snowflake.connector": fake_connector},
    ):
        with patch(
            "deepnote_toolkit.streamlit_data_apps.get_federated_auth_token",
            return_value={
                "integrationType": "snowflake",
                "accessToken": "viewer-access-token",
                "connectionParams": {
                    "type": "snowflake",
                    "accountName": "acc",
                    "warehouse": "WH",
                    "database": "DB",
                    "role": "VIEWER_ROLE",
                    "user": "viewer.user",
                },
            },
        ):
            get_snowflake_connection("integration-1", streamlit_token="cookie-value")

    kwargs = fake_connector.connect.call_args.kwargs
    assert kwargs["account"] == "acc"
    assert kwargs["authenticator"] == "oauth"
    assert kwargs["token"] == "viewer-access-token"
    assert kwargs["warehouse"] == "WH"
    assert kwargs["database"] == "DB"
    assert kwargs["role"] == "VIEWER_ROLE"
    assert kwargs["user"] == "viewer.user"
