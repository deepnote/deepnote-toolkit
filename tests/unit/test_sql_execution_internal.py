import uuid
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from google.api_core.client_info import ClientInfo

from deepnote_toolkit.sql import sql_execution as se


def test_build_params_for_bigquery_oauth_ok():
    with mock.patch(
        "deepnote_toolkit.sql.sql_execution.bigquery.Client"
    ) as mock_client:
        mock_client_instance = mock.Mock()
        mock_client.return_value = mock_client_instance

        out = se._build_params_for_bigquery_oauth({"access_token": "t", "project": "p"})

        assert "connect_args" in out and "client" in out["connect_args"]
        assert out["connect_args"]["client"] is mock_client_instance

        mock_client.assert_called_once()
        call_kwargs = mock_client.call_args[1]
        assert call_kwargs["project"] == "p"
        assert "credentials" in call_kwargs
        assert "client_info" in call_kwargs

        client_info_arg = call_kwargs["client_info"]
        assert isinstance(client_info_arg, ClientInfo)
        assert client_info_arg.user_agent == "Deepnote/1.0.0 (GPN:Deepnote;production)"


def test_build_params_for_bigquery_oauth_missing():
    with pytest.raises(Exception) as excinfo:
        se._build_params_for_bigquery_oauth({"access_token": "", "project": ""})
    assert excinfo.type.__name__ == "BigQueryCredentialsError"
    assert "missing credentials" in str(excinfo.value)


def test_sanitize_dataframe_for_parquet_conversions():
    data = pd.DataFrame(
        {
            "u": [uuid.uuid4(), uuid.uuid4()],
            "c": np.array([1 + 2j, 3 + 0j], dtype=np.complex64),
            "b": [2**63, 2**63 + 5],
            "i": [1, 2],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    # Ensure conversions happened
    assert data["u"].dtype == object
    assert data["c"].dtype == object
    assert data["b"].dtype == object
    # Unaffected integer column should remain integer dtype
    assert pd.api.types.is_integer_dtype(data["i"]) is True


def test_create_sql_ssh_uri_no_ssh():
    with se._create_sql_ssh_uri(False, {}) as url:
        assert url is None


def test_create_sql_ssh_uri_missing_key(monkeypatch):
    def fake_get_env(name, default=None):
        if name == "PRIVATE_SSH_KEY_BLOB":
            return None
        return default

    # Patch env accessor used by module to avoid mutating process env
    monkeypatch.setattr(se.dnenv, "get_env", fake_get_env)
    with pytest.raises(Exception) as excinfo:
        with se._create_sql_ssh_uri(
            True,
            {
                "ssh_options": {"host": "h", "port": 22, "user": "u"},
                "url": "pg://h:1/db",
            },
        ):
            pass
    # Assert specific failure mode text from implementation
    msg = str(excinfo.value).lower()
    assert "private key" in msg and "missing" in msg
