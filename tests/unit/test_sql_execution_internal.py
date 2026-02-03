import uuid
from typing import Any
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from google.api_core.client_info import ClientInfo

from deepnote_toolkit.sql import sql_execution as se


def _setup_mock_engine_with_cursor(mock_cursor: mock.Mock) -> mock.Mock:
    """Helper to set up mock engine and connection with a custom cursor.

    Returns mock_engine that can be passed to _execute_sql_on_engine.
    """
    import sqlalchemy

    mock_dbapi_connection: mock.Mock = mock.Mock()
    mock_dbapi_connection.cursor.return_value = mock_cursor

    mock_sa_connection = mock.Mock(spec=sqlalchemy.engine.Connection)
    mock_sa_connection._dbapi_connection = mock_dbapi_connection
    mock_sa_connection.connection = mock_dbapi_connection
    mock_sa_connection.in_transaction.return_value = False

    def mock_exec_driver_sql(sql: str, *args: Any) -> mock.Mock:
        cursor: mock.Mock = mock_sa_connection._dbapi_connection.cursor()
        cursor.execute(sql, *args)
        return cursor

    mock_sa_connection.exec_driver_sql = mock_exec_driver_sql

    mock_engine = mock.Mock()
    mock_engine.begin.return_value.__enter__ = mock.Mock(
        return_value=mock_sa_connection
    )
    mock_engine.begin.return_value.__exit__ = mock.Mock(return_value=False)

    return mock_engine


def test_bigquery_wait_or_cancel_handles_keyboard_interrupt():
    import google.cloud.bigquery._job_helpers as _job_helpers

    mock_job = mock.Mock()
    mock_job.result.side_effect = KeyboardInterrupt("User interrupted")
    mock_job.cancel = mock.Mock()

    with pytest.raises(KeyboardInterrupt):
        # _wait_or_cancel should be monkeypatched by `_monkeypatch_bigquery_wait_or_cancel`
        _job_helpers._wait_or_cancel(
            job_obj=mock_job,
            api_timeout=30.0,
            wait_timeout=60.0,
            retry=None,
            page_size=None,
            max_results=None,
        )

    mock_job.cancel.assert_called_once_with(retry=None, timeout=30.0)


def test_execute_sql_on_engine_cancels_cursor_on_keyboard_interrupt():
    """Test that _execute_sql_on_engine cancels cursors on KeyboardInterrupt.

    Uses real pandas.read_sql_query. The cursor's execute method throws
    KeyboardInterrupt, simulating cell cancellation.
    """
    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = KeyboardInterrupt("Cancelled")

    mock_engine = _setup_mock_engine_with_cursor(mock_cursor)

    with pytest.raises(KeyboardInterrupt):
        se._execute_sql_on_engine(mock_engine, "SELECT 1", {})

    mock_cursor.cancel.assert_called_once()


def test_execute_sql_on_engine_cancels_bigquery_query_job():
    """Test that _execute_sql_on_engine cancels BigQuery query_job if present.

    Uses real pandas.read_sql_query. The cursor's execute method throws
    KeyboardInterrupt, simulating cell cancellation.
    """
    mock_query_job = mock.Mock()
    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = KeyboardInterrupt("Cancelled")
    mock_cursor.query_job = mock_query_job

    mock_engine = _setup_mock_engine_with_cursor(mock_cursor)

    with pytest.raises(KeyboardInterrupt):
        se._execute_sql_on_engine(mock_engine, "SELECT 1", {})

    mock_query_job.cancel.assert_called_once()
    mock_cursor.cancel.assert_called_once()


def test_execute_sql_on_engine_handles_cancel_errors_gracefully():
    """Test that _execute_sql_on_engine handles cancel errors gracefully.

    Uses real pandas.read_sql_query. The cursor's execute method throws
    KeyboardInterrupt. The cursor's cancel method throws an error, which
    should be silently handled.
    """
    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = KeyboardInterrupt("Cancelled")
    mock_cursor.cancel.side_effect = RuntimeError("Cancel failed")

    mock_engine = _setup_mock_engine_with_cursor(mock_cursor)

    # Should raise original KeyboardInterrupt, not the cancel error
    with pytest.raises(KeyboardInterrupt):
        se._execute_sql_on_engine(mock_engine, "SELECT 1", {})

    mock_cursor.cancel.assert_called_once()


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
