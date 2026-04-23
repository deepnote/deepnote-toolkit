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

    mock_pool_connection = mock.Mock()
    mock_pool_connection.dbapi_connection = mock_dbapi_connection
    mock_pool_connection.cursor.side_effect = (
        lambda: mock_pool_connection.dbapi_connection.cursor()
    )

    mock_sa_connection = mock.Mock(spec=sqlalchemy.engine.Connection)
    mock_sa_connection.connection = mock_pool_connection
    mock_sa_connection.in_transaction.return_value = False

    def mock_exec_driver_sql(sql: str, *args: Any) -> mock.Mock:
        cursor: mock.Mock = mock_sa_connection.connection.cursor()
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
    """Test that _execute_sql_on_engine cancels cursors on KeyboardInterrupt."""

    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = KeyboardInterrupt("Cancelled")

    mock_engine = _setup_mock_engine_with_cursor(mock_cursor)

    with pytest.raises(KeyboardInterrupt):
        se._execute_sql_on_engine(mock_engine, "SELECT 1", {})

    mock_cursor.cancel.assert_called_once()


def test_execute_sql_on_engine_handles_cancel_errors_gracefully():
    """Test that _execute_sql_on_engine handles cancel errors gracefully."""

    mock_cursor = mock.MagicMock()
    mock_cursor.execute.side_effect = KeyboardInterrupt("Cancelled")
    mock_cursor.cancel.side_effect = RuntimeError("Cancel failed")

    mock_engine = _setup_mock_engine_with_cursor(mock_cursor)

    # Should raise original KeyboardInterrupt, not the cancel error
    with pytest.raises(KeyboardInterrupt):
        se._execute_sql_on_engine(mock_engine, "SELECT 1", {})

    mock_cursor.cancel.assert_called_once()


def test_cursor_tracking_dbapi_connection_cancel_all_cursors():
    """Test that CursorTrackingDBAPIConnection.cancel_all_cursors cancels all tracked cursors."""
    mock_wrapped_conn = mock.Mock()
    cursor1 = mock.Mock()
    cursor2 = mock.Mock()
    mock_wrapped_conn.cursor.side_effect = [cursor1, cursor2]

    tracking_conn = se.CursorTrackingDBAPIConnection(mock_wrapped_conn)

    # Create two cursors
    tracking_conn.cursor()
    tracking_conn.cursor()

    # Cancel all cursors
    tracking_conn.cancel_all_cursors()

    cursor1.cancel.assert_called_once()
    cursor2.cancel.assert_called_once()


def test_cursor_tracking_dbapi_connection_handles_unhashable_cursor():
    """Test that CursorTrackingDBAPIConnection handles cursors that can't be added to weakset."""
    mock_wrapped_conn = mock.Mock()

    class UnhashableCursor:
        __hash__ = None

    unhashable_cursor = UnhashableCursor()
    mock_wrapped_conn.cursor.return_value = unhashable_cursor

    tracking_conn = se.CursorTrackingDBAPIConnection(mock_wrapped_conn)

    with mock.patch.object(se.logger, "warning") as mock_warning:
        result = tracking_conn.cursor()

    assert result is unhashable_cursor
    mock_warning.assert_called_once()
    assert "can't be added to weakset" in mock_warning.call_args[0][0]


def test_cursor_tracking_sqlalchemy_connection_handles_none_dbapi_connection():
    """Test that CursorTrackingSQLAlchemyConnection handles None dbapi connection."""
    mock_conn_pool = mock.Mock()
    mock_conn_pool.dbapi_connection = None

    mock_sa_conn = mock.Mock()
    mock_sa_conn.connection = mock_conn_pool

    with mock.patch.object(se.logger, "warning") as mock_warning:
        se.CursorTrackingSQLAlchemyConnection(mock_sa_conn)

    mock_warning.assert_called_once()
    assert "DBAPI connection is None" in mock_warning.call_args[0][0]


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


def test_sanitize_dataframe_for_parquet_decimal_large_numbers():
    """Large decimal.Decimal values must be converted to strings."""
    from decimal import Decimal

    data = pd.DataFrame(
        {
            "d": [Decimal("99999999999999999999999999999999"), Decimal("1.5")],
            "i": [1, 2],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["d"].dtype == object
    assert data["d"].iloc[0] == "99999999999999999999999999999999"
    assert data["d"].iloc[1] == "1.5"
    assert pd.api.types.is_integer_dtype(data["i"]) is True


def test_sanitize_dataframe_for_parquet_decimal_small_numbers():
    """Decimal values within float64 exact range should not be converted."""
    from decimal import Decimal

    data = pd.DataFrame(
        {
            "d": [Decimal("100"), Decimal("200")],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["d"].iloc[0] == Decimal("100")


def test_sanitize_dataframe_for_parquet_decimal_nan():
    """Decimal('NaN') must not crash the sanitizer."""
    from decimal import Decimal

    data = pd.DataFrame(
        {
            "d": [Decimal("NaN"), Decimal("42")],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["d"].iloc[1] == Decimal("42")


def test_is_large_number():
    from decimal import Decimal

    from deepnote_toolkit.ocelots.pandas.utils import is_large_number

    # 2**53 boundary: float64 can represent integers exactly up to 2**53
    assert is_large_number(2**53) is False
    assert is_large_number(2**53 + 1) is True
    assert is_large_number(-(2**53)) is False
    assert is_large_number(-(2**53) - 1) is True

    # Small integers should not trigger
    assert is_large_number(0) is False
    assert is_large_number(1) is False
    assert is_large_number(-1) is False
    assert is_large_number(42) is False

    # Large ints well beyond 2**53 should trigger
    assert is_large_number(2**63 - 1) is True
    assert is_large_number(2**63) is True
    assert is_large_number(10**18) is True

    # Floats
    assert is_large_number(float("inf")) is True
    assert is_large_number(float("nan")) is False
    assert is_large_number(1.0) is False

    # Decimals
    assert is_large_number(Decimal("1e40")) is True
    assert is_large_number(Decimal("9007199254740994")) is True
    assert is_large_number(Decimal("100")) is False
    assert is_large_number(Decimal("NaN")) is False
    assert is_large_number(Decimal("sNaN")) is False
    assert is_large_number(Decimal("Infinity")) is True

    # Non-numeric types should not trigger
    assert is_large_number("not a number") is False
    assert is_large_number(None) is False


def test_sanitize_dataframe_for_parquet_large_int_precision_loss():
    """Integers above 2**53 must be converted to strings to preserve precision."""
    val_above = 2**53 + 1  # 9007199254740993
    val_exact = 2**53  # 9007199254740992

    data = pd.DataFrame(
        {
            "lossy": [val_above, val_exact],
            "safe": [42, 100],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["lossy"].dtype == object
    assert data["lossy"].iloc[0] == str(val_above)
    assert data["lossy"].iloc[1] == str(val_exact)
    assert pd.api.types.is_integer_dtype(data["safe"])


def test_sanitize_dataframe_for_parquet_large_int_negative():
    """Negative integers beyond -2**53 must also be converted."""
    data = pd.DataFrame(
        {
            "neg": [-(2**53) - 1, 0],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["neg"].dtype == object
    assert data["neg"].iloc[0] == str(-(2**53) - 1)


def test_sanitize_dataframe_for_parquet_int_at_boundary():
    """Integers exactly at 2**53 should not be converted (still exact in float64)."""
    data = pd.DataFrame(
        {
            "boundary": [2**53, -(2**53)],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert pd.api.types.is_integer_dtype(data["boundary"])


def test_sanitize_dataframe_for_parquet_mixed_int_with_none():
    """Mixed object column with None and large int should convert to strings."""
    data = pd.DataFrame(
        {
            "mixed": pd.array([2**53 + 1, None, 42], dtype=object),
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["mixed"].dtype == object
    assert data["mixed"].iloc[0] == str(2**53 + 1)


def test_sanitize_dataframe_for_parquet_decimal_int_precision_loss():
    """Integer-valued Decimals above 2**53 should be converted to strings."""
    from decimal import Decimal

    data = pd.DataFrame(
        {
            "d": [Decimal("9007199254740993"), Decimal("42")],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["d"].dtype == object
    assert data["d"].iloc[0] == str(Decimal("9007199254740993"))


def test_sanitize_dataframe_for_parquet_precision_loss_preserves_value():
    """Verify the string conversion preserves the exact integer value."""
    val = 9007199254740993
    assert float(val) == float(9007199254740992)  # proves precision loss in float64

    data = pd.DataFrame({"x": [val]})
    se._sanitize_dataframe_for_parquet(data)
    assert data["x"].iloc[0] == "9007199254740993"  # exact value preserved


def test_sanitize_dataframe_for_parquet_very_large_int():
    """Integers far beyond 2**53 (e.g. 2**64) must also be converted."""
    data = pd.DataFrame(
        {
            "huge": [2**64, 42],
        }
    )
    se._sanitize_dataframe_for_parquet(data)
    assert data["huge"].dtype == object
    assert data["huge"].iloc[0] == str(2**64)


def test_create_sql_ssh_uri_no_ssh():
    with se._create_sql_ssh_uri(False, {}) as url:
        assert url is None


def _make_sql_alchemy_dict(integration_id="integration_a", url=None, params=None):
    return {
        "url": url or "postgresql://u:p@localhost:5432/db",
        "params": params if params is not None else {},
        "param_style": "qmark",
        "integration_id": integration_id,
    }


def test_acquire_engine_outside_session_owns_resources():
    sql_alchemy_dict = _make_sql_alchemy_dict()

    with mock.patch(
        "deepnote_toolkit.sql.sql_execution.create_engine", return_value=mock.Mock()
    ) as create_engine_mock:
        engine_a, ssh_a, owns_a = se._acquire_engine(sql_alchemy_dict)
        engine_b, ssh_b, owns_b = se._acquire_engine(sql_alchemy_dict)

    assert owns_a is True
    assert owns_b is True
    assert ssh_a is None
    assert ssh_b is None
    # Two calls outside a session create two engines; caller disposes each.
    assert create_engine_mock.call_count == 2
    # Outside a session we don't impose pool_size on the user.
    kwargs = create_engine_mock.call_args.kwargs
    assert "pool_size" not in kwargs
    assert "max_overflow" not in kwargs
    assert kwargs["pool_pre_ping"] is True


def test_sql_session_reuses_engine_within_block():
    sql_alchemy_dict = _make_sql_alchemy_dict()
    fake_engine = mock.Mock()

    with mock.patch(
        "deepnote_toolkit.sql.sql_execution.create_engine", return_value=fake_engine
    ) as create_engine_mock:
        with se.sql_session():
            engine_a, _, owns_a = se._acquire_engine(sql_alchemy_dict)
            engine_b, _, owns_b = se._acquire_engine(sql_alchemy_dict)

    assert engine_a is fake_engine
    assert engine_b is fake_engine
    assert owns_a is False
    assert owns_b is False
    assert create_engine_mock.call_count == 1
    # In-session engines must use a single-connection pool so engine.begin()
    # returns the same physical DBAPI connection across calls — that is what
    # makes session state (USE WAREHOUSE, ...) persist between execute_sql
    # calls in the same block.
    kwargs = create_engine_mock.call_args.kwargs
    assert kwargs["pool_size"] == 1
    assert kwargs["max_overflow"] == 0
    assert kwargs["pool_pre_ping"] is True
    # On exit the session disposes the engine it owned.
    fake_engine.dispose.assert_called_once()


def test_sql_session_separates_engines_per_integration():
    dict_a = _make_sql_alchemy_dict(integration_id="int_a")
    dict_b = _make_sql_alchemy_dict(integration_id="int_b")
    engines = [mock.Mock(name="engine_a"), mock.Mock(name="engine_b")]

    with mock.patch(
        "deepnote_toolkit.sql.sql_execution.create_engine", side_effect=engines
    ):
        with se.sql_session():
            engine_a, _, _ = se._acquire_engine(dict_a)
            engine_b, _, _ = se._acquire_engine(dict_b)
            engine_a_again, _, _ = se._acquire_engine(dict_a)

    assert engine_a is engines[0]
    assert engine_b is engines[1]
    assert engine_a_again is engines[0]


def test_sql_session_opens_ssh_tunnel_once_and_closes_on_exit():
    sql_alchemy_dict = _make_sql_alchemy_dict()
    sql_alchemy_dict["ssh_options"] = {
        "enabled": True,
        "host": "h",
        "port": 22,
        "user": "u",
    }
    fake_engine = mock.Mock()
    fake_server = mock.Mock()
    fake_server.is_active = True
    rewritten_url = "postgresql://u:p@127.0.0.1:65000/db"

    with (
        mock.patch(
            "deepnote_toolkit.sql.sql_execution._open_ssh_tunnel",
            return_value=(fake_server, rewritten_url),
        ) as open_tunnel,
        mock.patch(
            "deepnote_toolkit.sql.sql_execution.create_engine",
            return_value=fake_engine,
        ) as create_engine_mock,
    ):
        with se.sql_session():
            _, ssh_a, owns_a = se._acquire_engine(sql_alchemy_dict)
            _, ssh_b, owns_b = se._acquire_engine(sql_alchemy_dict)

    # Tunnel opened once and reused; not torn down between calls.
    assert open_tunnel.call_count == 1
    assert create_engine_mock.call_count == 1
    assert ssh_a is fake_server
    assert ssh_b is fake_server
    assert owns_a is False
    assert owns_b is False
    # Engine creation uses the rewritten (tunneled) URL.
    assert create_engine_mock.call_args.args[0] == rewritten_url
    # On session exit both the engine and the tunnel are torn down.
    fake_engine.dispose.assert_called_once()
    fake_server.close.assert_called_once()


def test_sql_session_skips_sharing_when_user_supplies_pool_config():
    sql_alchemy_dict = _make_sql_alchemy_dict(params={"pool_size": 5})

    with mock.patch(
        "deepnote_toolkit.sql.sql_execution.create_engine", return_value=mock.Mock()
    ) as create_engine_mock:
        with se.sql_session():
            _, _, owns_a = se._acquire_engine(sql_alchemy_dict)
            _, _, owns_b = se._acquire_engine(sql_alchemy_dict)

    # Without sharing, caller owns disposal each call and we don't override the
    # user's pool config (their pool_size=5 passes through unchanged).
    assert owns_a is True
    assert owns_b is True
    assert create_engine_mock.call_count == 2
    kwargs = create_engine_mock.call_args.kwargs
    assert kwargs["pool_size"] == 5


def test_sql_session_swallows_per_resource_teardown_errors():
    sql_alchemy_dict = _make_sql_alchemy_dict()
    failing_engine = mock.Mock()
    failing_engine.dispose.side_effect = RuntimeError("boom")
    failing_server = mock.Mock()
    failing_server.is_active = True
    failing_server.close.side_effect = RuntimeError("boom")

    with (
        mock.patch(
            "deepnote_toolkit.sql.sql_execution.create_engine",
            return_value=failing_engine,
        ),
        mock.patch(
            "deepnote_toolkit.sql.sql_execution._open_ssh_tunnel",
            return_value=(failing_server, "postgresql://u@127.0.0.1:1/db"),
        ),
        mock.patch.object(se.logger, "warning") as mock_warning,
    ):
        sql_alchemy_dict["ssh_options"] = {
            "enabled": True,
            "host": "h",
            "port": 22,
            "user": "u",
        }
        with se.sql_session():
            se._acquire_engine(sql_alchemy_dict)

    failing_engine.dispose.assert_called_once()
    failing_server.close.assert_called_once()
    # One warning per failing resource.
    assert mock_warning.call_count == 2


def test_nested_sql_session_does_not_steal_outer_resources():
    sql_alchemy_dict = _make_sql_alchemy_dict()
    fake_engine = mock.Mock()

    with mock.patch(
        "deepnote_toolkit.sql.sql_execution.create_engine", return_value=fake_engine
    ) as create_engine_mock:
        with se.sql_session():
            engine_outer, _, _ = se._acquire_engine(sql_alchemy_dict)
            with se.sql_session():
                engine_inner, _, _ = se._acquire_engine(sql_alchemy_dict)
            # Inner block must not have disposed the outer-owned engine.
            fake_engine.dispose.assert_not_called()
            engine_after_inner, _, _ = se._acquire_engine(sql_alchemy_dict)

    assert engine_outer is fake_engine
    assert engine_inner is fake_engine
    assert engine_after_inner is fake_engine
    assert create_engine_mock.call_count == 1
    # Outer session disposes once on exit.
    fake_engine.dispose.assert_called_once()


def test_acquire_engine_closes_tunnel_when_engine_creation_fails_outside_session():
    sql_alchemy_dict = _make_sql_alchemy_dict()
    sql_alchemy_dict["ssh_options"] = {
        "enabled": True,
        "host": "h",
        "port": 22,
        "user": "u",
    }
    fake_server = mock.Mock()
    fake_server.is_active = True

    with (
        mock.patch(
            "deepnote_toolkit.sql.sql_execution._open_ssh_tunnel",
            return_value=(fake_server, "postgresql://u@127.0.0.1:1/db"),
        ),
        mock.patch(
            "deepnote_toolkit.sql.sql_execution.create_engine",
            side_effect=RuntimeError("bad url"),
        ),
    ):
        with pytest.raises(RuntimeError):
            se._acquire_engine(sql_alchemy_dict)

    # Tunnel must not be left open if engine construction blows up.
    fake_server.close.assert_called_once()


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
