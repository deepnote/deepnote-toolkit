import json
import os
from contextlib import contextmanager
from pathlib import Path
from unittest import mock
from urllib.parse import quote

import pandas as pd
import pytest
from dotenv import load_dotenv
from trino import dbapi
from trino.auth import BasicAuthentication

from deepnote_toolkit import env as dnenv
from deepnote_toolkit.sql.sql_execution import execute_sql


@contextmanager
def use_trino_sql_connection(connection_json, env_var_name="TEST_TRINO_CONNECTION"):
    dnenv.set_env(env_var_name, connection_json)
    try:
        yield env_var_name
    finally:
        dnenv.unset_env(env_var_name)


@pytest.fixture(scope="module")
def trino_credentials():
    env_path = Path(__file__).parent.parent.parent / ".env"

    if env_path.exists():
        load_dotenv(env_path)

    host = os.getenv("TRINO_HOST")
    port = os.getenv("TRINO_PORT", "8080")
    user = os.getenv("TRINO_USER")
    password = os.getenv("TRINO_PASSWORD")
    catalog = os.getenv("TRINO_CATALOG", "system")
    schema = os.getenv("TRINO_SCHEMA", "runtime")
    http_scheme = os.getenv("TRINO_HTTP_SCHEME", "https")

    if not host or not user:
        pytest.skip(
            "Trino credentials not found. "
            "Please set TRINO_HOST and TRINO_USER in .env file"
        )

    return {
        "host": host,
        "port": int(port),
        "user": user,
        "password": password,
        "catalog": catalog,
        "schema": schema,
        "http_scheme": http_scheme,
    }


@pytest.fixture(scope="module")
def trino_connection(trino_credentials):
    auth = None

    if trino_credentials["password"]:
        auth = BasicAuthentication(
            trino_credentials["user"], trino_credentials["password"]
        )

    conn = dbapi.connect(
        host=trino_credentials["host"],
        port=trino_credentials["port"],
        user=trino_credentials["user"],
        auth=auth,
        http_scheme=trino_credentials["http_scheme"],
        catalog=trino_credentials["catalog"],
        schema=trino_credentials["schema"],
    )

    try:
        yield conn
    finally:
        conn.close()


class TestTrinoConnection:
    """Test Trino database connection."""

    def test_connection_established(self, trino_connection):
        """Test that connection to Trino is established."""
        cursor = trino_connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == 1

        cursor.close()

    def test_show_catalogs(self, trino_connection):
        """Test listing available catalogs."""
        cursor = trino_connection.cursor()
        cursor.execute("SHOW CATALOGS")
        catalogs = cursor.fetchall()

        assert len(catalogs) > 0
        assert any("system" in str(catalog) for catalog in catalogs)

        cursor.close()


@pytest.fixture
def trino_toolkit_connection(trino_credentials):
    """Create a Trino connection JSON for deepnote toolkit."""
    username = quote(trino_credentials["user"], safe="")
    password_part = (
        f":{quote(trino_credentials['password'], safe='')}"
        if trino_credentials["password"]
        else ""
    )
    connection_url = (
        f"trino://{username}{password_part}"
        f"@{trino_credentials['host']}:{trino_credentials['port']}"
        f"/{trino_credentials['catalog']}/{trino_credentials['schema']}"
    )

    # Trino uses `qmark` paramstyle (`?` placeholders with list/tuple params), not pyformat, which is the default
    connection_json = json.dumps(
        {
            "url": connection_url,
            "params": {},
            "param_style": "qmark",
        }
    )

    with use_trino_sql_connection(connection_json) as env_var_name:
        yield env_var_name


class TestTrinoWithDeepnoteToolkit:
    """Test Trino connection using Toolkit's SQL execution."""

    def test_execute_sql_simple_query(self, trino_toolkit_connection):
        result = execute_sql(
            template="SELECT 1 as test_value",
            sql_alchemy_json_env_var=trino_toolkit_connection,
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert "test_value" in result.columns
        assert result["test_value"].iloc[0] == 1

    def test_execute_sql_with_jinja_template(self, trino_toolkit_connection):
        test_string = "test string"
        test_number = 123

        def mock_get_variable_value(variable_name):
            variables = {
                "test_string_var": test_string,
                "test_number_var": test_number,
            }
            return variables[variable_name]

        with mock.patch(
            "deepnote_toolkit.sql.jinjasql_utils._get_variable_value",
            side_effect=mock_get_variable_value,
        ):
            result = execute_sql(
                template="SELECT {{test_string_var}} as message, {{test_number_var}} as number",
                sql_alchemy_json_env_var=trino_toolkit_connection,
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "message" in result.columns
            assert "number" in result.columns
            assert result["message"].iloc[0] == test_string
            assert result["number"].iloc[0] == test_number

    def test_execute_sql_with_autodetection(self, trino_credentials):
        """
        Test execute_sql with auto-detection of param_style
        (regression reported in BLU-5135)

        This simulates the real-world scenario where the backend provides a connection
        JSON without explicit param_style, and Toolkit must auto-detect it.
        """

        username = quote(trino_credentials["user"], safe="")
        password_part = (
            f":{quote(trino_credentials['password'], safe='')}"
            if trino_credentials["password"]
            else ""
        )
        connection_url = (
            f"trino://{username}{password_part}"
            f"@{trino_credentials['host']}:{trino_credentials['port']}"
            f"/{trino_credentials['catalog']}/{trino_credentials['schema']}"
        )

        connection_json = json.dumps(
            {
                "url": connection_url,
                "params": {},
                # NO param_style - should auto-detect to `qmark` for Trino
            }
        )

        test_value = "test value"

        with (
            use_trino_sql_connection(
                connection_json, "TEST_TRINO_AUTODETECT"
            ) as env_var_name,
            mock.patch(
                "deepnote_toolkit.sql.jinjasql_utils._get_variable_value",
                return_value=test_value,
            ),
        ):
            result = execute_sql(
                template="SELECT {{test_var}} as detected",
                sql_alchemy_json_env_var=env_var_name,
            )

            assert isinstance(result, pd.DataFrame)
            assert len(result) == 1
            assert "detected" in result.columns
            assert result["detected"].iloc[0] == test_value
