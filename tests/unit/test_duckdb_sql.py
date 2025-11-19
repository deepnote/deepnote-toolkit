import pandas as pd
import pytest

from deepnote_toolkit.sql.duckdb_sql import (
    _get_duckdb_connection,
    _set_sample_size,
    _set_scan_all_frames,
)


@pytest.fixture(scope="function")
def duckdb_connection():
    import deepnote_toolkit.sql.duckdb_sql as duckdb_sql_module

    # reset the connection to ensure a fresh one is created for each test
    duckdb_sql_module._DEEPNOTE_DUCKDB_CONNECTION = None
    conn = _get_duckdb_connection()

    try:
        yield conn
    finally:
        conn.close()
        duckdb_sql_module._DEEPNOTE_DUCKDB_CONNECTION = None


@pytest.mark.parametrize("extension_name", ["spatial", "excel"])
def test_extension_installed_and_loadable(duckdb_connection, extension_name):
    result = duckdb_connection.execute(
        f"SELECT installed FROM duckdb_extensions() WHERE extension_name = '{extension_name}'"
    ).fetchone()

    assert (
        result is not None
    ), f"{extension_name} extension should be found in duckdb_extensions()"
    assert result[0] is True, f"{extension_name} extension should be installed"

    loaded_result = duckdb_connection.execute(
        f"SELECT loaded FROM duckdb_extensions() WHERE extension_name = '{extension_name}'"
    ).fetchone()
    assert loaded_result[0] is True, f"{extension_name} extension should be loaded"


def test_connection_singleton_pattern():
    conn1 = _get_duckdb_connection()
    conn2 = _get_duckdb_connection()

    assert conn1 is conn2, "Connection should be a singleton"


def test_set_sample_size(duckdb_connection):
    _set_sample_size(duckdb_connection, 50000)
    result = duckdb_connection.execute(
        "SELECT value FROM duckdb_settings() WHERE name = 'pandas_analyze_sample'"
    ).fetchone()
    assert int(result[0]) == 50000


def test_set_scan_all_frames(duckdb_connection):
    _set_scan_all_frames(duckdb_connection, False)
    result = duckdb_connection.execute(
        "SELECT value FROM duckdb_settings() WHERE name = 'python_scan_all_frames'"
    ).fetchone()
    assert result[0] == "false"

    _set_scan_all_frames(duckdb_connection, True)
    result = duckdb_connection.execute(
        "SELECT value FROM duckdb_settings() WHERE name = 'python_scan_all_frames'"
    ).fetchone()
    assert result[0] == "true"


def test_excel_extension_roundtrip(duckdb_connection, tmp_path):
    test_data = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [95.5, 87.3, 91.2],
        }
    )
    duckdb_connection.register("test_table", test_data)
    excel_path = tmp_path / "test_data.xlsx"
    duckdb_connection.execute(
        f"COPY test_table TO '{excel_path}' WITH (FORMAT xlsx, HEADER true)"
    )
    duckdb_connection.unregister("test_table")

    assert excel_path.exists(), "Excel file should be created"

    # read with spatial extension
    result = duckdb_connection.execute(f"SELECT * FROM st_read('{excel_path}')").df()
    diff = test_data.compare(result)
    assert diff.empty, "Data should be the same"

    # read with excel extension
    result = duckdb_connection.execute(f"SELECT * FROM read_xlsx('{excel_path}')").df()
    diff = test_data.compare(result)
    assert diff.empty, "Data should be the same"
