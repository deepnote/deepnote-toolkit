import sys

import duckdb
from duckdb_extensions import import_extension
from packaging.version import Version

_DEEPNOTE_DUCKDB_CONNECTION = None
_DEFAULT_DUCKDB_SAMPLE_SIZE = 20_000


def execute_duckdb_sql(query, bind_params):
    """
    Executes a SQL query using DuckDB and returns the result as a DataFrame.

    Args:
        query (str): The SQL query to execute.
        bind_params (dict): A dictionary of parameters to bind to the query.
    """
    duckdb_connection = _get_duckdb_connection()
    try:
        return duckdb_connection.execute(query, parameters=bind_params).df()
    except duckdb.InvalidInputException:
        # duckdb raises a InvalidInputException when it cannot correctly infer
        # a column type from the first 1000 rows. We'll retry the query with a larger sample size
        # https://stackoverflow.com/q/75352219/2761695
        try:
            _set_sample_size(duckdb_connection, sys.maxsize)
            return duckdb_connection.execute(query, parameters=bind_params).df()
        finally:
            _set_sample_size(duckdb_connection, _DEFAULT_DUCKDB_SAMPLE_SIZE)


def _get_duckdb_connection():
    """
    Returns a connection to the DuckDB database - singleton pattern.

    If a connection has already been established, it returns the existing connection.
    Otherwise, it creates a new connection to an in-memory database and installs the spatial extension.

    Returns:
      duckdb.Connection: A connection to the DuckDB database.
    """
    global _DEEPNOTE_DUCKDB_CONNECTION

    if not _DEEPNOTE_DUCKDB_CONNECTION:
        _DEEPNOTE_DUCKDB_CONNECTION = duckdb.connect(
            database=":memory:", read_only=False
        )

        # DuckDB extensions are loaded from included wheels to prevent loading them
        # from the internet on every notebook start
        #
        # Install and load the spatial extension. Primary use case: reading xlsx files
        # e.g. SELECT * FROM st_read('excel.xlsx')
        # there is also official excel extension, which mentions that Excel support from spatial extension
        # may be removed in the future (see: https://duckdb.org/docs/stable/core_extensions/excel)
        for extension_name in ["spatial", "excel"]:
            import_extension(
                name=extension_name, force_install=True, con=_DEEPNOTE_DUCKDB_CONNECTION
            )
            _DEEPNOTE_DUCKDB_CONNECTION.execute(f"LOAD '{extension_name}'")

        _set_sample_size(_DEEPNOTE_DUCKDB_CONNECTION, _DEFAULT_DUCKDB_SAMPLE_SIZE)

        # Since 1.1.0, DuckDB finds only frames from current scope by default.
        # `python_scan_all_frames` restores old behavior of scanning all frames in a replacement scan.
        # https://github.com/duckdb/duckdb/issues/14961#issuecomment-2577942761
        if Version(duckdb.__version__) >= Version("1.1.0"):
            _set_scan_all_frames(_DEEPNOTE_DUCKDB_CONNECTION, True)

    return _DEEPNOTE_DUCKDB_CONNECTION


def _set_sample_size(conn: duckdb.DuckDBPyConnection, sample_size: int) -> None:
    conn.execute(f"SET GLOBAL pandas_analyze_sample={sample_size}")


def _set_scan_all_frames(
    conn: duckdb.DuckDBPyConnection, scan_all_frames: bool
) -> None:
    conn.execute(f"SET python_scan_all_frames={scan_all_frames}")
