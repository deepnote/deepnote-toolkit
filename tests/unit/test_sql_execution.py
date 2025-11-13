import datetime
import io
import json
import os
import unittest
from unittest import TestCase, mock

import duckdb
import pandas as pd
from parameterized import parameterized

from deepnote_toolkit.sql.sql_execution import (
    _sanitize_dataframe_for_parquet,
    execute_sql,
    execute_sql_with_connection_json,
)

from .helpers.testing_dataframes import testing_dataframes


class TestExecuteSql(TestCase):
    def test_duckdb_group_by_on_date(self):
        test_df = pd.DataFrame([{"d": datetime.date(2011, 1, 1)}])
        duckdb.register("test_df_view", test_df)

        os.environ["SQL_DEEPNOTE_DATAFRAME_SQL"] = (
            '{"url":"deepnote+duckdb:///:memory:","params":{},"param_style":"qmark"}'
        )

        result = execute_sql(
            """SELECT *
        FROM test_df group by d
        """,
            "SQL_DEEPNOTE_DATAFRAME_SQL",
        )

        assert result is not None
        self.assertEqual(len(result), 1, "Result should have exactly one row")

    def test_duckdb_concat_with_percentage_sign(self):
        from deepnote_toolkit.sql.duckdb_sql import _get_duckdb_connection

        test_df = pd.DataFrame([{"value": 25.5}])
        duckdb_conn = _get_duckdb_connection()
        duckdb_conn.register("test_df_concat", test_df)

        os.environ["SQL_DEEPNOTE_DATAFRAME_SQL"] = (
            '{"url":"deepnote+duckdb:///:memory:","params":{},"param_style":"qmark"}'
        )

        result = execute_sql(
            """SELECT
                concat(round(value, 1), '%') as percentage_string
            FROM test_df_concat
            """,
            "SQL_DEEPNOTE_DATAFRAME_SQL",
        )

        assert result is not None
        self.assertEqual(result.iloc[0]["percentage_string"], "25.5%")
        self.assertNotEqual(result.iloc[0]["percentage_string"], "25.5%%")

    def test_duckdb_defaults_to_qmark_param_style(self):
        os.environ["SQL_DEEPNOTE_DATAFRAME_SQL"] = (
            '{"url":"deepnote+duckdb:///:memory:","params":{},"param_style":null}'
        )

        result = execute_sql(
            "SELECT '%' as value",
            "SQL_DEEPNOTE_DATAFRAME_SQL",
        )

        assert result is not None
        self.assertEqual(result.iloc[0]["value"], "%")

    @mock.patch("deepnote_toolkit.sql.sql_execution._execute_sql_on_engine")
    @mock.patch("sqlalchemy.engine.create_engine")
    def test_delete_sql_that_doesnt_produce_a_dataframe(
        self, mocked_create_engine, execute_sql_on_engine
    ):
        mocked_create_engine.return_value = mock.Mock()
        execute_sql_on_engine.return_value = None

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        result = execute_sql(
            """DELETE FROM mock_table WHERE id = '1'""",
            "SQL_ENV_VAR",
        )

        self.assertIsNone(result)

    @mock.patch("deepnote_toolkit.sql.sql_caching._generate_cache_key")
    @mock.patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_sql_executed_with_audit_comment_but_hash_calculated_without_it(
        self,
        mocked_query_data_source,
        mocked_request_cache_info_from_webapp,
        mocked_generate_cache_key,
    ):
        mocked_request_cache_info_from_webapp.return_value = None

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        execute_sql(
            "SELECT * FROM users",
            "SQL_ENV_VAR",
            audit_sql_comment="/*audit_comment*/",
            sql_cache_mode="read_or_write",
        )

        # expect mocked_generate_cache_key to be called with a param that doesn't contain /*audit_comment*/
        mocked_generate_cache_key.assert_called_with("SELECT * FROM users", mock.ANY)

        # expect mocked_query_data_source to be called with param containing /*audit_comment*/
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM users/*audit_comment*/",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            mock.ANY,
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_return_variable_type_parameter(self, mocked_query_data_source):
        # Setup mock return value
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        # Test with default return_variable_type
        execute_sql(
            "SELECT * FROM test_table",
            "SQL_ENV_VAR",
        )
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM test_table",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "dataframe",
            mock.ANY,
        )

        # Test with explicit return_variable_type='query_preview'
        execute_sql(
            "SELECT * FROM test_table",
            "SQL_ENV_VAR",
            return_variable_type="query_preview",
        )
        # For query_preview, a LIMIT 100 clause is added to the query
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM test_table\nLIMIT 100",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "query_preview",
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_query_preview_preserves_trailing_inline_comment(
        self, mocked_query_data_source
    ):
        # Setup mock return value
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        # Test that trailing inline comment is preserved before LIMIT clause
        execute_sql(
            "SELECT * FROM test_table -- trailing",
            "SQL_ENV_VAR",
            return_variable_type="query_preview",
        )
        # For query_preview, a LIMIT 100 clause is added after the trailing comment
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM test_table -- trailing\nLIMIT 100",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "query_preview",
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_caching._generate_cache_key")
    @mock.patch("deepnote_toolkit.sql.sql_caching._request_cache_info_from_webapp")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_sql_executed_with_audit_comment_with_semicolon(
        self,
        mocked_query_data_source,
        mocked_request_cache_info_from_webapp,
        mocked_generate_cache_key,
    ):
        mocked_request_cache_info_from_webapp.return_value = None

        os.environ["SQL_ENV_VAR"] = (
            '{"url":"postgresql://postgres:postgres@localhost:5432/postgres","params":{ },"param_style":"qmark", "integration_id": "integration_1"}'
        )

        execute_sql(
            "SELECT * FROM users;",  # Semicolon at the end of the query
            "SQL_ENV_VAR",
            audit_sql_comment="/*audit_comment*/",
            sql_cache_mode="read_or_write",
        )

        # expect mocked_generate_cache_key to be called with a param that doesn't contain /*audit_comment*/
        mocked_generate_cache_key.assert_called_with("SELECT * FROM users;", mock.ANY)

        # expect mocked_query_data_source to be called with param containing /*audit_comment*/
        mocked_query_data_source.assert_called_with(
            "SELECT * FROM users/*audit_comment*/;",
            mock.ANY,
            mock.ANY,
            mock.ANY,
            "dataframe",
            mock.ANY,
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_execute_sql_with_connection_json_with_snowflake_private_key(
        self, mock_execute_sql_with_caching
    ):
        template = "SELECT * FROM table"
        sql_alchemy_json = json.dumps(
            {
                "url": "snowflake://CHRISARTMANN@2nginys-hu78995?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {
                    "snowflake_private_key": "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRQzFqcUV2dVJ0ajd5bDgKZ29PMTNqWkErak1yU1lReklUWnYva09vVUJvS3dlVFZWKzhYWXgwQzF0QmdRRXUycFZHUUR6ZmY1RFhyU3NBOAo4bWpSczROU3k3aVNpZDdlLzg2QTdoYXRpQ2Q0SDg1aEtabzFxaHRGOW9ob2dwa3Z5NDRZc2RQVWNvYlBNMjFRClB5bkxDdzBOM2kxRklJSjVzY2xRY2Q3ZDNmK3hqZU5SK3M0QkozZTN0SFNaUFB0a25ZZ3EzTUNKaFVNZGoxODQKZ2x4VUFSYnpIbzVFaUJhVmlqckY1R2tSeXpIZFE1MnRORUhaOEllckFxWUJUYVk4bkQwNlJzRHNKUUNPcXIxcQo2bnUzcjBZbmNsNmZyOFgxTTJ3ZU04YWZqL3lRYThnM1Q1dG9CTTYxbVpBRm1uUzcvTDhqdTMvMmRucnZKTkkyCi95OE1TN2RiQWdNQkFBRUNnZ0VBRkl2Z1N6NzVqZVVDU1pMdVJqdGE0Y2p1MkN2cENCaEVIdHg1aFB5N3F4S24Kb1BVNG01UklpTiszNlRkSUw1S2ttUUdxamNNM3p1UkF2bnBOeVIySzg3M0EvNDhXdlI4dlJ0YXE2UUhnT3U5RAozZFJsY3BsSTg0YWpoL1dhVWNHMExRWHRpN3lzNjVuNFR2MDI3NWJWUFYweXU2dE5nMmw1VE45TGNoOERmQnVsCkhHRzJhN2lXWWowUVllUEVKZ3ArMzRVVXBzbjZEdnFmRVUzOGZiN3hHclhiaW5YendleGRLbGNvK21hdmFKYW4KSE1WSzZ3RlgzdzVsbGlQaW5tbFFhOXFueGl5amJ3THBObGgzTFVmbjRnRnQxZllwNWVVV0E2a3FJTm5UMDAzcQpRcWtnTVJ6enQ0MVdTb0FvbDlXZnpDN2NNdm5YSzhkVjkxNlVKN3d0d1FLQmdRRGFyenJkaGRIY0R2NjVYdm1jCldhSlNHQ3ZRdDhMbjUwK3liTVNGNFRXUnMvWGJibFhLUExiWHB2MTgyRVB1U0M1Umt1dVZjZDNlVXV2SmZzNGMKRHF3TGFIS2pIZ09lbE5jd0E1U3Z0eWdBTDRTNjBrcHB3TlNBQ1BPSDhHQ0haUlNuNDgrbVpMWnBINEFCWGwrQgpIaDgyWCt1RWR4M3I4UHUyenVRWUFnZ0UwUUtCZ1FEVWlaSHZhM2ZHcnoyNnRpQ2xYelFFZGRNOW1oTEdPaWtCCjN2Y3dVUWk1V3kwdFZtalpyN0JmWGxzVmY3OU5qRndjK1BqL0RYZjVCS29RcUY2ekZVdW9saG1yVVpSUWVmYk0KUC84K3ZxM015MXN6cXFZa1F5SVBoSDRiT1RWVFFmZ21vbWRPUGdPeVczLzJjSVJkb1RIS2c3L2NnY2lrSnoyNwo0cnlsTVZOMGF3S0JnUUNQSTUyRFBFRjJLZlovUFhSaTY2UzgyWWRzY2F2SkFYWUFFd083b2dMZllRenZXVlFjCk1RdDVNcHUvYVF0bDM2YzV5OUlhR3RNZjMrVG9HZkV0R2tsd21paFhMcUV0M3J6UGQ3aU9IM08yVTJRc3FOTCsKVDdLSUw5Ty95ZzVVOFV2STdPdVJQV0RNaEVyVUdvS20wQ0djQk1MekRNandFK2VlNitNTzk5MXAwUUtCZ1FDVgphLzZBZjJLZStiY0JYR2dKTzZ4N2NrYkg2VmxIcWI0SXhiT3RjVnNieldFdW5iQnJVdHhCd0RsekhQUG0xa1l3ClRFM3FLcEx0TEgxUDVyOWxVaFIxK3NraksrQ0V6NnBXSUt3WGRjRUUyUGRPbEt2bmxKY09wOHhzNFVSL08wTDIKRG5sb2hhcmRxdnlFeXNnVWQyNWsvVWxYQXB1SDVOcS9EQUlxZFVwQjd3S0JnR0tXYlltNE9kL2FGNzlsQUlGbwpEc09VU3YvUFhIQkNOM2xOSFYvaGF4dDlPKzhGL0NMQTBYVnpaRjdESDdScXBXd0NrL2xLa0c5c2lUNE1jK2JJCk9UZEtXOFdOYXptZ1dyVmpQYU9jdENrbnN5SUFJK0phZnB5Qys3cGZkVFdpL1dUUTFneTVUN3JjRFFDTURRZG0KbzlES2NRc3dPeldFa05qOW9NeExUTUlBCi0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0="
                },
                "param_style": "pyformat",
            }
        )

        execute_sql_with_connection_json(template, sql_alchemy_json)

        args, _ = mock_execute_sql_with_caching.call_args

        self.assertEqual(args[0], "SELECT * FROM table")
        self.assertEqual(
            args[2],
            {
                "url": "snowflake://CHRISARTMANN@2nginys-hu78995?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {"connect_args": {"private_key": mock.ANY}},
                "param_style": "pyformat",
            },
        )

    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_execute_sql_with_connection_json_with_snowflake_encrypted_private_key(
        self, mock_execute_sql_with_caching
    ):
        template = "SELECT * FROM table"
        sql_alchemy_json = json.dumps(
            {
                "url": "snowflake://CHRISARTMANN@2nginys-hu78995?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {
                    # This is a base-64 encoded private key that's been encrypted with the passphrase.
                    "snowflake_private_key": "LS0tLS1CRUdJTiBFTkNSWVBURUQgUFJJVkFURSBLRVktLS0tLQpNSUlGSkRCV0Jna3Foa2lHOXcwQkJRMHdTVEF4QmdrcWhraUc5dzBCQlF3d0pBUVFKZUdPYkVYdjY2VXFGem5BCmNBUzBFd0lDQ0FBd0RBWUlLb1pJaHZjTkFna0ZBREFVQmdncWhraUc5dzBEQndRSWJNT1BRQTE5cEhFRWdnVEkKc0NzQnpHOVlqRVRtZS80V2kxYWpyYkFCeDJPSU9UbXBONnJTWEVDcWkxeWNVb3JlWXlxKy9wRVF5WVNzNzBMdQpJenBpVk5zenJZRDAxOFZlOTlXZVU0UGdYb1QrN0dDWWFDWDZERGVIbVhCNzlzSkFmcWYvKzZiRTJUZEIyY2FkCmJ5bjNsOXZBbEd6UmVTSmtrcG5BdnR2YlhpZXpuaGFlWXE1SEtESmFBRnZNL0htdFlsemVCY3JNQW91NmlsSUYKRjgyQzF2TTVTekVRUmhxblN1RU03K2NVTUd4c2R6aHZoSTkwWWRRY0NJVXNsTnE2YkhGUHIyTmFoeGwvRGxKZQozRnY5M3hZSG5pa0tCN01hdGJNK1FQcGVBMnJXOFl4aWdRVTd2WHc2R1lNYkdvS1QwTDczZlo4dXh6UzcxeENJCndVMFR1S0tuN0ZjRW9CaXZIQ0NoNEJpZUpjSk5xdzJNSGJDc2ZmTXlSNUZOZUFSOG5tNjZuSUlURGdLb3NJczQKMVZWTjlKeUNDVk16WGxrNk80bkhaU2JZK0llblNWdE9kaGNpR2U2Zmp5TEFTMlJwekJOMExKb1FueDVnK1JpNgpMTGh1RE1GSDdybFZnT3hWeFY3RWNFeHJuZWF5R1E5andDYnNVNktENFpNeVhXWnhMNVdMbzdHU21lWkZHZmZNCk9MNnBWZXhhdjVpaXQzRzBHUkNNSWhZRjhMb2xidHRXYzB4dldrSjQ2UTJ5am9lcFVwa3IrQ0Q0THNraERkUCsKZ3RFVzEzc2xqbHNCbUxTbzdPMnRlY3ZqTnV4RlFpSU4vZ0x6bHZESTFMQUZVc0k3T1NWOVNRWmRUZXZ1SnZNUgpncDc0Q3N5dWV6US9rZUJhMStWYkltWVRsMDE1czNIeldrSHRjcTg0dS90cm56N1JqTFhKNUdobG9XSTRjL1BwCk15UXlYMWZqMnlzaURERWE4M05wTGNBZWNOYS9mNmpjVm5kRnBWWmlVcWtVaE1JajdPT0VwQUdjNVhILy9ObWYKRFp6UUxFd0xReFR6dlhZaDR4TGRkS3ZFNkovZXI3RUpMczFISGRyeDlxaG85T3dEUjdxZWRNQWxjVFp1Mm1OWgpJREpnM2J6aUF3RlF0MXRxVjV3Rkd6cGNMdFFtblo4dm9od2kzZWx2b2RkaUI2WGpPY3N2QmtqWFRjaEZvSEJyCmdCQUxSU1h6dmxObnN2ZG45UnlCYUtlUnJOL3RKYnhKeHdHa2ZYUkJWZzlTV2R6ZVNsNDE4OXJGaDkrbWR3Y2kKbmcrUTBPZkp6elNTY3cxYXpQVmc0NEd3ejBSTEdwS2FIZmpiSDM3SzltTDJQckJvYmZLdEd4SnNmZ0dxMTFZbAplb3I4bUszbEV3Qm5XL1NQd3dzcEl4QXhoN0x0VEVacTh0V2tjQlMrUWlmQ2Z4L1VVcWQyeGRFNk1XT29JT2ZsCkc1Ti9XdUdhMTREcUZKSDl6R0t4VTBLKzA1K0YwRUZkei9Tc01PQUNtM1VKQXc1UVhUR0ZEV3UyK1M3SW5IRXMKd1R4UGhMWjhKQnZjNHdoS0g3WGNkaEl0UUZhaHY4MWpoNEUwS1ZmUXRvejNEOVowdEFSRnF5K1NIM2JrWlJNSgprczFJWlJ6S1JLdi9ZUlpuaDVYK3VPc2N2Wm1NOHFrU3ZOZWtqdGV6WVo2NjAxWWNnMnRGcDFxYm4xODNVakFVCjZyRHFBQTVKOVRpZ3VyK1Q0TmhKUnZYSEM0Tkp2aklXdXNXSzVWR0E5ZlhKa1UrL1B4anBaOVF4QkozWUxraisKMHpjdU5FdHV5TUN6MFlhcWdRSmd4RUd1cEFrQXlNUlVTM1pvbk5jaEsySkJhekcyN2dGemtMSzF5RTdmQU41SQo0VW1jQ0luOWJkQlMybUFvYXlQU0VURFo2SHNSSU1NbDV3cG5ORkpaM3U4LytraTJjeDlQeXBYbHl3WWUzb0I3CjdnZGw3VjJpL1pHSjNtWld2OUFnZGJwWVBpTkVDSUduTGVpQUdYL2dzRXNlUG9xUERSUURuWGdUZjZOODBLbmUKdm5rb3hsdEE0emRGMVFManU1aXpreUx0NGZZV0dZV2pTLytMN3NqSDhxYktydjRQZFcxbWw1MW16ZEZrQ1pmdAowaEFCbWJMRDV4UkZMSVBoT2tHVjFiRmxQVGFoVHdEbAotLS0tLUVORCBFTkNSWVBURUQgUFJJVkFURSBLRVktLS0tLQ==",
                    "snowflake_private_key_passphrase": "0*>f-REO1p#1N[1/^",
                },
                "param_style": "pyformat",
            }
        )

        execute_sql_with_connection_json(template, sql_alchemy_json)

        args, _ = mock_execute_sql_with_caching.call_args

        self.assertEqual(args[0], "SELECT * FROM table")
        self.assertEqual(
            args[2],
            {
                "url": "snowflake://CHRISARTMANN@2nginys-hu78995?warehouse=&role=&application=Deepnote_Workspaces",
                "params": {"connect_args": {"private_key": mock.ANY}},
                "param_style": "pyformat",
            },
        )


class TestTrinoParamStyleAutoDetection(TestCase):
    """Tests for auto-detection of param_style for Trino connections"""

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_trino_url_auto_detects_qmark_param_style(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that Trino URLs automatically get 'qmark' param_style when not specified"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "trino://user@localhost:8080/catalog",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with 'qmark' param_style
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertEqual(call_args[2], "qmark")

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_non_trino_url_param_style_remains_none(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that non-Trino databases don't get auto-detected param_style"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "postgresql://user:pass@localhost:5432/mydb",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with None param_style
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertIsNone(call_args[2])

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_explicit_param_style_not_overridden(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that explicitly set param_style is preserved and not auto-detected"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        # Trino URL with explicit pyformat - should NOT be changed to qmark
        sql_alchemy_json = json.dumps(
            {
                "url": "trino://user@localhost:8080/catalog",
                "params": {},
                "param_style": "pyformat",
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with 'pyformat', NOT 'qmark'
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertEqual(call_args[2], "pyformat")

    @mock.patch("deepnote_toolkit.sql.sql_execution.compile_sql_query")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_trino_url_with_protocol_suffix_not_matched(
        self, mocked_query_data_source, mocked_compile_sql_query
    ):
        """Test that Trino URL variants like trino+rest:// don't match (drivername must be exactly 'trino')"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_compile_sql_query.return_value = (
            "SELECT * FROM test_table",
            {},
            "SELECT * FROM test_table",
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "trino+rest://user@localhost:8080/catalog",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json("SELECT * FROM test_table", sql_alchemy_json)

        # Verify compile_sql_query was called with None param_style
        # because "trino+rest" doesn't match "trino" in the dictionary
        mocked_compile_sql_query.assert_called_once()
        call_args = mocked_compile_sql_query.call_args[0]
        self.assertIsNone(call_args[2])

    @mock.patch("deepnote_toolkit.sql.sql_execution.render_jinja_sql_template")
    @mock.patch("deepnote_toolkit.sql.sql_execution._query_data_source")
    def test_trino_with_jinja_templates_uses_qmark(
        self, mocked_query_data_source, mocked_render_jinja
    ):
        """Test that Trino queries with Jinja templates correctly use qmark style"""
        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_query_data_source.return_value = mock_df
        mocked_render_jinja.return_value = (
            "SELECT * FROM test_table WHERE id = ?",
            [123],
        )

        sql_alchemy_json = json.dumps(
            {
                "url": "trino://user@localhost:8080/catalog",
                "params": {},
                "integration_id": "test_integration",
            }
        )

        execute_sql_with_connection_json(
            "SELECT * FROM test_table WHERE id = {{ user_id }}",
            sql_alchemy_json,
        )

        # Verify render_jinja_sql_template was called with 'qmark' param_style
        mocked_render_jinja.assert_called()
        call_args = mocked_render_jinja.call_args[0]
        self.assertEqual(call_args[1], "qmark")

        # Verify bind_params is a list (qmark style) not dict (pyformat style)
        call_args = mocked_query_data_source.call_args[0]
        bind_params = call_args[1]
        self.assertIsInstance(bind_params, list)
        self.assertEqual(bind_params, [123])

    @mock.patch("pandas.read_sql_query")
    def test_list_bind_params_converted_to_tuple_for_pandas(self, mocked_read_sql):
        """Test that list bind_params are converted to tuple for pandas.read_sql_query"""
        from deepnote_toolkit.sql.sql_execution import _execute_sql_on_engine

        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_read_sql.return_value = mock_df

        # Mock engine and connection
        mock_engine = mock.Mock()
        mock_connection = mock.Mock()
        mock_engine.begin.return_value.__enter__ = mock.Mock(
            return_value=mock_connection
        )
        mock_engine.begin.return_value.__exit__ = mock.Mock(return_value=None)

        # Test with list bind_params (qmark style for Trino)
        list_params = [123, "test"]
        _execute_sql_on_engine(
            mock_engine,
            "SELECT * FROM test_table WHERE id = ? AND name = ?",
            list_params,
        )

        # Verify pandas.read_sql_query was called
        self.assertTrue(mocked_read_sql.called)

        # Get the params argument passed to pandas.read_sql_query
        call_kwargs = mocked_read_sql.call_args[1]
        params_arg = call_kwargs.get("params")

        # Verify that list was converted to tuple
        self.assertIsInstance(params_arg, tuple)
        self.assertEqual(params_arg, (123, "test"))

    @mock.patch("pandas.read_sql_query")
    def test_dict_bind_params_not_converted_for_pandas(self, mocked_read_sql):
        """Test that dict bind_params remain as dict for pandas.read_sql_query"""
        from deepnote_toolkit.sql.sql_execution import _execute_sql_on_engine

        mock_df = pd.DataFrame({"col1": [1, 2, 3]})
        mocked_read_sql.return_value = mock_df

        # Mock engine and connection
        mock_engine = mock.Mock()
        mock_connection = mock.Mock()
        mock_engine.begin.return_value.__enter__ = mock.Mock(
            return_value=mock_connection
        )
        mock_engine.begin.return_value.__exit__ = mock.Mock(return_value=None)

        # Test with dict bind_params (pyformat style)
        dict_params = {"id": 123, "name": "test"}
        _execute_sql_on_engine(
            mock_engine,
            "SELECT * FROM test_table WHERE id = %(id)s AND name = %(name)s",
            dict_params,
        )

        # Verify pandas.read_sql_query was called
        self.assertTrue(mocked_read_sql.called)

        # Get the params argument passed to pandas.read_sql_query
        call_kwargs = mocked_read_sql.call_args[1]
        params_arg = call_kwargs.get("params")

        # Verify that dict was NOT converted (remains as dict)
        self.assertIsInstance(params_arg, dict)
        self.assertEqual(params_arg, {"id": 123, "name": "test"})


class TestSanitizeDataframe(unittest.TestCase):
    @parameterized.expand([(key, df) for key, df in testing_dataframes.items()])
    def test_all_dataframes_serialize_to_parquet(self, key, df):
        # these are skipped because we are not expecting it's possible for dataframes like these
        # to come out of a SQL query
        skipped_dataframes = {
            "categorical_columns",
            "nested_list_column",
            "mixed_column_types",
            "multi_level_columns",
            "period_index",
            "non_serializable_values",
            "column_distinct_values",
        }

        if key in skipped_dataframes:
            return

        df_cleaned = df.copy()
        _sanitize_dataframe_for_parquet(df_cleaned)

        with io.BytesIO() as in_memory_file:
            try:
                df_cleaned.to_parquet(in_memory_file)
            except:  # noqa: E722
                self.fail(f"serializing to parquet failed for {key}")
