import pytest
import sqlparse.engine.grouping
from sqlparse.exceptions import SQLParseError

from deepnote_toolkit.sql.sql_utils import (
    configure_sqlparse_limits,
    is_single_select_query,
    reset_sqlparse_limits,
)


class TestSqlparseLimits:
    @pytest.fixture(autouse=True)
    def disable_sqlparse_limits(self):
        """Ensure every test starts and ends with limits disabled."""
        configure_sqlparse_limits()
        yield
        configure_sqlparse_limits()

    @staticmethod
    def _build_large_select(num_columns: int = 5000) -> str:
        """Build a SELECT with enough columns to exceed the default 10,000 token limit."""
        columns = ", ".join(f"column_{i}" for i in range(num_columns))
        return f"SELECT {columns} FROM some_table"

    def test_disables_limits_by_default(self):
        assert sqlparse.engine.grouping.MAX_GROUPING_TOKENS is None
        assert sqlparse.engine.grouping.MAX_GROUPING_DEPTH is None

    def test_sets_custom_values(self):
        expected_tokens = 50_000
        expected_depth = 200
        configure_sqlparse_limits(
            max_grouping_tokens=expected_tokens, max_grouping_depth=expected_depth
        )
        assert sqlparse.engine.grouping.MAX_GROUPING_TOKENS == expected_tokens
        assert sqlparse.engine.grouping.MAX_GROUPING_DEPTH == expected_depth

    def test_restores_builtin_defaults(self):
        reset_sqlparse_limits()
        assert sqlparse.engine.grouping.MAX_GROUPING_TOKENS == 10_000
        assert sqlparse.engine.grouping.MAX_GROUPING_DEPTH == 100

    def test_large_query_parses_with_limits_disabled(self):
        large_query = self._build_large_select()
        assert is_single_select_query(large_query) is True

    def test_large_query_fails_with_default_limits(self):
        reset_sqlparse_limits()
        large_query = self._build_large_select()
        with pytest.raises(SQLParseError):
            is_single_select_query(large_query)
