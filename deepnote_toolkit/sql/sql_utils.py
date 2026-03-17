from typing import Optional

import sqlparse


def is_single_select_query(sql_string):
    parsed_queries = sqlparse.parse(sql_string)

    # Check if there is only one query in the string
    if len(parsed_queries) != 1:
        return False

    # Check if the query is a SELECT statement
    return parsed_queries[0].get_type() == "SELECT"


def configure_sqlparse_limits(
    max_grouping_tokens: Optional[int] = None,
    max_grouping_depth: Optional[int] = None,
) -> None:
    """Disable or adjust sqlparse's grouping limits for large analytical queries.

    sqlparse v0.5.4 started capping token count at 10,000 by default.
    Since the toolkit runtime is isolated and users write their own queries,
    we disable limits by default.

    See: https://github.com/andialbrecht/sqlparse/blob/0.5.4/docs/source/api.rst#security-and-performance-considerations
    """
    try:
        import sqlparse.engine.grouping

        sqlparse.engine.grouping.MAX_GROUPING_TOKENS = max_grouping_tokens
        sqlparse.engine.grouping.MAX_GROUPING_DEPTH = max_grouping_depth
    except (ImportError, AttributeError):
        pass


def reset_sqlparse_limits() -> None:
    """Restore sqlparse grouping limits to their built-in defaults."""
    configure_sqlparse_limits(max_grouping_tokens=10_000, max_grouping_depth=100)
