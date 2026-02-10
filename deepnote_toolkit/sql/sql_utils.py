from functools import lru_cache

import sqlparse


def is_single_select_query(sql_string):
    parsed_queries = _cached_sqlparse_parse(sql_string)
    # Check if there is only one query in the string

    # Check if there is only one query in the string
    if len(parsed_queries) != 1:
        return False

    # Check if the query is a SELECT statement
    return parsed_queries[0].get_type() == "SELECT"


# LRU cache for SQL parsing for up to 64 distinct queries
@lru_cache(maxsize=64)
def _cached_sqlparse_parse(sql_string: str):
    return sqlparse.parse(sql_string)
