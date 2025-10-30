import sqlparse


def is_single_select_query(sql_string):
    parsed_queries = sqlparse.parse(sql_string)

    # Check if there is only one query in the string
    if len(parsed_queries) != 1:
        return False

    # Check if the query is a SELECT statement
    return parsed_queries[0].get_type() == "SELECT"
