import __main__
import sqlparse
from sqlparse.tokens import Keyword

from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview
from deepnote_toolkit.sql.sql_utils import is_single_select_query


def add_limit_clause(query: str, limit: int = 100):
    class ExecuteSqlError(Exception):
        pass

    # Chained SQL only supports single SELECT queries
    # NOTE: the rest of this function depends on this assumption
    if not is_single_select_query(query):
        raise ExecuteSqlError(
            "Invalid query type: Query Preview supports only a single SELECT statement"
        )

    # Remove any trailing semicolons for processing
    query = query.strip()
    has_semicolon = query.endswith(";")
    if has_semicolon:
        query = query[:-1].strip()

    statement = sqlparse.parse(query)[0]

    # Check for top-level LIMIT clause
    has_top_level_limit = False
    # Examine top-level tokens only (not going into nested queries)
    for token in statement.tokens:
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "LIMIT":
            has_top_level_limit = True
            break

    if has_top_level_limit:
        # If query already has a top-level LIMIT, wrap it; surround the query with new lines to avoid appending into SQL comments
        result = f"SELECT * FROM (\n{query}\n) wrapped_deepnote_subquery LIMIT {limit}"
    else:
        # If no top-level LIMIT exists, simply add it to the end; prefix with new line to avoid appending into SQL comments
        result = f"{query}\nLIMIT {limit}"

    # Add back the semicolon if it was present in the original query
    if has_semicolon:
        result += ";"

    return result


def extract_table_reference_from_token(token):
    """Extract table references from linear sequence of raw tokens."""

    # Exclude invalid tokens
    if not hasattr(token, "ttype") or not hasattr(token, "value"):
        return set()

    # Exclude SQL keywords
    # EXAMPLE: token with token.ttype = Keyword and token.value = 'FROM'
    if token.ttype in (Keyword, sqlparse.tokens.DML) or token.ttype in (
        Keyword,
        sqlparse.tokens.DDL,
    ):
        return set()

    return {token.value.strip()}


def extract_table_references(query):
    """Extract table references from SQL query including CTEs and subqueries."""
    table_references = set()

    try:
        parsed = sqlparse.parse(query)
    except Exception:
        return []

    # State to indicate the next token is a potential table name
    expect_table = False

    for statement in parsed:
        # Flattening the statement will let us process tokens in linear sequence meaning we won't have to process groups of tokens (Identifier or IdentifierList)
        for token in statement.flatten():
            if token.is_whitespace or token.ttype == sqlparse.tokens.Punctuation:
                continue

            if expect_table:
                table_references.update(extract_table_reference_from_token(token))
                expect_table = False  # reset state after table name is found
                continue

            if token.ttype is Keyword:
                normalized_token = token.normalized.upper()
                # Check if token is "FROM" or contains "JOIN"
                if normalized_token == "FROM" or "JOIN" in normalized_token:
                    expect_table = True

    return list(table_references)


def find_query_preview_references(
    query, query_preview_references=None, processed_queries=None
):
    """
    Recursively find all DeepnoteQueryPreview objects referenced in a SQL query.

    Args:
        query (str): The SQL query to analyze
        query_preview_references (list, optional): List to store found query preview objects.
            Defaults to None (creates a new list).
        processed_queries (set, optional): Set to track processed queries to avoid circular references.
            Defaults to None (creates a new set).

    Returns:
        dict: A dictionary of names of DeepnoteQueryPreview objects referenced in the query and their source queries
    """
    # Initialize the list and set if not provided
    if query_preview_references is None:
        query_preview_references = {}

    if processed_queries is None:
        processed_queries = set()

    # If query is None or already processed, return the current references
    if query is None or query in processed_queries:
        return query_preview_references

    # Add query to processed queries to prevent circular references
    processed_queries.add(query)

    # Chained SQL only supports single SELECT queries
    # NOTE: the rest of this function depends on this assumption
    if not is_single_select_query(query):
        return query_preview_references

    # Extract table references from the query
    table_references = extract_table_references(query)

    # Check each table reference
    for table_reference in table_references:
        # Check if the reference exists in the main module
        if hasattr(__main__, table_reference):
            variable_name = table_reference
            variable = getattr(__main__, table_reference)
            # If it's a QueryPreview object and not already in our list
            # Use any() with a generator expression to check if the variable is already in the list
            # This avoids using the pandas object in a boolean context
            if isinstance(variable, DeepnoteQueryPreview) and not any(
                id(variable) == id(ref) for ref in query_preview_references
            ):
                # Add it to our list
                query_preview_source = variable._deepnote_query
                query_preview_references[variable_name] = query_preview_source
                # Recursively check its query
                # Use explicit string check to avoid boolean context
                if query_preview_source is not None and len(query_preview_source) > 0:
                    find_query_preview_references(
                        query_preview_source,
                        query_preview_references,
                        processed_queries,
                    )

    return query_preview_references


def unchain_sql_query(query):
    """
    Unchain SQL query with DeepnoteQueryPreview objects as Common Table Expressions (CTEs).

    This function analyzes the provided SQL query for references to DeepnoteQueryPreview
    objects from the main module. It then constructs a new query that includes these
    references as CTEs, allowing for modular and reusable SQL components.

    Args:
        query (str): The SQL query to process

    Returns:
        str: The processed SQL query with DeepnoteQueryPreview references converted to CTEs.
             If no references are found, returns the original query unchanged.
    """
    query_preview_references = find_query_preview_references(query)

    # If no query preview references are found we can just return
    if not query_preview_references:
        return query

    # We need to reverse the list to ensure proper dependency order
    # since find_query_preview_references builds the list starting from the original query
    # and recursively goes deeper into dependencies
    reversed_query_preview_references = {
        query_name: query_value
        for query_name, query_value in reversed(list(query_preview_references.items()))
    }

    # Build CTEs from query preview references
    cte_parts = []
    for query_name, query_value in reversed_query_preview_references.items():
        # Add the reference's query as a CTE
        cte_parts.append(
            f"{query_name} AS (\n    {query_value.replace(';', '').strip()}\n)"
        )

    # If no CTEs were found, return the original query
    if not cte_parts:
        return query

    # Parse the query
    parsed = sqlparse.parse(query)[0]

    # Check if query already has WITH clause
    has_with = False
    for token in parsed.tokens:
        if token.is_keyword and token.normalized.upper() == "WITH":
            has_with = True
            break

    # We need to combine the existing CTEs with the ones that we want to add, in order to create a list of CTE definitions starting with a singular WITH statement
    if has_with:
        # Find the first CTE in the existing query
        first_cte = None
        for token in parsed.tokens:
            if token.is_group and any(
                t.is_keyword and t.normalized.upper() == "AS" for t in token.tokens
            ):
                first_cte = token
                break

        if first_cte:
            # Insert new CTEs before the first CTE. This is necessary as the CTE in the query that we're unchaining could itself contain a reference to a query_preview object
            our_ctes = ",\n".join(cte_parts) + ",\n"
            query_parts = query.split(str(first_cte), 1)
            final_query = query_parts[0] + our_ctes + str(first_cte) + query_parts[1]
            return final_query

    # If not, or if we couldn't find the first CTE, use the original approach
    cte_sql = "WITH " + ",\n".join(cte_parts)
    final_query = f"{cte_sql}\n{query.strip()}"
    return final_query
