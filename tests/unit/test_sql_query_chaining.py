from unittest import TestCase

import sqlparse
from sqlparse.tokens import Keyword

from deepnote_toolkit.sql.sql_query_chaining import (
    add_limit_clause,
    extract_table_reference_from_token,
    extract_table_references,
    find_query_preview_references,
    unchain_sql_query,
)


class TestSqlQueryChaining(TestCase):
    def test_extract_table_reference_from_token_valid(self):
        """Test extract_table_reference_from_token with a valid token."""

        # Create a token with ttype and value attributes
        class MockToken:
            def __init__(self, ttype, value):
                self.ttype = ttype
                self.value = value

        token = MockToken(None, "table_name")

        # Test the function
        result = extract_table_reference_from_token(token)

        # Check the result
        expected = {"table_name"}
        self.assertEqual(result, expected)

    def test_extract_table_reference_from_token_keyword(self):
        """Test extract_table_reference_from_token with a keyword token."""

        # Create a token with keyword type
        class MockKeywordToken:
            def __init__(self, ttype, value):
                self.ttype = ttype
                self.value = value

        token = MockKeywordToken(Keyword, "FROM")

        # Test the function
        result = extract_table_reference_from_token(token)

        # Should return empty set for keywords
        self.assertEqual(result, set())

    def test_extract_table_reference_from_token_dml_keyword(self):
        """Test extract_table_reference_from_token with a DML keyword token."""

        # Create a token with DML keyword type
        class MockDMLToken:
            def __init__(self, ttype, value):
                self.ttype = ttype
                self.value = value

        token = MockDMLToken(sqlparse.tokens.DML, "SELECT")

        # Test the function
        result = extract_table_reference_from_token(token)

        # Should return empty set for DML keywords
        self.assertEqual(result, set())

    def test_extract_table_reference_from_token_ddl_keyword(self):
        """Test extract_table_reference_from_token with a DDL keyword token."""

        # Create a token with DDL keyword type
        class MockDDLToken:
            def __init__(self, ttype, value):
                self.ttype = ttype
                self.value = value

        token = MockDDLToken(sqlparse.tokens.DDL, "CREATE")

        # Test the function
        result = extract_table_reference_from_token(token)

        # Should return empty set for DDL keywords
        self.assertEqual(result, set())

    def test_extract_table_reference_from_token_invalid(self):
        """Test extract_table_reference_from_token with an invalid token."""

        # Create a token without ttype or value attributes
        class InvalidToken:
            pass

        token = InvalidToken()

        # Test the function
        result = extract_table_reference_from_token(token)

        # Should return empty set for invalid tokens
        self.assertEqual(result, set())

    def test_extract_table_reference_from_token_whitespace(self):
        """Test extract_table_reference_from_token with whitespace in the value."""

        # Create a token with whitespace in the value
        class MockToken:
            def __init__(self, ttype, value):
                self.ttype = ttype
                self.value = value

        token = MockToken(None, "  table_name  ")

        # Test the function
        result = extract_table_reference_from_token(token)

        # Should strip whitespace
        expected = {"table_name"}
        self.assertEqual(result, expected)

    # TESTS FOR: extract_table_references

    def test_extract_table_references_simple_from(self):
        """Test extract_table_references with a simple FROM clause."""
        query = "SELECT * FROM table1"
        result = extract_table_references(query)
        self.assertEqual(result, ["table1"])

    def test_extract_table_references_multiple_joins(self):
        """Test extract_table_references with multiple JOIN types."""
        query = """
        SELECT * FROM table1
        INNER JOIN table2 ON table1.id = table2.id
        LEFT JOIN table3 ON table2.id = table3.id
        RIGHT JOIN table4 ON table3.id = table4.id
        FULL JOIN table5 ON table4.id = table5.id
        CROSS JOIN table6
        """
        result = extract_table_references(query)
        expected = ["table1", "table2", "table3", "table4", "table5", "table6"]
        self.assertEqual(sorted(result), sorted(expected))

    def test_extract_table_references_subqueries(self):
        """Test extract_table_references with subqueries."""
        query = """
        SELECT * FROM (
            SELECT * FROM inner_table1
            JOIN inner_table2 ON inner_table1.id = inner_table2.id
        ) subq
        JOIN outer_table ON subq.id = outer_table.id
        """
        result = extract_table_references(query)
        expected = ["inner_table1", "inner_table2", "outer_table"]
        self.assertEqual(sorted(result), sorted(expected))

    def test_extract_table_references_edge_cases(self):
        """Test extract_table_references with edge cases."""
        # Empty query
        self.assertEqual(extract_table_references(""), [])

        # Invalid SQL
        self.assertEqual(extract_table_references("SELECT * INVALID SQL"), [])

        # Query without any table references
        self.assertEqual(extract_table_references("SELECT 1"), [])

    def test_extract_table_references_lowercase_keywords(self):
        """Test extract_table_references with lowercase SQL keywords."""
        # Test with lowercase FROM
        query1 = "SELECT * from table1"
        self.assertEqual(extract_table_references(query1), ["table1"])

        # Test with lowercase JOIN variants
        query2 = """
        SELECT * from table1
        inner join table2 ON table1.id = table2.id
        left join table3 ON table2.id = table3.id
        right join table4 ON table3.id = table4.id
        full join table5 ON table4.id = table5.id
        cross join table6
        """
        result2 = extract_table_references(query2)
        expected2 = ["table1", "table2", "table3", "table4", "table5", "table6"]
        self.assertEqual(sorted(result2), sorted(expected2))

        # Test with mixed case keywords
        query3 = "SELECT * FROM table1 join table2 ON table1.id = table2.id"
        result3 = extract_table_references(query3)
        expected3 = ["table1", "table2"]
        self.assertEqual(sorted(result3), sorted(expected3))

    # TESTS FOR: find_query_preview_references

    def test_find_query_preview_references_basic(self):
        """Test find_query_preview_references with simple references."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create mock query preview objects
        customers = DeepnoteQueryPreview(deepnote_query="SELECT * FROM customer_source")
        orders = DeepnoteQueryPreview(deepnote_query="SELECT * FROM order_source")

        # Add them to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "customers", customers)
            setattr(__main__, "orders", orders)

            # Test the function
            query = "SELECT c.name, o.order_id FROM customers c JOIN orders o ON c.id = o.customer_id"
            result = find_query_preview_references(query)

            # Verify that both references are found
            self.assertEqual(len(result), 2)
            # Check that both customers and orders are in the result dictionary
            self.assertIn("customers", result)
            self.assertIn("orders", result)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    def test_find_query_preview_references_nested(self):
        """Test find_query_preview_references with nested dependencies."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create mock query preview objects with dependencies
        level3 = DeepnoteQueryPreview(deepnote_query="SELECT id, value FROM base_table")
        level2 = DeepnoteQueryPreview(
            deepnote_query="SELECT id, value FROM level3 WHERE value > 0"
        )
        level1 = DeepnoteQueryPreview(
            deepnote_query="SELECT id, value FROM level2 ORDER BY value"
        )

        # Add them to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "level3", level3)
            setattr(__main__, "level2", level2)
            setattr(__main__, "level1", level1)

            # Test the function
            query = "SELECT * FROM level1 LIMIT 10"
            result = find_query_preview_references(query)

            # Verify that all three levels are found
            self.assertEqual(len(result), 3)
            # Check that all three levels are in the result dictionary
            self.assertIn("level1", result)
            self.assertIn("level2", result)
            self.assertIn("level3", result)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    def test_find_query_preview_references_circular(self):
        """Test find_query_preview_references with circular references."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create mock query preview objects with circular references
        circular1 = DeepnoteQueryPreview(deepnote_query="SELECT * FROM circular2")
        circular2 = DeepnoteQueryPreview(deepnote_query="SELECT * FROM circular1")

        # Add them to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "circular1", circular1)
            setattr(__main__, "circular2", circular2)

            # Test the function
            query = "SELECT * FROM circular1"
            result = find_query_preview_references(query)

            # Verify that both references are found without infinite recursion
            self.assertEqual(len(result), 2)
            self.assertIn("circular1", result)
            self.assertIn("circular2", result)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    def test_find_query_preview_references_no_references(self):
        """Test find_query_preview_references with no references."""
        # Test with a query that doesn't reference any DeepnoteQueryPreview objects
        query = "SELECT * FROM regular_table"
        result = find_query_preview_references(query)

        # Should return an empty dictionary
        self.assertEqual(result, {})

    def test_find_query_preview_references_non_select_query(self):
        """Test find_query_preview_references with non-SELECT queries."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create a mock query preview object
        table = DeepnoteQueryPreview(deepnote_query="SELECT * FROM source_table")

        # Add it to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "table", table)

            # Test with INSERT query
            insert_query = "INSERT INTO table VALUES (1, 2, 3)"
            result = find_query_preview_references(insert_query)
            # Should return an empty dictionary even though 'table' exists in __main__
            self.assertEqual(result, {})

            # Test with UPDATE query
            update_query = "UPDATE table SET col = 1 WHERE id = 2"
            result = find_query_preview_references(update_query)
            self.assertEqual(result, {})

            # Test with DELETE query
            delete_query = "DELETE FROM table WHERE id = 3"
            result = find_query_preview_references(delete_query)
            self.assertEqual(result, {})

            # Test with multiple queries
            multiple_queries = "SELECT * FROM table; SELECT * FROM other_table"
            result = find_query_preview_references(multiple_queries)
            self.assertEqual(result, {})
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    # TESTS FOR: unchain_sql_query

    def test_unchain_sql_query_no_references(self):
        """Test unchain_sql_query with no query preview references."""
        query = "SELECT * FROM table1"
        result = unchain_sql_query(query)
        self.assertEqual(result, query)

    def test_unchain_sql_query_with_references(self):
        """Test unchain_sql_query with query preview references."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create mock query preview objects in __main__
        sales_per_category = DeepnoteQueryPreview(
            deepnote_query="SELECT category_id, SUM(sales_amount) AS total_sales FROM sales GROUP BY category_id"
        )
        top_categories = DeepnoteQueryPreview(
            deepnote_query="SELECT category_id FROM sales_per_category WHERE total_sales > 10000"
        )

        # Add them to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "sales_per_category", sales_per_category)
            setattr(__main__, "top_categories", top_categories)

            # Test the function
            query = """SELECT
                        p.product_id,
                        p.product_name,
                        p.category_id,
                        sc.total_sales
                    FROM products p
                    JOIN sales_per_category sc ON p.category_id = sc.category_id
                    JOIN top_categories tc ON p.category_id = tc.category_id;
                    """

            result = unchain_sql_query(query)

            # Check that the result starts with WITH
            self.assertTrue(result.strip().startswith("WITH"))

            # Check that both CTEs are included
            sales_cte = "sales_per_category AS ("
            top_cte = "top_categories AS ("
            self.assertTrue(sales_cte in result)
            self.assertTrue(top_cte in result)

            # Check that the CTEs contain the correct SQL
            sales_sql = "SELECT category_id, SUM(sales_amount) AS total_sales FROM sales GROUP BY category_id"
            top_sql = (
                "SELECT category_id FROM sales_per_category WHERE total_sales > 10000"
            )
            self.assertTrue(sales_sql in result)
            self.assertTrue(top_sql in result)

            # Check that the original query is included at the end
            query_lines = [line.strip() for line in query.strip().split("\n")]
            for line in query_lines:
                if line:  # Skip empty lines
                    self.assertTrue(line in result)

            # Simplify the test to just check for presence of the expected elements
            self.assertTrue(result.strip().startswith("WITH"))
            self.assertTrue(sales_cte in result)
            self.assertTrue(top_cte in result)
            self.assertTrue(sales_sql in result)
            self.assertTrue(top_sql in result)

            # Make sure the original query is included at the end
            for line in query_lines:
                if line:  # Skip empty lines
                    self.assertTrue(line in result)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    def test_unchain_sql_query_with_deeper_dependencies(self):
        """Test unchain_sql_query with deeper nested dependencies."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create mock query preview objects with multiple levels of dependency
        raw_data = DeepnoteQueryPreview(
            deepnote_query="SELECT id, name, value FROM source_table WHERE active = TRUE"
        )
        filtered_data = DeepnoteQueryPreview(
            deepnote_query="SELECT id, name, value FROM raw_data WHERE value > 100"
        )
        aggregated_data = DeepnoteQueryPreview(
            deepnote_query="SELECT name, SUM(value) as total FROM filtered_data GROUP BY name"
        )

        # Add them to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "raw_data", raw_data)
            setattr(__main__, "filtered_data", filtered_data)
            setattr(__main__, "aggregated_data", aggregated_data)

            # Test the function
            query = (
                "SELECT name, total FROM aggregated_data ORDER BY total DESC LIMIT 10"
            )

            result = unchain_sql_query(query)

            # Check that the result starts with WITH
            self.assertTrue(result.strip().startswith("WITH"))

            # Check that all CTEs are included
            raw_cte = "raw_data AS ("
            filtered_cte = "filtered_data AS ("
            agg_cte = "aggregated_data AS ("
            self.assertTrue(raw_cte in result)
            self.assertTrue(filtered_cte in result)
            self.assertTrue(agg_cte in result)

            # Check that the CTEs contain the correct SQL
            raw_sql = "SELECT id, name, value FROM source_table WHERE active = TRUE"
            filtered_sql = "SELECT id, name, value FROM raw_data WHERE value > 100"
            agg_sql = (
                "SELECT name, SUM(value) as total FROM filtered_data GROUP BY name"
            )
            self.assertTrue(raw_sql in result)
            self.assertTrue(filtered_sql in result)
            self.assertTrue(agg_sql in result)

            # Check that the original query is included at the end
            self.assertTrue(query in result)

            # Check proper dependency ordering
            # raw_data should come before filtered_data reference
            # filtered_data should come before aggregated_data reference
            raw_cte_idx = result.find(raw_cte)
            filtered_sql_idx = result.find(filtered_sql)
            filtered_cte_idx = result.find(filtered_cte)
            agg_sql_idx = result.find(agg_sql)

            self.assertTrue(raw_cte_idx < filtered_sql_idx)
            self.assertTrue(filtered_cte_idx < agg_sql_idx)

            # Check that all CTEs come before the main query
            main_query_idx = result.find(query)
            self.assertTrue(raw_cte_idx < main_query_idx)
            self.assertTrue(filtered_cte_idx < main_query_idx)

            # Find the index of the aggregated_data CTE
            agg_cte_idx = result.find(agg_cte)
            self.assertTrue(agg_cte_idx < main_query_idx)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    def test_unchain_sql_query_with_no_deepnote_query(self):
        """Test unchain_sql_query with references that don't have a deepnote_query."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create a mock query preview object without a deepnote_query
        data = DeepnoteQueryPreview()  # No deepnote_query provided

        # Add to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "data", data)

            # Test the function
            query = "SELECT * FROM data"

            result = unchain_sql_query(query)

            # Should return the original query since there are no CTEs to add
            self.assertEqual(result, query)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    def test_unchain_sql_query_with_circular_dependencies(self):
        """Test unchain_sql_query with circular dependencies."""
        import __main__

        from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

        # Create mock query preview objects with circular dependencies
        # This may not make sense in real SQL, but we need to test how the function handles it
        table_a = DeepnoteQueryPreview(
            deepnote_query="SELECT * FROM table_b WHERE id > 10"
        )
        table_b = DeepnoteQueryPreview(
            deepnote_query="SELECT * FROM table_a WHERE name LIKE 'test%'"
        )

        # Add them to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "table_a", table_a)
            setattr(__main__, "table_b", table_b)

            # Test the function
            query = "SELECT * FROM table_a JOIN table_b ON table_a.id = table_b.id"

            result = unchain_sql_query(query)

            # Check that the result starts with WITH
            self.assertTrue(result.strip().startswith("WITH"))

            # Check that both CTEs are included
            self.assertTrue("table_a AS (" in result)
            self.assertTrue("table_b AS (" in result)

            # Check that the CTEs contain the correct SQL
            a_sql = "SELECT * FROM table_b WHERE id > 10"
            b_sql = "SELECT * FROM table_a WHERE name LIKE 'test%'"
            self.assertTrue(a_sql in result)
            self.assertTrue(b_sql in result)

            # Check that the original query is included at the end
            self.assertTrue(query in result)

            # With circular dependencies, either ordering can work
            # Just check that both CTEs come before the main query
            a_cte_idx = result.find("table_a AS (")
            b_cte_idx = result.find("table_b AS (")
            main_query_idx = result.find(query)

            self.assertTrue(a_cte_idx < main_query_idx)
            self.assertTrue(b_cte_idx < main_query_idx)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    def test_unchain_sql_query_with_pandas_dataframe(self):
        """Test unchain_sql_query with a pandas DataFrame instead of a DeepnoteQueryPreview."""
        import __main__
        import pandas as pd

        # Create a pandas DataFrame
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

        # Add to __main__
        original_vars = vars(__main__).copy()
        try:
            setattr(__main__, "df", df)

            # Test the function
            query = "SELECT * FROM df"

            result = unchain_sql_query(query)

            # Should return the original query since pandas DataFrames are not DeepnoteQueryPreview objects
            self.assertEqual(result, query)
        finally:
            # Restore original __main__ variables
            for key in list(vars(__main__).keys()):
                if key not in original_vars:
                    delattr(__main__, key)

    # TESTS FOR: add_limit_clause

    def test_add_limit_to_simple_query(self):
        """Test adding a LIMIT clause to a simple query without an existing LIMIT."""
        query = "SELECT * FROM table1"
        result = add_limit_clause(query)
        self.assertEqual(result, "SELECT * FROM table1\nLIMIT 100")

    def test_add_limit_with_custom_value(self):
        """Test adding a LIMIT clause with a custom limit value."""
        query = "SELECT * FROM table1"
        result = add_limit_clause(query, limit=50)
        self.assertEqual(result, "SELECT * FROM table1\nLIMIT 50")

    def test_add_limit_to_query_with_existing_limit(self):
        """Test adding a LIMIT clause to a query that already has a LIMIT."""
        query = "SELECT * FROM table1 LIMIT 10"
        result = add_limit_clause(query)
        self.assertEqual(
            result,
            "SELECT * FROM (\nSELECT * FROM table1 LIMIT 10\n) wrapped_deepnote_subquery LIMIT 100",
        )

    def test_add_limit_to_query_with_semicolon(self):
        """Test adding a LIMIT clause to a query that ends with a semicolon."""
        query = "SELECT * FROM table1;"
        result = add_limit_clause(query)
        self.assertEqual(result, "SELECT * FROM table1\nLIMIT 100;")

    def test_add_limit_to_query_with_existing_limit_and_semicolon(self):
        """Test adding a LIMIT clause to a query with both an existing LIMIT and a semicolon."""
        query = "SELECT * FROM table1 LIMIT 10;"
        result = add_limit_clause(query)
        self.assertEqual(
            result,
            "SELECT * FROM (\nSELECT * FROM table1 LIMIT 10\n) wrapped_deepnote_subquery LIMIT 100;",
        )

    def test_add_limit_to_complex_query(self):
        """Test adding a LIMIT clause to a more complex query."""
        query = """
        SELECT
            t1.id,
            t1.name,
            t2.value
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.id
        WHERE t1.active = TRUE
        ORDER BY t2.value DESC
        """
        result = add_limit_clause(query)
        # Don't test exact whitespace, just check that the LIMIT clause is added
        self.assertTrue(result.strip().endswith("LIMIT 100"))
        self.assertTrue("SELECT" in result)
        self.assertTrue("FROM table1 t1" in result)
        self.assertTrue("JOIN table2 t2 ON t1.id = t2.id" in result)
        self.assertTrue("WHERE t1.active = TRUE" in result)
        self.assertTrue("ORDER BY t2.value DESC" in result)
        # Check that newline is added before LIMIT
        self.assertTrue("\nLIMIT 100" in result)

    def test_add_limit_to_query_with_lowercase_limit(self):
        """Test adding a LIMIT clause to a query with a lowercase 'limit'."""
        query = "SELECT * FROM table1 limit 10"
        result = add_limit_clause(query)
        self.assertEqual(
            result,
            "SELECT * FROM (\nSELECT * FROM table1 limit 10\n) wrapped_deepnote_subquery LIMIT 100",
        )

    def test_add_limit_to_query_with_subquery_containing_limit(self):
        """Test adding a LIMIT clause to a query with a subquery that has a LIMIT."""
        query = "SELECT * FROM (SELECT * FROM inner_table LIMIT 5) t"
        result = add_limit_clause(query)
        self.assertEqual(
            result, "SELECT * FROM (SELECT * FROM inner_table LIMIT 5) t\nLIMIT 100"
        )

    def test_add_limit_to_query_with_whitespace(self):
        """Test adding a LIMIT clause to a query with extra whitespace."""
        query = "  SELECT  *  FROM  table1  "
        result = add_limit_clause(query)
        # Don't test exact whitespace, just check that the LIMIT clause is added
        self.assertTrue("SELECT" in result)
        self.assertTrue("FROM  table1" in result)
        self.assertTrue("LIMIT 100" in result)
        # Check that newline is added before LIMIT
        self.assertTrue("\nLIMIT 100" in result)

    def test_non_single_select_query_raises_exception(self):
        """Test that a non-single SELECT query raises an ExecuteSqlError."""
        # Multiple queries
        query1 = "SELECT * FROM table1; SELECT * FROM table2"
        with self.assertRaises(Exception) as context:
            add_limit_clause(query1)
        self.assertIn(
            "Invalid query type: Query Preview supports only a single SELECT statement",
            str(context.exception),
        )

        # Non-SELECT query
        query2 = "INSERT INTO table1 VALUES (1, 'test')"
        with self.assertRaises(Exception) as context:
            add_limit_clause(query2)
        self.assertIn(
            "Invalid query type: Query Preview supports only a single SELECT statement",
            str(context.exception),
        )
