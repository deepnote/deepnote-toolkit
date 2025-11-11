import unittest

from deepnote_toolkit.sql.jinjasql_utils import (
    _escape_jinja_template,
    render_jinja_sql_template,
)


class TestEscapeJinjaTemplate(unittest.TestCase):
    def test_plain_sql_wildcard_gets_escaped(self):
        template = "SELECT * FROM users WHERE name LIKE '%kokso%'"

        escaped_template = _escape_jinja_template(template)

        self.assertEqual(
            escaped_template, "SELECT * FROM users WHERE name LIKE '%%kokso%%'"
        )

    # the non-jinja % symbol should get escaped to %% but the jinja {% should not be turned to {%%
    def test_jinja_template_escaped_correctly(self):
        template = """
        SELECT *
        FROM users
        WHERE name LIKE '%kokso%'
        {% if true %} AND name = 'lokso' {% endif %}
        """

        escaped_template = _escape_jinja_template(template)

        expected_result = """
        SELECT *
        FROM users
        WHERE name LIKE '%%kokso%%'
        {% if true %} AND name = 'lokso' {% endif %}
        """
        self.assertEqual(escaped_template, expected_result)

    # %k% can cause regex match group overlap if not done properly
    def test_one_char_between_two_percent_symbols(self):
        template = """
        SELECT *
        FROM users
        WHERE name LIKE '%k%'
        """

        escaped_template = _escape_jinja_template(template)

        expected_result = """
        SELECT *
        FROM users
        WHERE name LIKE '%%k%%'
        """
        self.assertEqual(escaped_template, expected_result)

    def test_no_op(self):
        template = "SELECT * FROM users"

        escaped_template = _escape_jinja_template(template)

        self.assertEqual(escaped_template, template)


class TestRenderTemplate(unittest.TestCase):
    def test_with_no_params(self):
        template = "SELECT * FROM users WHERE id = '123'"

        query, bind_params = render_jinja_sql_template(template)

        self.assertEqual(query, template)
        self.assertEqual(bind_params, {})

    def test_with_simple_param_uses_pyformat(self):
        template = """
        {% set user_id = 'test' %}
        SELECT * FROM users WHERE id = {{ user_id }}"""

        query, bind_params = render_jinja_sql_template(template)

        self.assertEqual(query.strip(), "SELECT * FROM users WHERE id = %(user_id_1)s")
        self.assertEqual(bind_params, {"user_id_1": "test"})

    def test_qmark_format(self):
        template = """
        {% set user_id = 'test' %}
        SELECT * FROM users WHERE id = {{ user_id }}"""

        query, bind_params = render_jinja_sql_template(template, param_style="qmark")

        self.assertEqual(query.strip(), "SELECT * FROM users WHERE id = ?")
        self.assertEqual(bind_params, ["test"])

    def test_qmark_escaping(self):
        template = "SELECT date_format(TIMESTAMP '2022-10-20 05:10:00', '%m-%d-%Y %H')"

        query, bind_params = render_jinja_sql_template(template, param_style="qmark")

        self.assertEqual(query, template)
        self.assertEqual(bind_params, [])

    def test_pyformat_escaping(self):
        query, bind_params = render_jinja_sql_template(
            "SELECT '% character'",
            param_style="pyformat",
        )

        self.assertEqual(query, "SELECT '%% character'")
        self.assertEqual(bind_params, {})

    def test_format_escaping(self):
        query, bind_params = render_jinja_sql_template(
            "SELECT '% character'",
            param_style="format",
        )

        self.assertEqual(query, "SELECT '%% character'")
        self.assertEqual(bind_params, [])


if __name__ == "__main__":
    unittest.main()
