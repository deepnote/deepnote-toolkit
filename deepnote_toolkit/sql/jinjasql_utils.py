import re

import __main__
from jinja2 import meta

from .jinjasql import JinjaSql


def render_jinja_sql_template(template, param_style=None):
    """
    Renders a Jinja SQL template by turning it into a parametrized SQL query and a parameters dict.
    The output follow Python DB API 2.0 specification: https://peps.python.org/pep-0249/

    Args:
        template (str): The Jinja SQL template to render.
        param_style (str, optional): The parameter style to use. Defaults to "pyformat".
            Common styles: "qmark" (?), "format" (%s), "pyformat" (%(name)s)

    Returns:
        str: The rendered SQL query.
    """

    # Default to pyformat for backwards compatibility
    # Note: Some databases like Trino require "qmark" or "format" style
    effective_param_style = param_style if param_style is not None else "pyformat"

    escaped_template = _escape_jinja_template(template, effective_param_style)

    jinja_sql = JinjaSql(param_style=effective_param_style)
    parsed_content = jinja_sql.env.parse(escaped_template)
    required_variables = meta.find_undeclared_variables(parsed_content)
    jinja_sql_data = {
        variable_name: _get_variable_value(variable_name)
        for variable_name in required_variables
    }
    return jinja_sql.prepare_query(escaped_template, jinja_sql_data)


def _get_variable_value(variable_name):
    return getattr(__main__, variable_name)


def _escape_jinja_template(template, param_style: str = "pyformat"):
    # see https://github.com/sripathikrishnan/jinjasql/issues/28 and https://stackoverflow.com/q/8657508/2761695
    # we have to replace % by %% in the SQL query due to how SQL alchemy interprets %
    # but ONLY for param styles that use % (format and pyformat)
    # For other param styles (qmark), % has no special meaning
    # and should not be escaped (e.g., in date format strings like '%m-%d-%Y')
    if param_style in ("format", "pyformat"):
        # Only escape % if it's not part of a jinja block (not preceded by { or followed by })
        # we use lookbehind ?<= and lookahead ?= regex matchers to capture the { and } symbols
        return re.sub(r"(?<=[^{])%(?=[^}])", "%%", template)
    return template
