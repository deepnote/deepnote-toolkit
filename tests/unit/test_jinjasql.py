from datetime import date
from importlib.resources import files
from pathlib import Path

import pytest
from jinja2 import DictLoader, Environment
from yaml import safe_load_all

from deepnote_toolkit.sql.jinjasql import JinjaSql

FIXTURES_ROOT = Path(str(files(__package__) / "fixtures"))

_DATA = {
    "etc": {
        "columns": "project, timesheet, hours",
        "lt": "<",
        "gt": ">",
    },
    "request": {
        "project": {"id": 123, "name": "Acme Project"},
        "project_id": 123,
        "days": ["mon", "tue", "wed", "thu", "fri"],
        "day": "mon",
        "start_date": date.today(),
    },
    "session": {"user_id": "sripathi"},
}


def test_import():
    """Test import functionality with macros."""
    utils = """
    {% macro print_where(value) -%}
    WHERE dummy_col = {{value}}
    {%- endmacro %}
    """
    source = """
    {% import 'utils.sql' as utils %}
    select * from dual {{ utils.print_where(100) }}
    """
    loader = DictLoader({"utils.sql": utils})
    env = Environment(loader=loader)

    j = JinjaSql(env)
    query, bind_params = j.prepare_query(source, _DATA)
    expected_query = "select * from dual WHERE dummy_col = %s"
    assert query.strip() == expected_query.strip()
    assert len(bind_params) == 1
    assert list(bind_params)[0] == 100


def test_include():
    """Test include functionality."""
    where_clause = """where project_id = {{request.project_id}}"""

    source = """
    select * from dummy {% include 'where_clause.sql' %}
    """
    loader = DictLoader({"where_clause.sql": where_clause})
    env = Environment(loader=loader)

    j = JinjaSql(env)
    query, bind_params = j.prepare_query(source, _DATA)
    expected_query = "select * from dummy where project_id = %s"
    assert query.strip() == expected_query.strip()
    assert len(bind_params) == 1
    assert list(bind_params)[0] == 123


def test_precompiled_template():
    """Test using precompiled templates."""
    source = "select * from dummy where project_id = {{ request.project_id }}"
    j = JinjaSql()
    query, _ = j.prepare_query(j.env.from_string(source), _DATA)
    expected_query = "select * from dummy where project_id = %s"
    assert query.strip() == expected_query.strip()


def test_large_inclause():
    """Test large IN clause with many parameters."""
    num_of_params = 50000
    alphabets = ["A"] * num_of_params
    source = "SELECT 'x' WHERE 'A' in {{alphabets | inclause}}"
    j = JinjaSql()
    query, bind_params = j.prepare_query(source, {"alphabets": alphabets})
    assert len(bind_params) == num_of_params
    assert query == "SELECT 'x' WHERE 'A' in (" + "%s," * (num_of_params - 1) + "%s)"


@pytest.mark.parametrize(
    ("table_name", "expected_query"),
    [
        ("users", 'select * from "users"'),
        (("myschema", "users"), 'select * from "myschema"."users"'),
        ('a"b', 'select * from "a""b"'),
        (("users",), 'select * from "users"'),
    ],
)
def test_identifier_filter(table_name, expected_query):
    """Test identifier filter with various table name formats."""
    j = JinjaSql()
    template = "select * from {{table_name | identifier}}"
    query, _ = j.prepare_query(template, {"table_name": table_name})
    assert query == expected_query


@pytest.mark.parametrize(
    ("table_name", "expected_query"),
    [
        ("users", "select * from `users`"),
        (("myschema", "users"), "select * from `myschema`.`users`"),
        ("a`b", "select * from `a``b`"),
    ],
)
def test_identifier_filter_backtick(table_name, expected_query):
    """Test identifier filter with backtick quote character."""
    j = JinjaSql(identifier_quote_character="`")
    template = "select * from {{table_name | identifier}}"
    query, _ = j.prepare_query(template, {"table_name": table_name})
    assert query == expected_query


def test_yaml_cases(subtests):
    """Test cases loaded from YAML fixtures."""
    file_path = FIXTURES_ROOT / "jinjasql_macros.yaml"

    with open(file_path, encoding="utf-8") as f:
        configs = list(safe_load_all(f))

    for config in configs:
        test_name = config["name"]
        source = config["template"]

        for param_style, expected_sql in config["expected_sql"].items():
            with subtests.test(name=test_name, param_style=param_style):
                jinja = JinjaSql(param_style=param_style)
                query, bind_params = jinja.prepare_query(source, _DATA)

                if "expected_params" in config:
                    if param_style in ("pyformat", "named"):
                        expected_params = config["expected_params"]["as_dict"]
                    else:
                        expected_params = config["expected_params"]["as_list"]
                    assert list(bind_params) == expected_params

                assert query.strip() == expected_sql.strip()
