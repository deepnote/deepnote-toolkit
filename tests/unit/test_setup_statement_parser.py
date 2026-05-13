import pytest

from deepnote_toolkit.sql.setup_statement_parser import (
    SetupStatementError,
    extract_setup_statements,
)


def test_extracts_single_use_warehouse():
    setup, remaining = extract_setup_statements("USE WAREHOUSE abc; SELECT 1")
    assert setup == ["USE WAREHOUSE abc"]
    assert remaining.strip() == "SELECT 1"


def test_extracts_multiple_setup_statements_in_order():
    setup, remaining = extract_setup_statements(
        "USE WAREHOUSE abc; USE ROLE r; SELECT 1"
    )
    assert setup == ["USE WAREHOUSE abc", "USE ROLE r"]
    assert remaining.strip() == "SELECT 1"


def test_recognises_all_allowlist_prefixes():
    cell = (
        "USE WAREHOUSE w; USE DATABASE d; USE SCHEMA s; "
        "USE ROLE r; USE SECONDARY ROLES ALL; "
        "SET v = 'x'; ALTER SESSION SET TIMEZONE = 'UTC'; "
        "SELECT 1"
    )
    setup, remaining = extract_setup_statements(cell)
    assert setup == [
        "USE WAREHOUSE w",
        "USE DATABASE d",
        "USE SCHEMA s",
        "USE ROLE r",
        "USE SECONDARY ROLES ALL",
        "SET v = 'x'",
        "ALTER SESSION SET TIMEZONE = 'UTC'",
    ]
    assert remaining.strip() == "SELECT 1"


def test_case_insensitive():
    setup, remaining = extract_setup_statements("use Warehouse abc; SeLeCt 1")
    assert setup == ["use Warehouse abc"]
    assert remaining.strip() == "SeLeCt 1"


def test_skips_leading_whitespace_and_newlines():
    setup, remaining = extract_setup_statements("\n\n  USE WAREHOUSE abc;\nSELECT 1")
    assert setup == ["USE WAREHOUSE abc"]
    assert "SELECT 1" in remaining


def test_skips_leading_line_comments():
    setup, remaining = extract_setup_statements(
        "-- pick the right wh\nUSE WAREHOUSE abc;\nSELECT 1"
    )
    assert setup == ["USE WAREHOUSE abc"]
    assert "SELECT 1" in remaining


def test_skips_leading_block_comments():
    setup, remaining = extract_setup_statements(
        "/* setup */ USE WAREHOUSE abc;\nSELECT 1"
    )
    assert setup == ["USE WAREHOUSE abc"]
    assert "SELECT 1" in remaining


def test_skips_comments_between_setup_statements():
    setup, remaining = extract_setup_statements(
        "USE WAREHOUSE abc;\n-- next\nUSE ROLE r;\nSELECT 1"
    )
    assert setup == ["USE WAREHOUSE abc", "USE ROLE r"]
    assert "SELECT 1" in remaining


def test_quoted_identifier_with_semicolon():
    setup, remaining = extract_setup_statements('USE WAREHOUSE "my;wh"; SELECT 1')
    assert setup == ['USE WAREHOUSE "my;wh"']
    assert remaining.strip() == "SELECT 1"


def test_string_literal_with_semicolon():
    setup, remaining = extract_setup_statements("SET v = 'a;b'; SELECT 1")
    assert setup == ["SET v = 'a;b'"]
    assert remaining.strip() == "SELECT 1"


def test_dollar_quoted_string_with_semicolon():
    setup, remaining = extract_setup_statements("SET v = $$a;b$$; SELECT 1")
    assert setup == ["SET v = $$a;b$$"]
    assert remaining.strip() == "SELECT 1"


def test_doubled_inner_quotes_in_identifier():
    setup, remaining = extract_setup_statements('USE WAREHOUSE "a""b;c"; SELECT 1')
    assert setup == ['USE WAREHOUSE "a""b;c"']
    assert remaining.strip() == "SELECT 1"


def test_no_extraction_when_first_statement_is_not_setup():
    setup, remaining = extract_setup_statements("SELECT * FROM t; USE WAREHOUSE abc")
    assert setup == []
    assert remaining == "SELECT * FROM t; USE WAREHOUSE abc"


def test_extraction_stops_at_first_non_setup_statement():
    setup, remaining = extract_setup_statements(
        "USE WAREHOUSE abc; SELECT 1; USE ROLE r"
    )
    assert setup == ["USE WAREHOUSE abc"]
    assert remaining.strip() == "SELECT 1; USE ROLE r"


def test_setup_only_cell_passes_through_unchanged():
    """If everything is setup with no main query the input is returned
    unchanged so the user sees the original failure mode rather than a
    silent no-op."""
    cell = "USE WAREHOUSE abc; USE ROLE r;"
    setup, remaining = extract_setup_statements(cell)
    assert setup == []
    assert remaining == cell


def test_setup_only_cell_with_trailing_comments_passes_through():
    cell = "USE WAREHOUSE abc;\n-- trailing comment\n"
    setup, remaining = extract_setup_statements(cell)
    assert setup == []
    assert remaining == cell


def test_no_setup_statements_returns_input_unchanged():
    cell = "SELECT 1"
    setup, remaining = extract_setup_statements(cell)
    assert setup == []
    assert remaining == cell


def test_unterminated_setup_statement_passes_through():
    """No closing ; on the leading USE — would otherwise consume the rest of
    the cell as a setup statement; safer to not extract."""
    cell = "USE WAREHOUSE abc"  # no semicolon
    setup, remaining = extract_setup_statements(cell)
    assert setup == []
    assert remaining == cell


def test_set_keyword_must_have_word_boundary():
    """`SETUP` must not match `SET`."""
    cell = "SETUP something_else; SELECT 1"
    setup, remaining = extract_setup_statements(cell)
    assert setup == []
    assert remaining == cell


@pytest.mark.parametrize(
    "param_style,placeholder",
    [
        ("pyformat", "%(p_0)s"),
        ("format", "%s"),
        ("named", ":p_0"),
        ("numeric", ":1"),
        ("qmark", "?"),
    ],
)
def test_raises_when_setup_statement_contains_placeholder(param_style, placeholder):
    """A templated value in a setup statement renders as a bind placeholder.
    `connection.exec_driver_sql` doesn't bind; raise so the caller knows to
    inline the value or pass it explicitly."""
    cell = f"USE WAREHOUSE {placeholder}; SELECT 1"
    with pytest.raises(SetupStatementError, match="setup_statements="):
        extract_setup_statements(cell, param_style)


def test_placeholder_inside_string_literal_does_not_trigger_error():
    """`%(x)s` inside a single-quoted string is literal text, not a placeholder."""
    setup, remaining = extract_setup_statements("SET v = '%(x)s'; SELECT 1", "pyformat")
    assert setup == ["SET v = '%(x)s'"]
    assert remaining.strip() == "SELECT 1"


def test_default_param_style_is_pyformat():
    """When ``param_style`` is None we still detect pyformat placeholders
    because that's JinjaSQL's default."""
    cell = "USE WAREHOUSE %(p_0)s; SELECT 1"
    with pytest.raises(SetupStatementError, match="setup_statements="):
        extract_setup_statements(cell, None)
