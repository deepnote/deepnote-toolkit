"""Parser that strips a strict leading run of session-setup statements
(``USE WAREHOUSE ...``, ``USE ROLE ...``, ``SET ...``, ``ALTER SESSION ...``)
off a rendered SQL query so they can be executed as `setup_statements` on the
same connection as the main query.
"""

import re
from typing import Optional

# Allowlist of leading session-setup statement keywords. Each entry is the
# tuple of consecutive keywords (case-insensitive, whitespace between).
_SETUP_PREFIXES: tuple[tuple[str, ...], ...] = (
    ("USE", "WAREHOUSE"),
    ("USE", "DATABASE"),
    ("USE", "SCHEMA"),
    ("USE", "ROLE"),
    ("USE", "SECONDARY", "ROLES"),
    ("SET",),
    ("ALTER", "SESSION"),
)

# Placeholder pattern per param_style. JinjaSQL emits these for bound values.
_PLACEHOLDER_PATTERNS: dict[str, re.Pattern[str]] = {
    "pyformat": re.compile(r"%\([^)]+\)s"),
    "format": re.compile(r"%s"),
    "named": re.compile(r":[A-Za-z_][A-Za-z0-9_]*"),
    "numeric": re.compile(r":\d+"),
    "qmark": re.compile(r"\?"),
}


class SetupStatementError(ValueError):
    """Raised when a leading USE/SET/ALTER SESSION statement contains a
    Jinja-bound value, which can't be passed as a SQL parameter to most
    drivers (and isn't accepted by Snowflake's USE WAREHOUSE at all).
    """


def extract_setup_statements(
    rendered_query: str, param_style: Optional[str] = None
) -> tuple[list[str], str]:
    """Strip a strict leading run of session-setup statements off the input.

    Returns ``(setup_statements, remaining_query)``. ``setup_statements`` are
    the trimmed statement bodies (no trailing ``;``); ``remaining_query`` is
    the rest of the input from the first non-setup statement onwards.

    A statement is a setup-statement candidate iff its first non-whitespace,
    non-comment tokens match one of the allowlisted prefixes (``USE WAREHOUSE``,
    ``USE DATABASE``, ``USE SCHEMA``, ``USE ROLE``, ``USE SECONDARY ROLES``,
    ``SET``, ``ALTER SESSION``). Comparison is case-insensitive.

    The leading run ends at the first statement whose prefix is not in the
    allowlist; everything from there on is the remaining query.

    If the entire input is setup-only (no main query follows), the input is
    returned unchanged with an empty list — callers fall through to today's
    pandas-multi-statement behavior rather than silently swallowing the cell.

    If any candidate setup statement contains a bind placeholder for the
    given ``param_style`` (outside quoted regions), raises
    :class:`SetupStatementError`. The message points the caller at the
    explicit ``setup_statements=`` kwarg.
    """
    pos = 0
    n = len(rendered_query)
    extracted_ranges: list[tuple[int, int]] = []  # (start, end_excl_semicolon)

    while pos < n:
        new_pos = _skip_whitespace_and_comments(rendered_query, pos)
        if new_pos >= n:
            break
        stmt_start = new_pos

        match_end = _match_setup_prefix(rendered_query, stmt_start)
        if match_end is None:
            break

        stmt_end = _find_unquoted_semicolon(rendered_query, match_end)
        if stmt_end is None:
            # No closing semicolon — would consume the rest of the cell as a
            # setup statement. Don't extract; pass through unchanged.
            return [], rendered_query

        extracted_ranges.append((stmt_start, stmt_end))
        pos = stmt_end + 1

    if not extracted_ranges:
        return [], rendered_query

    # If nothing of substance follows the last extracted setup statement,
    # the whole cell was setup-only — pass through unchanged so the user
    # sees the original failure mode rather than a silent no-op.
    tail = rendered_query[pos:]
    if _skip_whitespace_and_comments(tail, 0) >= len(tail):
        return [], rendered_query

    setup_statements = [
        rendered_query[start:end].strip() for (start, end) in extracted_ranges
    ]

    placeholder_re = _PLACEHOLDER_PATTERNS.get(param_style or "pyformat")
    if placeholder_re is not None:
        for stmt in setup_statements:
            if _has_match_outside_quotes(stmt, placeholder_re):
                raise SetupStatementError(
                    "Templated values in leading USE/SET/ALTER SESSION "
                    "statements aren't supported. Either inline the value "
                    "(e.g. `USE WAREHOUSE prod`) or pass the dynamic "
                    "statement via the setup_statements= kwarg."
                )

    remaining_query = rendered_query[pos:]
    return setup_statements, remaining_query


def _skip_whitespace_and_comments(s: str, pos: int) -> int:
    n = len(s)
    while pos < n:
        c = s[pos]
        if c.isspace():
            pos += 1
        elif c == "-" and pos + 1 < n and s[pos + 1] == "-":
            nl = s.find("\n", pos + 2)
            pos = nl + 1 if nl != -1 else n
        elif c == "/" and pos + 1 < n and s[pos + 1] == "*":
            close = s.find("*/", pos + 2)
            pos = close + 2 if close != -1 else n
        else:
            break
    return pos


def _match_setup_prefix(s: str, pos: int) -> Optional[int]:
    """If ``s[pos:]`` starts (case-insensitive) with one of the allowlist
    prefixes followed by a word boundary, return the index right after the
    prefix. Otherwise None. Whitespace between consecutive keywords is allowed.
    """
    n = len(s)
    for prefix in _SETUP_PREFIXES:
        cur = pos
        ok = True
        for i, word in enumerate(prefix):
            if i > 0:
                cur = _skip_inline_whitespace(s, cur)
            wl = len(word)
            if cur + wl > n or s[cur : cur + wl].upper() != word:
                ok = False
                break
            cur += wl
        if not ok:
            continue
        # Must end on a word boundary so e.g. "SETUP" doesn't match "SET".
        if cur < n and (s[cur].isalnum() or s[cur] == "_"):
            continue
        return cur
    return None


def _skip_inline_whitespace(s: str, pos: int) -> int:
    n = len(s)
    while pos < n and s[pos] in " \t\r\n":
        pos += 1
    return pos


def _find_unquoted_semicolon(s: str, pos: int) -> Optional[int]:
    n = len(s)
    while pos < n:
        c = s[pos]
        if c == ";":
            return pos
        elif c == "'":
            pos = _skip_single_quoted(s, pos)
        elif c == '"':
            pos = _skip_double_quoted(s, pos)
        elif c == "$" and pos + 1 < n and s[pos + 1] == "$":
            pos = _skip_dollar_quoted(s, pos)
        elif c == "-" and pos + 1 < n and s[pos + 1] == "-":
            nl = s.find("\n", pos + 2)
            pos = nl + 1 if nl != -1 else n
        elif c == "/" and pos + 1 < n and s[pos + 1] == "*":
            close = s.find("*/", pos + 2)
            pos = close + 2 if close != -1 else n
        else:
            pos += 1
    return None


def _skip_single_quoted(s: str, pos: int) -> int:
    """``pos`` is at the opening ``'``. Returns position past the closing one,
    treating doubled ``''`` as an escaped quote."""
    n = len(s)
    pos += 1
    while pos < n:
        if s[pos] == "'":
            if pos + 1 < n and s[pos + 1] == "'":
                pos += 2
            else:
                return pos + 1
        else:
            pos += 1
    return n


def _skip_double_quoted(s: str, pos: int) -> int:
    n = len(s)
    pos += 1
    while pos < n:
        if s[pos] == '"':
            if pos + 1 < n and s[pos + 1] == '"':
                pos += 2
            else:
                return pos + 1
        else:
            pos += 1
    return n


def _skip_dollar_quoted(s: str, pos: int) -> int:
    """``pos`` is at the first ``$`` of ``$$``. Returns position past the
    closing ``$$`` (or EOF if missing)."""
    pos += 2
    close = s.find("$$", pos)
    return close + 2 if close != -1 else len(s)


def _has_match_outside_quotes(s: str, pattern: re.Pattern[str]) -> bool:
    """Check whether *pattern* matches anywhere in *s* outside of SQL string
    literals, double-quoted identifiers, ``$$``-quoted strings, or comments."""
    pos = 0
    n = len(s)
    while pos < n:
        c = s[pos]
        if c == "'":
            pos = _skip_single_quoted(s, pos)
        elif c == '"':
            pos = _skip_double_quoted(s, pos)
        elif c == "$" and pos + 1 < n and s[pos + 1] == "$":
            pos = _skip_dollar_quoted(s, pos)
        elif c == "-" and pos + 1 < n and s[pos + 1] == "-":
            nl = s.find("\n", pos + 2)
            pos = nl + 1 if nl != -1 else n
        elif c == "/" and pos + 1 < n and s[pos + 1] == "*":
            close = s.find("*/", pos + 2)
            pos = close + 2 if close != -1 else n
        else:
            if pattern.match(s, pos):
                return True
            pos += 1
    return False
