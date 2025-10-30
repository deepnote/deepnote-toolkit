import pytest

from deepnote_toolkit.sql.url_utils import replace_user_pass_in_pg_url

# Test cases: (test_id, original_url, new_user, new_password, expected_url)
_test_cases = [
    (
        "with_port_preservation",
        "postgresql://olduser:oldpassword@localhost:5432/mydb",
        "newuser",
        "newpassword",
        "postgresql://newuser:newpassword@localhost:5432/mydb",
    ),
    (
        "without_port",
        "postgresql://olduser:oldpassword@localhost/mydb",
        "newuser",
        "newpassword",
        "postgresql://newuser:newpassword@localhost/mydb",
    ),
]


@pytest.mark.parametrize(
    ("original_url", "new_user", "new_password", "expected_url"),
    [case[1:] for case in _test_cases],
    ids=[case[0] for case in _test_cases],
)
def test_replace_user_pass_in_pg_url(
    original_url, new_user, new_password, expected_url
):
    """Test replacing user and password in PostgreSQL URLs."""
    modified_url = replace_user_pass_in_pg_url(original_url, new_user, new_password)
    assert modified_url == expected_url
