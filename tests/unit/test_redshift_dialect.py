"""Check the published Redshift dialects used by the two SQLAlchemy branches."""

import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import pytest
import sqlalchemy as sa


def test_redshift_distribution_and_connection_options():
    """Use exactly one dialect distribution and preserve verified SSL defaults."""
    distribution, excluded = "sqlalchemy-redshift", "deepnote-sqlalchemy-redshift"
    if sys.version_info < (3, 12):
        distribution, excluded = excluded, distribution
    assert version(distribution)
    with pytest.raises(PackageNotFoundError):
        version(excluded)

    engine = sa.create_engine("redshift+psycopg2://localhost/test")
    try:
        _, options = engine.dialect.create_connect_args(engine.url)
        assert options["sslmode"] == "verify-full"
        assert Path(options["sslrootcert"]).is_file()
        statement = sa.select(sa.bindparam("value"))
        compiled = statement.compile(dialect=engine.dialect)
        assert "%(value)s" in str(compiled)
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("format_type", "expected_type"),
    [("integer", "INTEGER"), ("numeric(12,2)", "NUMERIC(12, 2)"), ("super", "SUPER")],
)
def test_redshift_column_reflection(format_type, expected_type):
    """Exercise the PostgreSQL reflection API that changed in SQLAlchemy 2."""
    engine = sa.create_engine("redshift+psycopg2://localhost/test")
    try:
        column = engine.dialect._get_column_info(
            name="value",
            format_type=format_type,
            default=None,
            notnull=False,
            domains={},
            enums={},
            schema="public",
            comment="test column",
            encode="az64",
        )
        assert str(column["type"]) == expected_type
        assert column["nullable"] is True
        assert column["info"]["encode"] == "az64"
    finally:
        engine.dispose()
