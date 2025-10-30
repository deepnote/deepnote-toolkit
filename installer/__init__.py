"""Local package wrapper to prefer Deepnote installer over third-party wheel."""

# Keep module namespace available for callers expecting installer.module.*
from . import module  # noqa: F401

__all__ = ["module"]
