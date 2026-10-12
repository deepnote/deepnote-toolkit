"""Stores and deletes named DataFrames in the project's filesystem.

Only the names in __all__ are supported public API.
"""

from .dataframes import delete_dataframe, store_dataframe

__all__ = ["delete_dataframe", "store_dataframe"]
