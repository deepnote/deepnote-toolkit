"""Stores and deletes named DataFrames in the project's filesystem."""

from .dataframes import delete_dataframe, store_dataframe

__all__ = ["delete_dataframe", "store_dataframe"]
