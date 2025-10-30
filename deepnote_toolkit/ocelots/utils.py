import sys

import pandas as pd
from typing_extensions import TypeGuard

from deepnote_toolkit.ocelots.types import PandasDF, PandasOnSparkDF, PysparkDF
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview


def is_pandas_dataframe(df) -> TypeGuard[PandasDF]:
    return isinstance(df, (pd.DataFrame, DeepnoteQueryPreview))


def is_pyspark_dataframe(df) -> TypeGuard[PysparkDF]:
    # We do not distribute and import PySpark directly as it's huge dependency, instead
    # we try to detect already imported (by user) library and use it
    sql_module = sys.modules.get("pyspark.sql")
    if sql_module is not None and isinstance(df, sql_module.DataFrame):
        return True

    connect_module = sys.modules.get("pyspark.sql.connect.dataframe")
    if connect_module is not None and isinstance(df, connect_module.DataFrame):
        return True

    return False


def is_pandas_on_spark_dataframe(df) -> TypeGuard[PandasOnSparkDF]:
    # We do not distribute and import PySpark directly as it's huge dependency, instead
    # we try to detect already imported (by user) library and use it
    pandas_on_spark_module = sys.modules.get("pyspark.pandas")
    if pandas_on_spark_module is not None and isinstance(
        df, pandas_on_spark_module.DataFrame
    ):
        return True

    return False
