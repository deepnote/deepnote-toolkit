import datetime
import uuid

import numpy as np
import pandas as pd
import pyspark.sql.types as pst
import pytz


def create_multi_index_dataframe():
    df = pd.DataFrame(
        data={
            "col1": [1, 2, 3, 4],
            "col2": [1, 2, None, 4],
            "col3": [1, 2.1, 3, 4],
        }
    )
    df.columns = pd.MultiIndex.from_product([df.columns, ["col0"]])
    return df


def create_dataframe_with_duplicate_column_names():
    df = pd.DataFrame(
        data={
            "col1": ["a", "b", "c", "d"],
            "col2": [1, 2, 3, 4],
        }
    )
    df.columns = ["col1", "col1"]
    return df


# Create testing dataframes dictionary with all common test dataframes
testing_dataframes = {
    "basic": pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}),
    "basic2": pd.DataFrame(data={"col1": [3, 1, 2]}),
    "categorical_columns": pd.DataFrame(
        data={
            "cat1": ["a", "b", "c", "d"],
            "cat2": ["a", "b", None, "d"],
            "cat3": [1, (2, 3), "4", []],
            "cat4": [1, (2, 3), "4", 5],
            "cat5": [True, True, True, False],
        }
    ),
    "column_distinct_values": pd.DataFrame(
        data={
            "col1": [2, 2, 2, 4, 42, 77, 4],
            "col2": ["a", "b", "a", "c", "a", "b", "a"],
            "col3": [2, 1, 1, "wow", "test", "wow", 1],
        }
    ),
    "numerical_columns": pd.DataFrame(
        data={
            "col1": [1, 2, 3, 4],
            "col2": [1, 2, None, 4],
            "col3": [1, 2.1, complex(-1.0, 0.0), 10**100],
            "col4": [1, 2.1, 3, 4],
        }
    ),
    "multi_columns_sort": pd.DataFrame(
        data={
            "numeric_col": [1, 2, 2, 3, 4, 5, 1],
            "string_col": [
                "apple",
                "banana",
                "cherry",
                "apple",
                "date",
                "fig",
                "apple",
            ],
        }
    ),
    "multi_level_columns": create_multi_index_dataframe(),
    "many_rows_10k": pd.DataFrame(
        data={
            "col1": np.arange(10_000),
            "col2": np.arange(10_000),
            "col3": np.arange(10_000),
        }
    ),
    "many_rows_100k": pd.DataFrame(
        data={
            "col1": np.arange(100_000),
            "col2": np.arange(100_000),
            "col3": np.arange(100_000),
        }
    ),
    "many_rows_200k": pd.DataFrame(
        data={
            "col1": np.arange(200_000),
            "col2": np.arange(200_000),
            "col3": np.arange(200_000),
        }
    ),
    "many_columns": pd.DataFrame(data={f"col{i}": [i] for i in np.arange(10000)}),
    "no_rows": pd.DataFrame(
        data={
            "col1": [],
            "col2": [],
        }
    ),
    "no_columns": pd.DataFrame(data={}),
    "duplicate_columns": create_dataframe_with_duplicate_column_names(),
    "nans": pd.DataFrame(
        data={
            "col1": [None, None, None],
        }
    ),
    "large_numbers": pd.DataFrame(
        data={
            "col1": [2**53],
        }
    ),
    "infinity": pd.DataFrame(
        data={
            "col1": [0, np.inf, -np.inf],
        }
    ),
    "nat": pd.DataFrame(
        data={"col1": [np.datetime64("2005-02-25"), np.datetime64("NaT")]}
    ),
    "pandas_nat": pd.DataFrame(
        data={
            "col1": [1, 2],
            "col2": [pd.Timestamp.now(), pd.NaT],
        }
    ),
    "nan": pd.DataFrame({"A": [1, 2, np.nan], "B": [4, np.nan, 8]}),
    "int64_nan": pd.DataFrame(
        {"A": [1, 2, np.nan], "B": [4, np.nan, 8]}, dtype="Int64"
    ),
    "dict_column": pd.DataFrame(
        data={
            "col1": [{}, {"a": 2}, {"3": {"4": "b"}}],
        }
    ),
    "list_column": pd.DataFrame(
        data={
            "col1": ["a", "b", "c"],
        }
    ),
    "nested_list_column": pd.DataFrame(
        data={
            "col1": [[], ["a"], [[4]]],
        }
    ),
    "set_column": pd.DataFrame(
        data={
            "col1": [set(), {"a"}],
        }
    ),
    "datetime": pd.DataFrame(
        data={
            "col1": [1, 2],
            "col2": [datetime.date(2000, 1, 1), datetime.date(2000, 1, 1)],
            "col3": [
                datetime.datetime.now().astimezone(pytz.timezone("UTC")),
                datetime.datetime.now().astimezone(None),
            ],
        }
    ),
    "datetime_time": pd.DataFrame(
        data={
            "col1": [1, 2],
            "col2": [datetime.time(1, 1, 1), datetime.time(10, 30)],
        }
    ),
    "date_range_index": pd.DataFrame(
        np.random.randn(2, 3),
        index=pd.date_range("1/1/2000", periods=2),
        columns=["A", "B", "C"],
    ),
    "datetime_numpy": pd.DataFrame(
        data={
            "col1": [1, 2],
            "col2": [np.datetime64("2005-02-25"), np.datetime64("2005-02-26")],
        }
    ),
    "timedelta": pd.DataFrame(
        data={
            "col1": [1, 2],
            "col2": [np.timedelta64(4, "h"), np.timedelta64(2, "D")],
            "col3": [
                np.datetime64("2009-01-01") - np.datetime64("2008-01-01"),
                np.datetime64("2009-01-01") - np.datetime64("2008-01-02"),
            ],
            "col4": [datetime.timedelta(days=64), datetime.timedelta(seconds=1)],
        }
    ),
    "category_dtype": pd.DataFrame(
        {
            "A": ["a", "b", "c", "a"],
            "B": pd.Series(["a", "b", None, "a"], dtype="category"),
        }
    ),
    "datetime_with_empty_first_row": pd.DataFrame(
        data={
            "col1": [None, 2],
            "col2": [None, datetime.time(10, 30)],
            "col3": [None, datetime.datetime.now().astimezone(None)],
        }
    ),
    "long_string": pd.DataFrame(
        data={
            "col1": [
                "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse ut nisl id nulla commodo dictum id vitae metus."
                * 100
            ],
        }
    ),
    "long_dict": pd.DataFrame(
        data={
            "col1": [
                {
                    "key": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse ut nisl id nulla commodo dictum id vitae metus."
                    * 100
                }
            ],
        }
    ),
    "with_uuid_column": pd.DataFrame(
        data={
            "col1": [uuid.uuid4(), uuid.uuid4()],
        }
    ),
    "array_column": pd.DataFrame(
        data={
            "col1": [np.array([1, 2, 3]), np.array([4, 5, 6])],
        }
    ),
    "mixed_column_types": pd.DataFrame(
        data={
            "col1": [1, "a", [3, 4]],
            "col2": [
                datetime.date(2000, 1, 1),
                datetime.time(10, 30),
                datetime.timedelta(days=64),
            ],
            "col3": [
                datetime.datetime.now().astimezone(pytz.timezone("UTC")),
                datetime.datetime.now().astimezone(None),
                np.datetime64("2009-01-01"),
            ],
            "col4": [
                datetime.datetime.now(),
                np.timedelta64("nat"),
                np.datetime64("nat"),
            ],
        }
    ),
    "period_index": pd.DataFrame(
        [1, 2, 3], columns=pd.PeriodIndex(["2021-06"], freq="M", name="order_month")
    ),
    "non_serializable_values": {
        "data": pd.DataFrame(
            data={
                "list": [[1, 2, 3], [4, 5, 6]],
                "datetime": [
                    datetime.datetime(2023, 1, 1, 12, 0, 0),
                    datetime.datetime(2023, 1, 2, 12, 0, 0),
                ],
            }
        ),
        "pyspark_schema": pst.StructType(
            [
                pst.StructField("list", pst.ArrayType(pst.IntegerType()), True),
                pst.StructField("datetime", pst.TimestampType(), True),
            ]
        ),
    },
}

# Handle geopandas dataset based on version compatibility
try:
    import importlib.metadata

    import geopandas

    geopandas_version = importlib.metadata.version("geopandas")
    if geopandas_version and tuple(map(int, geopandas_version.split(".")[:2])) < (1, 0):
        # For older versions of geopandas (<1.0), use datasets.get_path
        testing_dataframes["geopandas"] = geopandas.read_file(
            geopandas.datasets.get_path("naturalearth_lowres")
        )
    else:
        # For geopandas 1.0+, use a simple GeoDataFrame with Point geometries
        from shapely.geometry import Point

        # Create a basic geometric dataset
        df = pd.DataFrame(
            {
                "name": ["Point A", "Point B", "Point C"],
                "value": [1, 2, 3],
                "geometry": [
                    Point(0, 0),
                    Point(1, 1),
                    Point(2, 2),
                ],
            }
        )
        testing_dataframes["geopandas"] = geopandas.GeoDataFrame(
            df, geometry="geometry"
        )
except (ImportError, AttributeError):
    # If geopandas is not available or has issues, provide a fallback
    pass
