from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union

import pandas as pd
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    try:
        # This import here is only to make module types available (used in PysparkDF), so
        # it's okay for it to fail if user doesn't have it installed (as we do not
        # include it as dependency of the toolkit)
        import pyspark.pandas.frame
        import pyspark.sql
        import pyspark.sql.connect.dataframe
    except ImportError:
        pass


@dataclass
class ColumnStats:
    unique_count: Optional[int] = None
    nan_count: Optional[int] = None
    min: Optional[str] = None
    max: Optional[str] = None
    histogram: Optional[List[Dict[str, float]]] = None
    categories: Optional[List[str]] = None


@dataclass
class ColumnsStatsRecord:
    name: str
    dtype: str
    stats: Optional[ColumnStats] = None

    def serialize(self):
        result = asdict(self)

        # TODO(NB-3996): this is here to please Zod schema in main app. I'm not satisfied with this ad-hoc approach to serialization :/
        if result["stats"] is None:
            del result["stats"]
        else:
            if result["stats"]["unique_count"] is None:
                del result["stats"]["unique_count"]

            if result["stats"]["nan_count"] is None:
                del result["stats"]["nan_count"]
        return result


@dataclass
class Column:
    name: str
    native_type: str

    def __repr__(self) -> str:
        return f"Column(name='{self.name}', native_type='{self.native_type}')"


class UnsupportedDataFrameException(TypeError):
    pass


PandasDF: TypeAlias = pd.DataFrame
PysparkDF: TypeAlias = Union[
    "pyspark.sql.connect.dataframe.DataFrame", "pyspark.sql.DataFrame"
]
PandasOnSparkDF: TypeAlias = "pyspark.pandas.DataFrame"

# Union of all supported dataframe types
NativeInputDF = Union[PandasDF, PysparkDF, PandasOnSparkDF]
NativeOutputDF = Union[PandasDF, PysparkDF]
NativeOutputType = Literal["pandas", "pyspark"]
