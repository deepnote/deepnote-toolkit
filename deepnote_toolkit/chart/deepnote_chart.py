import copy
import json
import uuid
import warnings
from typing import Any, Dict, List, Optional, Union

import pyarrow as pa
import vegafusion
import vl_convert

import deepnote_toolkit.ocelots as oc
from deepnote_toolkit.chart.spec_utils import (
    attach_config_to_vega_lite_spec,
    attach_selection_parameters_to_vega_lite_spec,
    verify_used_fields,
)
from deepnote_toolkit.chart.types import CHART_ROW_LIMIT, VEGA_5_MIME_TYPE, ChartError
from deepnote_toolkit.chart.utils import (
    sanitize_dataframe_for_chart,
    serialize_values_list_for_json,
)
from deepnote_toolkit.logging import LoggerManager
from deepnote_toolkit.ocelots.constants import DEEPNOTE_INDEX_COLUMN

logger = LoggerManager().get_logger()


def _create_vf_runtime_for_dataframe(
    oc_df: oc.DataFrame, name: str
) -> vegafusion.VegaFusionRuntime:
    if oc_df.native_type == "pandas":
        return vegafusion.VegaFusionRuntime()
    elif oc_df.native_type == "pyspark":
        spark_df = oc_df.to_native()

        def spark_executor(sql_query: str) -> pa.Table:
            # NOTE: we expect user to set Spark session to UTC, or else temporal
            # fields might be calculated not accurately (see public Deepnote docs)
            spark_df.createOrReplaceTempView(name)
            # Apply limit here because in VegaFusion it's applied after pulling records into memory,
            # which in case of e.g. scatterplot using big dataframe will cause OOM
            result_df = spark_df.sparkSession.sql(sql_query).limit(CHART_ROW_LIMIT + 1)
            # .toArrow() is available only from Spark 4.0+, so we use Pandas as intermediate layer
            pandas_df = result_df.toPandas()
            arrow_table = pa.Table.from_pandas(pandas_df)
            # Drop temp view to not pollute Spark namespace
            spark_df.sparkSession.catalog.dropTempView(name)
            return arrow_table

        return vegafusion.VegaFusionRuntime.new_vendor("sparksql", spark_executor)

    raise TypeError(
        f"Can't construct VegaFusion runtime for unknown DataFrame type {oc_df.native_type}"
    )


def _create_vf_inline_dataset_from_dataframe(oc_df: oc.DataFrame) -> Any:
    if oc_df.native_type == "pandas":
        return oc_df.to_native()
    elif oc_df.native_type == "pyspark":
        from pyspark.sql.pandas.types import to_arrow_schema

        spark_df = oc_df.to_native()
        return to_arrow_schema(spark_df.schema)

    raise TypeError(f"Unsupported DataFrame type: {oc_df.native_type}")


class DeepnoteChart:
    """
    A class used for chart block outputs in Deepnote. Pass only spec or spec_dict, not both.

    Parameters:
    - dataframe: The input dataframe for the chart.
    - spec: A JSON string representing the Vega-Lite specification for the chart.
    - spec_dict: A dictionary representing the Vega-Lite specification for the chart.
    - attach_config: Whether to attach config fields to Vega-Lite spec before executing. These fields
    affect visual appearance of generated chart.
    - attach_selection: Whether to attach `params` which add interactive selection filtering to
    Vega-Lite spec before processing it. Those are intended for use only by charts generated in chart
    block and are further processed on front-end.
    - filters: Either JSON string or list of oc.Filters to be applied to the dataframe before charting
    """

    compiled_vega_spec_dict: Dict[str, Any]
    source_vega_spec_dict: Dict[str, Any]
    source_vega_lite_spec_dict: Dict[str, Any]
    dataframe: oc.DataFrame
    filters: List[oc.Filter]

    def __init__(
        self,
        dataframe: oc.NativeInputDF,
        spec: Optional[str] = None,
        spec_dict: Optional[Dict[str, Any]] = None,
        *,
        attach_config: bool = True,
        attach_selection: bool = False,
        filters: Union[str, List[oc.Filter], None] = None,
    ):
        if filters is None:
            filters = []
        if spec is None and spec_dict is None:
            raise ValueError(
                "either spec or spec_dict should be provided when constructing DeepnoteChart"
            )

        if spec is not None and spec_dict is not None:
            raise ValueError(
                "only one of spec or spec_dict should be provided when constructing DeepnoteChart"
            )

        # TODO (ENT-226): previously, webapp was adding this extra layer of escaping, as it was required by Altair. We don't
        # use Altair anymore and this escaping messes up with vl-convert, so we need to strip it here until we update
        # frontend to stop doing it altogether
        if spec is not None:
            slash = "\\"
            spec = spec.replace(f"{slash}{slash}{slash}", "\\").replace(
                f"{slash}{slash}'", "'"
            )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            if isinstance(filters, str):
                filters_raw = json.loads(filters)
                parsed_filters = [oc.Filter.from_dict(f) for f in filters_raw]
            else:
                parsed_filters = filters
            self.filters = parsed_filters

            spec_dict = json.loads(spec) if spec else copy.deepcopy(spec_dict)
            if attach_config:
                attach_config_to_vega_lite_spec(spec_dict)
            if attach_selection:
                attach_selection_parameters_to_vega_lite_spec(spec_dict)

            oc_df = oc.DataFrame.from_native(dataframe)
            filtered_df = oc_df.filter(*self.filters).prepare_for_serialization()
            if filtered_df.native_type == "pandas":
                sanitized_pandas = sanitize_dataframe_for_chart(filtered_df.to_native())
                oc_sanitized_df = oc.DataFrame.from_native(sanitized_pandas)
            elif filtered_df.native_type == "pyspark":
                oc_sanitized_df = filtered_df
            else:
                raise TypeError(
                    f"DataFrame type {filtered_df.native_type} is not supported"
                )

            verify_used_fields(oc_sanitized_df, spec_dict)

            dataframe_name = f"deepnote_{uuid.uuid4().hex}"

            spec_dict["data"] = {"url": f"vegafusion+dataset://{dataframe_name}"}

            self.source_vega_lite_spec_dict = spec_dict
            self.source_vega_spec_dict = vl_convert.vegalite_to_vega(spec_dict)
            self.dataframe = oc_sanitized_df

            vf_runtime = _create_vf_runtime_for_dataframe(
                oc_sanitized_df, dataframe_name
            )
            inline_dataset = _create_vf_inline_dataset_from_dataframe(oc_sanitized_df)

            try:
                transformed_spec, orig_transformation_warnings = (
                    vf_runtime.pre_transform_spec(
                        self.source_vega_spec_dict,
                        inline_datasets={dataframe_name: inline_dataset},
                        local_tz="UTC",
                        default_input_tz="UTC",
                        preserve_interactivity=False,
                        row_limit=CHART_ROW_LIMIT,
                    )
                )
            except pa.ArrowNotImplementedError as err:
                error_msg = (
                    f"DataFrame contains data types that cannot be serialized into Arrow format for charting: {err}. "
                    f"Common causes include:\n"
                    f"• Mixed data types in columns (e.g., numbers and strings)\n"
                    f"• Complex objects like dictionaries or custom classes\n"
                    f"• Nested data structures\n"
                    f"Try converting problematic columns to consistent types or removing them."
                )
                raise ChartError(error_msg) from err

            transformation_warnings = [w["type"] for w in orig_transformation_warnings]

            unsupported_spec = "Unsupported" in transformation_warnings
            if unsupported_spec:
                logger.error(
                    "VegaFusion couldn't transform chart spec",
                    extra={"spec_json": json.dumps(self.source_vega_spec_dict)},
                )

            row_limit_exceeded = "RowLimitExceeded" in transformation_warnings

            distinct_values = oc_sanitized_df.get_columns_distinct_values(
                oc_sanitized_df.columns, 30
            )
            columns = [
                {
                    "name": col.name,
                    "nativeType": col.native_type,
                    "distinctValues": serialize_values_list_for_json(
                        distinct_values[col.name]
                    ),
                }
                for col in oc_sanitized_df.columns
                if col.name != DEEPNOTE_INDEX_COLUMN
            ]

            # NOTE: If changing output metadata, also update specOutputMetadataSchema in chart-utils.ts in web app
            output_metadata = {
                "rowLimitExceeded": row_limit_exceeded,
                "rowLimit": CHART_ROW_LIMIT,
                "filteredDataframeSize": oc_sanitized_df.size(),
                "columns": columns,
            }
            if "usermeta" not in transformed_spec:
                transformed_spec["usermeta"] = {}
            transformed_spec["usermeta"]["outputMetadata"] = output_metadata

            self.compiled_vega_spec_dict = transformed_spec

    def _repr_mimebundle_(self, include, exclude):
        """
        A class method _repr_mimebundle_ is a special method used in IPython for custom display of objects.
        It returns a dictionary where the keys are MIME types (like 'text/html', 'application/json', etc.)
        and the values are the corresponding representations of the object.
        """

        return {
            VEGA_5_MIME_TYPE: self.compiled_vega_spec_dict,
        }
