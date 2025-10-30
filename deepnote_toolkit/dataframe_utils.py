import traceback
import warnings

import pandas as pd
from IPython import get_ipython

import deepnote_toolkit.ocelots as oc
from deepnote_toolkit.logging import LoggerManager

from .dataframe_browser import BrowseSpec, browse_df

df_formatter_spec = None

df_browse_spec_map = {}

logger = LoggerManager().get_logger()


def configure_dataframe_formatter(spec):
    global df_formatter_spec
    df_formatter_spec = spec


def browse_dataframe(native_df: oc.NativeInputDF, spec):
    class BrowseDataframeError(Exception):
        pass

    if oc.DataFrame.is_supported(native_df):
        global df_browse_spec_map
        # TODO(NB-3993): using id() here is non ideal because different objects might have same id (as long as their
        # lifetime does not overlap). For pandas we can use df.attrs, but PySpark DF doesn't have anything like this
        df_browse_spec_map[id(native_df)] = spec
        return native_df
    else:
        raise BrowseDataframeError("This variable is not a supported Dataframe")


def get_dataframe_browsing_spec(native_df: oc.NativeInputDF):
    return df_browse_spec_map.get(id(native_df))


def clear_dataframe_browsing_spec(native_df: oc.NativeInputDF):
    if id(native_df) in df_browse_spec_map:
        del df_browse_spec_map[id(native_df)]


def add_formatters():
    """
    Adds a custom formatter for supported DataFrame objects to display them in a specific format.

    The formatter converts the DataFrame into a JSON-like representation and returns it as a MIME bundle.
    This then powers the rich and interactive DataFrame viewer in the Deepnote UI.

    This is an INIT FUNCTION that should be called during kernel initialization.
    """

    def dataframe_formatter(native_df: oc.NativeInputDF):
        # let's ignore warnings here because otherwise they clutter the output
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")

            # inspired by https://jupyter.readthedocs.io/en/latest/reference/mimetype.html
            MIME_TYPE_V3 = "application/vnd.deepnote.dataframe.v3+json"

            result = None
            browse_spec = None

            try:
                # First try to get the browsing spec associated with this specific df
                browse_spec = get_dataframe_browsing_spec(native_df)

                # Otherwise use the pre-configured browsing spec (configure_dataframe_formatter). This is set in
                # advance when we don't have DF object created (see code-snippets.ts in the main app repo)
                if browse_spec is None and df_formatter_spec is not None:
                    browse_spec = df_formatter_spec

                # Get the formatted df table
                result = _describe_dataframe(native_df, browse_spec)
            except:  # noqa: E722
                # Report a df table error so we can tell the user
                result = {"error": traceback.format_exc()}
            finally:
                # Cleanup the table spec to not influence different cell executions
                clear_dataframe_browsing_spec(native_df)

            # Return whatever the outcome is
            # According to IPython's mimebundle formatter spec, we need to return
            # a tuple of (data_dict, metadata_dict) to place metadata at the top level
            return (
                {MIME_TYPE_V3: result},
                {"table_state_spec": browse_spec},
            )

    mimebundle_formatter = get_ipython().display_formatter.mimebundle_formatter
    mimebundle_formatter.for_type(pd.DataFrame, dataframe_formatter)
    mimebundle_formatter.for_type_by_name(
        "pyspark.sql.connect.dataframe", "DataFrame", dataframe_formatter
    )
    mimebundle_formatter.for_type_by_name(
        "pyspark.sql.dataframe", "DataFrame", dataframe_formatter
    )
    mimebundle_formatter.for_type_by_name(
        "pyspark.pandas.frame", "DataFrame", dataframe_formatter
    )
    logger.info("Attached mimebundle formatters")


def _describe_dataframe(native_df: oc.NativeInputDF, browse_spec_json):
    oc_df = oc.DataFrame.from_native(native_df)
    column_count = len(oc_df.columns)

    browse_spec = BrowseSpec.from_json(browse_spec_json, oc_df.column_names)
    browse_result = browse_df(oc_df, browse_spec)

    if oc_df.data_preview is not None and oc_df.data_preview.satisfies(
        filters=browse_spec.filters, sort_by=browse_spec.sort_by
    ):
        columns_with_stats = oc_df.data_preview.get_columns_stats(
            browse_spec.color_scale_column_names
        )
    else:
        columns_with_stats = browse_result.processed_df.analyze_columns(
            browse_spec.color_scale_column_names
        )

    return {
        "column_count": column_count,
        "columns": [column.serialize() for column in columns_with_stats],
        "row_count": browse_result.row_count,
        "preview_row_count": browse_result.preview_row_count,
        "rows": browse_result.rows,
        "type": browse_result.output_type,
    }
