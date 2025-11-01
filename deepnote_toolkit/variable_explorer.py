import datetime
import json
import threading
import traceback
from pathlib import Path
from sys import getsizeof
from typing import List, Tuple, Union

import numpy as np
import pandas as pd
from IPython import get_ipython
from IPython.core.magics.namespace import NamespaceMagics

import deepnote_toolkit.ocelots as oc

from .dataframe_browser import BrowseSpec
from .logging import LoggerManager

MAX_CONTENT_ELEMENTS = 10
MAX_CONTENT_LENGTH = 500
TIMEOUT_SECONDS = 1


class ExportDataframeError(Exception):
    pass


class ExportSizeDataframeError(Exception):
    pass


def get_var_list():
    """
    Retrieves a list of variables from the Jupyter kernel's user namespace.

    Returns:
        str: A JSON string representing the dictionary of variables.

    Raises:
        Exception: If a timeout occurs while retrieving the variables.
        Exception: If an error occurs during the retrieval process.
    """

    _nms = NamespaceMagics()
    _ipython = get_ipython()
    _nms.shell = _ipython.kernel.shell
    logger = LoggerManager().get_logger()

    def _get_var_list():
        logger.debug("[Variable Explorer] starting _get_var_list()")

        all_variables = _nms.who_ls()

        filtered_variables = []
        for v in all_variables:
            if v not in ["_html", "_nms", "NamespaceMagics", "_Jupyter", "In", "Out"]:
                filtered_variables.append(v)

        filtered_variables_by_type = []
        for v in filtered_variables:
            if type(_ipython.kernel.shell.user_ns[v]).__name__ not in [
                "module",
                "function",
                "builtin_function_or_method",
                "instance",
                "_Feature",
                "type",
                "ufunc",
            ]:
                filtered_variables_by_type.append(v)

        variables_dic = {
            v: _get_variable_dict_entry(v, _ipython.kernel.shell.user_ns[v])
            for v in filtered_variables_by_type
        }
        variables_dic = {k: v for k, v in variables_dic.items() if v is not None}

        logger.debug("[Variable Explorer] finished _get_var_list()")

        return variables_dic

    def _get_var_list_or_catch_to_dict(return_dict):
        try:
            return_dict["variables"] = _get_var_list()
        except Exception as e:
            return_dict["error"] = str(e)
            return_dict["traceback"] = traceback.format_exc()

    logger.debug("[Variable Explorer] Creating thread")
    return_dict = {}
    thread = threading.Thread(
        target=_get_var_list_or_catch_to_dict, args=(return_dict,)
    )
    logger.debug("[Variable Explorer] Starting thread")
    thread.start()
    thread.join(timeout=TIMEOUT_SECONDS)
    logger.debug("[Variable Explorer] Thread finished")

    if thread.is_alive():
        # Handling the timeout scenario
        logger.error("[Variable Explorer] Timeout")
        raise Exception("Timeout")

    if "error" in return_dict:
        raise Exception(return_dict["error"] + "\n" + return_dict["traceback"])

    if "variables" not in return_dict:
        raise Exception("Error: variables not found in return dictionary.")

    logger.debug("[Variable Explorer] Success: returning variables")

    return json.dumps(return_dict)


def deepnote_export_df(native_df: oc.NativeInputDF, spec_json, filename):
    """
    Export a supported DataFrame to a CSV file.

    Parameters:
    - df (oc.NativeDF): The DataFrame to be exported.
    - spec: The specification for exporting the DataFrame.
    - filename (str): The path and filename of the CSV file to be created.

    Raises:
    - ExportDataframeError: If the variable is not a supported DataFrame.
    - ExportSizeDataframeError: If the variable size is too big to export.

    Returns:
    - None
    """

    if not oc.DataFrame.is_supported(native_df):
        raise ExportDataframeError("This variable type is not supported as DataFrame")

    oc_df = oc.DataFrame.from_native(native_df)

    spec = BrowseSpec.from_json(spec_json, oc_df.column_names)
    processed_df = (
        oc_df.prepare_for_serialization().filter(*spec.filters).sort(spec.sort_by)
    )

    # Enforce a max variable size in memory rule.
    # The number is arbitrary, feel free to tune it, in case it's too small
    # according to customers or if this error is raised in Sentry too often.
    # Customers are notified in the UI if the export  is too big.
    max_size = 1024 * 1024 * 1024  # 1GB
    estimated_size = processed_df.estimate_export_byte_size("csv")
    if estimated_size > max_size:
        raise ExportSizeDataframeError(
            f"Estimated export file size is too big. Max size is {_format_bytes(max_size)} after filtering, estimated size: {_format_bytes(estimated_size)}"
        )

    filepath = Path(filename)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    processed_df.to_csv(filename)


def deepnote_get_data_preview_json(
    native_df: oc.NativeInputDF,
    filters: Union[str, List[oc.Filter]],
    sort_by: Union[str, List[Tuple[str, bool]]],
    size=oc.constants.DEFAULT_DATA_PREVIEW_RECORDS,
    mode="sampled",
) -> str:
    # NOTE: we don't cache data previews from this function as we don't expect webapp
    # to call it often
    if isinstance(filters, str):
        filters_raw = json.loads(filters)
        parsed_filters = [oc.Filter.from_dict(f) for f in filters_raw]
    else:
        parsed_filters = filters
    parsed_sort_by = (
        [(col, asc) for col, asc in json.loads(sort_by)]
        if isinstance(sort_by, str)
        else sort_by
    )

    data_preview = oc.DataPreview(native_df, size, mode)
    data_preview.update_if_needed(filters=parsed_filters, sort_by=parsed_sort_by)

    # We limit output to 10MB as in certain cases (a lot of columns or binary data in cells)
    # even 10,000 records we pull might be too big
    MAX_SIZE_BYTES = 10 * 1024 * 1024  # 10MB
    serialized_records = []
    current_size = 0

    for record in data_preview.data:
        serialized_record = json.dumps(record)
        record_size = len(serialized_record.encode("utf-8"))

        if current_size + record_size > MAX_SIZE_BYTES:
            break

        serialized_records.append(serialized_record)
        current_size += record_size

    return f"[{','.join(serialized_records)}]"


def _format_bytes(size_bytes: int) -> str:
    """Convert bytes into human readable format."""

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def _is_fast_to_stringify(x):
    fast_types = (
        # built-in types
        str,
        int,
        float,
        complex,
        bool,
        bytes,
        bytearray,
        type(None),
        # datetime types
        datetime.datetime,
        datetime.date,
        datetime.time,
        datetime.timedelta,
        datetime.tzinfo,
        # standard library types
        range,
        slice,
        memoryview,
    )

    # Check if it's one of the basic fast types
    if isinstance(x, fast_types):
        return True

    # Handle numpy types with version compatibility
    try:
        # Check for numpy types with safe fallbacks
        if np is not None:
            numpy_types = (
                np.bool_,
                np.number,
                np.integer,
                np.floating,
                np.complex_,
                np.timedelta64,
                np.datetime64,
            )
            if isinstance(x, numpy_types):
                return True
    except (AttributeError, TypeError):
        # If numpy types change or there's an issue with the comparison
        pass

    return False


def _get_elements_of(x):
    # return the list of stringified elements from array like var if not too big
    # return None otherwise

    # has to be in sync with main app INPUT_SELECT_MAX_VAR_NUM_ELEMENTS constant
    MAX_ELEMENTS = 1000

    # has to be in sync with main app INPUT_SELECT_MAX_TOTAL_CHARS constant
    MAX_TOTAL_LENGTH = 10000

    try:
        if (
            not isinstance(x, (list, np.ndarray, pd.Series))
            and not oc.utils.is_pandas_dataframe(x)
            or len(x) > MAX_ELEMENTS
        ):
            return None

        if oc.utils.is_pandas_dataframe(x) and x.shape[1] > 0:
            first_column_elements = x[x.columns[0]]
            elements = [item for item in first_column_elements]
        else:
            elements = [item for item in x]

        if not all(map(_is_fast_to_stringify, elements)):
            return None

        str_elements = [str(item) for item in elements]

        total_string_length = sum(map(len, str_elements))
        if total_string_length > MAX_TOTAL_LENGTH:
            return None

        return str_elements
    except:  # noqa: E722
        return None


def _get_size(x):
    """
    return the size of variable x. Amended version of sys.getsizeof
    which also supports ndarray, Series and DataFrame
    """

    try:
        if isinstance(x, (np.ndarray, pd.Series)):
            return x.nbytes
        elif oc.utils.is_pandas_dataframe(x):
            return x.memory_usage().sum()
        else:
            return getsizeof(x)
    except:  # noqa: E722
        return None


def _get_shape(x):
    """
    returns the stringified shape of x if it has one
    returns None otherwise - might want to return an empty string for an empty column
    """

    try:
        return str(x.shape) if x.shape else ""
    # x does not have a shape
    except:  # noqa: E722
        return None


def _get_columns(x):
    """
    if x is a dataframe, returns its column names in an array
    returns None otherwise
    """

    if oc.utils.is_pandas_dataframe(x):
        return x.columns[: oc.constants.MAX_COLUMNS_TO_DISPLAY].map(str).tolist()
    return None


def _get_column_types(x):
    """
    if x is a dataframe, returns its column types in an array
    returns None otherwise
    """

    if oc.utils.is_pandas_dataframe(x):
        return list(map(str, x.dtypes[: oc.constants.MAX_COLUMNS_TO_DISPLAY]))
    return None


def _get_underlying_data_type(x):
    """
    returns the stringified underlying datatype of x if it has one
    returns None otherwise
    """

    try:
        return str(x.dtype.name) if x.dtype.name else ""
    # x does not have an underlying dtype
    except:  # noqa: E722
        return None


def _get_content(x):
    try:
        if oc.utils.is_pandas_dataframe(x):
            colnames = ", ".join(x.columns.map(str))
            content = "Column names: %s" % colnames
        elif isinstance(x, pd.Series):
            content = "Series [%d rows]" % x.shape
        elif isinstance(x, np.ndarray):
            content = x.__repr__()
        elif hasattr(x, "__len__") and len(x) > MAX_CONTENT_ELEMENTS:
            content = str(x[:MAX_CONTENT_ELEMENTS]) + "…"
        elif _is_fast_to_stringify(x):
            content = str(x)
        else:
            # the variable can be huge and doing str(x) can easily take more than 1 second,
            # which is our budget for all variables, so we avoid calling str(x) for arbitrary objects
            return ""

        if len(content) > MAX_CONTENT_LENGTH:
            return content[:MAX_CONTENT_LENGTH] + "…"
        else:
            return content
    except:  # noqa: E722
        return ""


def _get_number_of_elements(x):
    try:
        return len(x)
    except:  # noqa: E722
        return None


def _get_number_of_columns(x):
    try:
        if oc.utils.is_pandas_dataframe(x):
            return len(x.columns)
        else:
            return None
    except:  # noqa: E722
        return None


def _to_int(x):
    # for JSON serialization purposes, we need to convert numpy integers to standard integers
    return int(x) if x else None


def _get_type_name(v):
    """
    Get the normalized type name for a variable with backwards compatibility.

    In Python 3.13, NumPy's bool_ type may report as 'bool' instead of 'bool_'.
    This function ensures we always return 'bool_' for NumPy bool types to maintain
    compatibility with older Python versions.
    """
    # Check for NumPy bool_ type first (most reliable check)
    # This handles the case where Python 3.13 reports np.bool_ as 'bool'
    try:
        if np is not None and isinstance(v, np.bool_):
            return "bool_"
    except (AttributeError, TypeError):
        # If numpy types change or there's an issue with the comparison
        pass

    # Fall back to standard type name
    return type(v).__name__


def _get_variable_dict_entry(var_name, v):
    try:
        shape = _get_shape(v)
        columns = _get_columns(v)
        columnTypes = _get_column_types(v)
        underlying_data_type = _get_underlying_data_type(v)
        var_result = {
            "varName": var_name,
            "varType": _get_type_name(v),
            "varSize": _to_int(_get_size(v)),
            "varShape": shape,
            "varContent": _get_content(v),
            "varElements": _get_elements_of(v),
            "varColumns": columns,
            "varColumnTypes": columnTypes,
            "varUnderlyingType": underlying_data_type,
            "numElements": _to_int(_get_number_of_elements(v)),
            "numColumns": _to_int(_get_number_of_columns(v)),
        }

        return var_result
    except LookupError:
        return None
