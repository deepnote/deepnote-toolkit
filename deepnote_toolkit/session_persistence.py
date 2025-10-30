import contextlib
import io

import dill
import requests


def persist_notebook_session(url):
    """Persists the current notebook session to a PUT URL."""

    with _remove_system_modules_from_globals():
        with io.BytesIO() as in_memory_file:
            dill.dump_session(in_memory_file)
            in_memory_file.seek(0)
            response = requests.put(url, data=in_memory_file)
            response.raise_for_status()


def restore_notebook_session(url):
    """Restores the notebook session from a GET URL."""

    response = requests.get(url)
    response.raise_for_status()

    with io.BytesIO(response.content) as in_memory_file:
        dill.load_session(in_memory_file)


@contextlib.contextmanager
def _remove_system_modules_from_globals():
    DEEPNOTE_GLOBALS = [
        # deepnote-toolkit
        "deepnote_toolkit",
        # @deepnote/deepnote:executor
        "_deepnote_get_var_list",
        "_deepnote_delete_last_ipython_history_item",
        # @deepnote/deepnote:shared
        "_deepnote_hide_ipython_history_once",
        "_deepnote_get_dataframe_column_distinct_values_once",  # TODO: Move to toolkit.
        "__deepnote_big_number__",  # TODO: Move to toolkit?
        "_deepnote_export_last_block_result_once",  # TEMP: Will be moved to toolkit.
        "_deepnote_run_notebook_function",  # TEMP: Will be moved to toolkit.
        "_deepnote_cancel_notebook_function",  # TEMP: Will be moved to toolkit.
    ]

    popped_globals = {}

    # remove all deepnote functions from globals()
    for deepnote_function_name in DEEPNOTE_GLOBALS:
        try:
            popped_function = globals().pop(deepnote_function_name)
            popped_globals[deepnote_function_name] = popped_function
        except KeyError:
            # deepnote_function_name was not found in globals(), never mind, we want to continue
            pass

    try:
        yield
    finally:
        # restore all deepnote functions to globals()
        for deepnote_function_name, popped_function in popped_globals.items():
            globals()[deepnote_function_name] = popped_function
