"""Import every name deepnote_toolkit/__init__.py loads, so a broken dependency fails."""

import deepnote_toolkit
from deepnote_toolkit import artifacts, experimental_components, ocelots
from deepnote_toolkit.artifacts import delete_dataframe, store_dataframe
from deepnote_toolkit.chart import DeepnoteChart
from deepnote_toolkit.dataframe_utils import browse_dataframe
from deepnote_toolkit.execute_post_start_hooks import execute_post_start_hooks
from deepnote_toolkit.notebook_functions import (
    cancel_notebook_function,
    export_last_block_result,
    run_notebook_function,
)
from deepnote_toolkit.runtime_initialization import init_deepnote_runtime
from deepnote_toolkit.session_persistence import (
    persist_notebook_session,
    restore_notebook_session,
)
from deepnote_toolkit.set_integrations_env import set_integration_env
from deepnote_toolkit.set_notebook_path import set_notebook_path
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview
from deepnote_toolkit.sql.sql_execution import (
    execute_sql,
    execute_sql_with_connection_json,
)
from deepnote_toolkit.variable_explorer import (
    deepnote_export_df,
    deepnote_get_data_preview_json,
    get_var_list,
)

print(f"Imported deepnote_toolkit from {deepnote_toolkit.__file__}")
