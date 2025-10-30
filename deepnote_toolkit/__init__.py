from .imports import safe_import

try:
    from ._version import __version__
except ImportError:
    __version__ = "0.1.0"

__all__ = ["__version__"]

# Import experimental components (not added to __all__)
# Since we want to import the entire module (not a specific attribute within it),
# and safe_import is designed for importing specific attributes, let's use a direct import
# and ignore any import errors since it's not exposed through __all__
try:
    from . import experimental_components
except Exception:
    pass

# Define components to import: (module_path, attribute_name)
# module_path: Relative path to the module file (e.g., '.chart')
# attribute_name: The specific class/function name inside the module, or None if importing the entire module
_IMPORT_MAPPINGS = [
    (".chart", "DeepnoteChart"),
    (".sql.query_preview", "DeepnoteQueryPreview"),
    (".dataframe_utils", "browse_dataframe"),
    (".execute_post_start_hooks", "execute_post_start_hooks"),
    (".runtime_initialization", "init_deepnote_runtime"),
    (".notebook_functions", "cancel_notebook_function"),
    (".notebook_functions", "run_notebook_function"),
    (".notebook_functions", "export_last_block_result"),
    (".session_persistence", "persist_notebook_session"),
    (".session_persistence", "restore_notebook_session"),
    (".set_integrations_env", "set_integration_env"),
    (".set_notebook_path", "set_notebook_path"),
    (".sql.sql_execution", "execute_sql"),
    (".sql.sql_execution", "execute_sql_with_connection_json"),
    (".variable_explorer", "deepnote_export_df"),
    (".variable_explorer", "deepnote_get_data_preview_json"),
    (".variable_explorer", "get_var_list"),
    (".ocelots", None),
]

# Perform safe imports
for module_path, attribute_name in _IMPORT_MAPPINGS:
    imported_object = safe_import(module_path, attribute_name, package=__name__)

    # Skip if import failed
    if imported_object is None:
        continue

    # Add to globals and __all__ either the imported attribute or module
    if attribute_name is not None:
        globals()[attribute_name] = imported_object
        __all__.append(attribute_name)
    else:
        export_name = module_path.split(".")[-1]
        globals()[export_name] = imported_object
        __all__.append(export_name)

# Remove artifacts from memory
del _IMPORT_MAPPINGS
del safe_import

del attribute_name
del module_path
