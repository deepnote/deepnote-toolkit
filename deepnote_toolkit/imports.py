"""Import utilities for the Deepnote toolkit.

This module provides utilities for safely importing modules and reporting
import errors to the webapp.
"""

import importlib
from typing import Optional

from .logging import report_error_to_webapp


def safe_import(
    module_path: str, attribute_name: Optional[str], package: str = __name__
):
    """
    Attempts to import an attribute from a module, registers it globally,
    adds it to __all__, and reports errors if it fails.

    Args:
        module_path: Relative path to the module (e.g., '.chart').
        attribute_name: The name of the attribute to import.
        package: The package to use for the import (default is __name__).

    Returns:
        The imported attribute or module if it exists, None otherwise.
    """

    try:
        module = importlib.import_module(module_path, package=package)
        if attribute_name is not None:
            attribute = getattr(module, attribute_name)
            return attribute
        return module
    except ModuleNotFoundError as e:
        _error_msg = (
            f"Module '{module_path}' not found. Cannot import '{attribute_name}': {e}"
        )
        report_error_to_webapp(
            "TOOLKIT_IMPORT_ERROR",
            _error_msg,
            extra_context={"module": module_path, "attribute": attribute_name},
        )
        return None
    except AttributeError as e:
        _error_msg = (
            f"Attribute '{attribute_name}' not found in module '{module_path}': {e}"
        )
        report_error_to_webapp(
            "TOOLKIT_IMPORT_ERROR",
            _error_msg,
            extra_context={"module": module_path, "attribute": attribute_name},
        )
        return None
    except ValueError as e:
        _error_msg = f"Failed to import '{attribute_name}' (from {module_path}.{attribute_name}): {e}"
        report_error_to_webapp(
            "TOOLKIT_IMPORT_ERROR",
            _error_msg,
            extra_context={"module": module_path, "attribute": attribute_name},
        )
        return None
    except Exception as e:
        _error_msg = f"Failed to import '{attribute_name}' (from {module_path}.{attribute_name}): {e}"
        report_error_to_webapp(
            "TOOLKIT_IMPORT_ERROR",
            _error_msg,
            extra_context={
                "module": module_path,
                "attribute": attribute_name,
                "exception_type": type(e).__name__,
            },
        )
        return None
