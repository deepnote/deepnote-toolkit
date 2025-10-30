from typing import Any

from IPython import get_ipython

from deepnote_toolkit.ocelots.data_preview import (
    DeepnoteDataFrameWithDataPreview,
    should_wrap_into_data_preview,
)


def _process_execution_result(result: Any):
    if should_wrap_into_data_preview(result):
        return DeepnoteDataFrameWithDataPreview(result)
    return result


def _create_middleware_displayhook_class(base_class):
    """Create a new displayhook class that wraps __call__ method to call middleware"""

    class DynamicOutputMiddlewareDisplayHook(base_class):
        def __call__(self, result=None):
            middleware_result = _process_execution_result(result)
            return super().__call__(middleware_result)

    return DynamicOutputMiddlewareDisplayHook


def add_output_middleware():
    """Replace type of the default display hook with dynamically created middleware class."""

    # IPython doesn't let us configure displayhook for interactive shell (ZMQInteractiveShell)
    # beforehand (e.g. in ipython_kernel_config.py)
    # We also can't change it like this:
    #     ip.displayhook = OutputMiddlewareDisplayHook(ip)
    # since then it loses established connection and output won't be properly forwarded further.
    # We can use wrapt.ObjectProxy to create a wrapper class (similar to what we do in data_preview.py),
    # but then we'll still need to reassign ip.displayhook:
    #     ip.displayhook = OurProxy(ip.displayhook)
    # And IPython can react to this reassignment and invoke some side effects, which we'd like to avoid.
    # So instead we change class of already initialized display hook instance. Since our class inherits
    # from the original class of displayhook and simply wraps the __call__ method, this doesn't interfere
    # with any internals of the original displayhook or interactive shell.
    ip = get_ipython()
    middleware_class = _create_middleware_displayhook_class(ip.displayhook.__class__)
    ip.displayhook.__class__ = middleware_class
