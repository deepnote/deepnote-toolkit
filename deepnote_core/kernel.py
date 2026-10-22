"""Kernel class for Deepnote kernels, set with ``c.IPKernelApp.kernel_class``.

It lets the old kernel exit on restart instead of waiting 2.5 s for
jupyter_client to kill it. This module is imported before the shell exists, so
it must not import deepnote_toolkit.
"""

import atexit
from typing import TYPE_CHECKING, Any, List

from ipykernel.ipkernel import IPythonKernel

if TYPE_CHECKING:
    import psutil


def _is_running(process: "psutil.Process") -> bool:
    """Return whether the process exists and isn't a zombie."""
    # Imported here because psutil is optional since ipykernel 7, which lists no
    # children without it
    import psutil

    try:
        return process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


class DeepnoteKernel(IPythonKernel):
    """IPython kernel that exits on shutdown instead of waiting to be killed."""

    def __init__(self, **kwargs: Any) -> None:
        """Create the kernel and register an exit hook for the control thread."""
        super().__init__(**kwargs)
        # Exit hooks run last-in, first-out and ipykernel registers its own first,
        # so this stops the control thread while IOPub can still serve its flushes
        atexit.register(self._stop_control_thread)

    def _stop_control_thread(self) -> None:
        """Stop the control thread and wait up to 2 s for it to finish."""
        thread = self.control_thread
        if thread is not None and thread.is_alive():
            thread.stop()
            thread.join(timeout=2)

    def _process_children(self) -> List["psutil.Process"]:
        """List child processes, leaving out exited ones nobody has reaped."""
        # ipykernel's shutdown signals listed children until none are left, which
        # never happens for an exited one
        return [child for child in super()._process_children() if _is_running(child)]
