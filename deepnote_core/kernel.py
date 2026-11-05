"""Kernel class for Deepnote kernels, set with ``c.IPKernelApp.kernel_class``.

It works around ipykernel 6 bugs that make restarts wait 2.5 s for
jupyter_client to kill the old kernel, and that fail control requests sent
while the kernel starts. This module is imported before the shell exists, so it
must not import deepnote_toolkit.
"""

import asyncio
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
    """IPython kernel with fixes for ipykernel 6 start and shutdown bugs."""

    def __init__(self, **kwargs: Any) -> None:
        """Create the kernel and register an exit hook for the control thread."""
        super().__init__(**kwargs)
        # Exit hooks run last-in, first-out and ipykernel registers its own first,
        # so this stops the control thread while IOPub can still serve its flushes
        atexit.register(self._stop_control_thread)

    def start(self) -> None:
        """Start handling messages, with the control lock created first."""
        # ipykernel 6 creates it only after control requests start arriving, so an
        # early one fails. Fixed upstream in 7.4 (ipython/ipykernel#1568).
        self._control_lock = asyncio.Lock()
        super().start()

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
