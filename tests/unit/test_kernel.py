"""Tests for the Deepnote kernel class."""

import asyncio
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Iterator, List, Tuple

import psutil
import pytest
from ipykernel.control import ControlThread
from ipykernel.ipkernel import IPythonKernel
from jupyter_client.blocking.client import BlockingKernelClient
from jupyter_client.kernelspec import KernelSpecManager
from jupyter_client.manager import KernelManager

from deepnote_core.config.resources import get_resources_source_path
from deepnote_core.kernel import DeepnoteKernel, _is_running


@pytest.fixture
def kernel(tmp_path: Path) -> Iterator[Tuple[KernelManager, BlockingKernelClient]]:
    """A kernel started with the toolkit's IPython profile, without runtime init."""
    ipython_dir = tmp_path / "ipython"
    shutil.copytree(get_resources_source_path() / "ipython", ipython_dir)

    # A kernelspec of our own, so the kernel runs on this interpreter
    spec_dir = tmp_path / "kernels" / "toolkit-test"
    spec_dir.mkdir(parents=True)
    argv = [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"]
    (spec_dir / "kernel.json").write_text(
        json.dumps({"argv": argv, "display_name": "Test", "language": "python"})
    )

    manager = KernelManager(
        kernel_name="toolkit-test",
        kernel_spec_manager=KernelSpecManager(kernel_dirs=[str(tmp_path / "kernels")]),
    )
    env = {
        **os.environ,
        "IPYTHONDIR": str(ipython_dir),
        "DISABLE_KERNEL_INITIALIZATION_CUSTOM_HOOKS": "1",
    }
    manager.start_kernel(env=env)
    client = manager.client()
    client.start_channels()
    try:
        client.wait_for_ready(timeout=60)
        yield manager, client
    finally:
        client.stop_channels()
        if manager.is_alive():
            manager.shutdown_kernel(now=True)


class TestKernelWithProfile:
    """Kernels started with the toolkit's IPython profile."""

    def test_runs_deepnote_kernel(
        self, kernel: Tuple[KernelManager, BlockingKernelClient]
    ) -> None:
        """The profile makes the kernel app create a DeepnoteKernel."""
        _, client = kernel
        reply = client.execute_interactive(
            "",
            user_expressions={"kernel_class": "type(get_ipython().kernel).__name__"},
            timeout=30,
        )

        result = reply["content"]["user_expressions"]["kernel_class"]
        assert result["data"]["text/plain"] == "'DeepnoteKernel'"

    @pytest.mark.skipif(
        sys.platform != "linux",
        reason="Only Linux lists exited children in the kernel's process group",
    )
    def test_exited_child_does_not_delay_shutdown(
        self, kernel: Tuple[KernelManager, BlockingKernelClient]
    ) -> None:
        """A kernel with an exited, unreaped child shuts down without SIGTERM."""
        manager, client = kernel
        code = (
            "import subprocess, time, psutil\n"
            "child = subprocess.Popen(['true'])\n"
            "while psutil.Process(child.pid).status() != psutil.STATUS_ZOMBIE:\n"
            "    time.sleep(0.01)\n"
        )
        reply = client.execute_interactive(code, timeout=30)
        assert reply["content"]["status"] == "ok"

        process = manager.provisioner.process
        manager.shutdown_kernel()

        # A kernel that jupyter_client had to SIGTERM ends with -15
        assert process.returncode == 0


class TestStart:
    """Starting the kernel's message handlers."""

    def test_creates_control_lock_before_ipykernel_starts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The control lock exists before ipykernel starts handling control requests."""
        locks: List[asyncio.Lock] = []
        monkeypatch.setattr(
            IPythonKernel, "start", lambda kernel: locks.append(kernel._control_lock)
        )
        kernel = DeepnoteKernel.__new__(DeepnoteKernel)

        kernel.start()

        assert isinstance(locks[0], asyncio.Lock)


class TestIsRunning:
    """Telling running child processes from exited ones."""

    def test_only_running_process_counts(self) -> None:
        """A running process counts, an unreaped or reaped one doesn't."""
        running = subprocess.Popen(["sleep", "30"])
        exited = subprocess.Popen(["true"])
        try:
            exited_process = psutil.Process(exited.pid)
            while exited_process.status() != psutil.STATUS_ZOMBIE:
                time.sleep(0.01)

            assert _is_running(psutil.Process(running.pid))
            assert not _is_running(exited_process)

            exited.wait()
            assert not _is_running(exited_process)
        finally:
            running.kill()
            running.wait()


class TestStopControlThread:
    """The exit hook, called on a stand-in because a real kernel needs sockets."""

    def test_stops_running_thread(self) -> None:
        """The control thread is stopped and joined."""
        thread = ControlThread(daemon=True)
        thread.start()

        DeepnoteKernel._stop_control_thread(SimpleNamespace(control_thread=thread))

        assert not thread.is_alive()

    def test_does_nothing_without_thread(self) -> None:
        """A kernel without a control thread is left alone."""
        DeepnoteKernel._stop_control_thread(SimpleNamespace(control_thread=None))
