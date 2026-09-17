"""Exercise real local processes with ``poetry install --with dev --extras server``."""

import json
import os
import queue
import subprocess
import sys
import threading
import time
from urllib.request import Request, urlopen

import pytest
from jupyter_client import KernelManager


@pytest.fixture
def runtime_env(tmp_path):
    """Keep server state and configuration separate from the developer's setup."""
    env = os.environ.copy()
    for name in (
        "JUPYTER_CONFIG_DIR",
        "JUPYTER_DATA_DIR",
        "JUPYTER_RUNTIME_DIR",
        "IPYTHONDIR",
        "XDG_STATE_HOME",
    ):
        directory = tmp_path / name.lower()
        directory.mkdir(mode=0o700)
        env[name] = str(directory)
    return env


def _stop_process(process):
    """Reap a child even when an assertion or startup fails."""
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def test_jupyter_server_starts(tmp_path, runtime_env):
    """Serve an authenticated API request using the installed server extra."""
    log_path = tmp_path / "jupyter.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "jupyter_server",
                "--no-browser",
                "--allow-root",
                "--ServerApp.ip=127.0.0.1",
                "--ServerApp.port=0",
                "--IdentityProvider.token=smoke-test-token",
                f"--ServerApp.root_dir={tmp_path}",
            ],
            env=runtime_env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            info_file = (
                tmp_path / "jupyter_runtime_dir" / f"jpserver-{process.pid}.json"
            )
            deadline = time.monotonic() + 30
            while not info_file.exists():
                assert process.poll() is None, log_path.read_text()
                assert time.monotonic() < deadline, log_path.read_text()
                time.sleep(0.1)
            info = json.loads(info_file.read_text())
            request = Request(
                f"http://127.0.0.1:{info['port']}/api/kernelspecs",
                headers={"Authorization": "token smoke-test-token"},
            )
            with urlopen(request, timeout=10) as response:
                assert "python3" in json.load(response)["kernelspecs"]
        finally:
            _stop_process(process)


def test_kernel_execution_and_toolkit_display(runtime_env):
    """Exercise IPython hooks, deferred annotations, async execution and errors."""
    manager = KernelManager(kernel_name="python3")
    manager.start_kernel(env=runtime_env)
    client = manager.blocking_client()
    client.start_channels()
    try:
        client.wait_for_ready(timeout=30)
        code = """
import asyncio
import sys
import pandas as pd
from deepnote_toolkit.dataframe_utils import add_formatters
from deepnote_toolkit.output_middleware import add_output_middleware
from deepnote_toolkit.notebook_functions import serialize_export, parse_export_data
add_formatters()
add_output_middleware()
def annotated(value: int) -> int:
    return value + 1
data, content_type = serialize_export(annotated, "dill")
restored = parse_export_data(data, "dill", "function")
assert restored(41) == 42
assert restored.__annotations__ == {"value": int, "return": int}
await asyncio.sleep(0)
assert sys.version_info[:2] == EXPECTED_PYTHON
pd.DataFrame({"value": [42]})
""".replace("EXPECTED_PYTHON", repr(sys.version_info[:2]))
        messages = []
        reply = client.execute_interactive(
            code, timeout=30, output_hook=messages.append
        )
        assert reply["content"]["status"] == "ok", reply
        outputs = [
            message["content"].get("data", {})
            for message in messages
            if message["header"]["msg_type"] in {"execute_result", "display_data"}
        ]
        mime = "application/vnd.deepnote.dataframe.v3+json"
        assert any(mime in output and "error" not in output[mime] for output in outputs)

        reply = client.execute_interactive(
            "raise ValueError('smoke error')", timeout=30, output_hook=lambda _: None
        )
        assert reply["content"]["ename"] == "ValueError"
        if sys.version_info >= (3, 14):
            reply = client.execute_interactive(
                'name = "world"\nassert t"hello {name}".values == ("world",)',
                timeout=30,
                output_hook=lambda _: None,
            )
            assert reply["content"]["status"] == "ok", reply
    finally:
        client.stop_channels()
        manager.shutdown_kernel(now=True)


def _send_lsp(process, message):
    """Write a JSON-RPC message using LSP's byte-counted framing."""
    body = json.dumps({"jsonrpc": "2.0", **message}).encode()
    process.stdin.write(f"Content-Length: {len(body)}\r\n\r\n".encode() + body)
    process.stdin.flush()


def _read_lsp(process, message_id):
    """Wait for one response with a bounded timeout, ignoring notifications."""
    responses = queue.Queue()

    def read():
        try:
            while True:
                headers = {}
                while True:
                    line = process.stdout.readline()
                    if not line:
                        raise EOFError("Language server closed stdout")
                    if line == b"\r\n":
                        break
                    key, value = line.decode().split(":", 1)
                    headers[key.lower()] = value.strip()
                response = json.loads(
                    process.stdout.read(int(headers["content-length"]))
                )
                if response.get("id") == message_id:
                    responses.put(response)
                    return
        except Exception as error:
            responses.put(error)

    threading.Thread(target=read, daemon=True).start()
    result = responses.get(timeout=30)
    if isinstance(result, Exception):
        raise result
    assert "error" not in result, result
    return result["result"]


def test_language_server_completion(tmp_path, runtime_env):
    """Initialize our LSP fork and request a completion over its actual protocol."""
    with (tmp_path / "pylsp.log").open("w") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "pylsp"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log,
            env=runtime_env,
        )
        try:
            _send_lsp(
                process,
                {
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "processId": os.getpid(),
                        "rootUri": tmp_path.as_uri(),
                        "capabilities": {},
                    },
                },
            )
            assert "capabilities" in _read_lsp(process, 1)
            _send_lsp(process, {"method": "initialized", "params": {}})
            uri = (tmp_path / "example.py").as_uri()
            _send_lsp(
                process,
                {
                    "method": "textDocument/didOpen",
                    "params": {
                        "textDocument": {
                            "uri": uri,
                            "languageId": "python",
                            "version": 1,
                            "text": "import os\nos.pa",
                        }
                    },
                },
            )
            _send_lsp(
                process,
                {
                    "id": 2,
                    "method": "textDocument/completion",
                    "params": {
                        "textDocument": {"uri": uri},
                        "position": {"line": 1, "character": 5},
                    },
                },
            )
            result = _read_lsp(process, 2)
            items = result["items"] if isinstance(result, dict) else result
            assert any(item["label"] == "path" for item in items), items
            _send_lsp(process, {"id": 3, "method": "shutdown", "params": None})
            _read_lsp(process, 3)
            _send_lsp(process, {"method": "exit", "params": None})
            process.wait(timeout=10)
            assert process.returncode == 0
        finally:
            _stop_process(process)
            process.stdin.close()
            process.stdout.close()
