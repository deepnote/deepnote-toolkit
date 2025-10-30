import os
import subprocess
import threading
import time

import pytest
import requests


def read_stream(stream, buffer):
    """Read the stream line by line and store it in the buffer."""
    while True:
        line = stream.readline()
        if not line:
            break
        buffer.append(line)


@pytest.fixture(scope="module")
def installer_process():
    """Create a subprocess for the installer."""
    installer_bundle_path = os.getenv("INSTALLER_BUNDLE_PATH")
    server_process = subprocess.Popen(
        ["python", installer_bundle_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Give the server some time to start
    # TODO replace with retrying requests to /info instead of fixing the timeout
    time.sleep(15)

    # Buffers to store stdout and stderr
    stdout_buffer = []
    stderr_buffer = []

    # Threads to read stdout and stderr
    stdout_thread = threading.Thread(
        target=read_stream, args=(server_process.stdout, stdout_buffer)
    )
    stderr_thread = threading.Thread(
        target=read_stream, args=(server_process.stderr, stderr_buffer)
    )

    # Start the threads
    stdout_thread.start()
    stderr_thread.start()

    stdout_string = "".join(stdout_buffer)
    stderr_string = "".join(stderr_buffer)

    yield server_process, stdout_string, stderr_string

    # Stop the server
    server_process.terminate()
    server_process.wait()


def test_installer_bootstrap(installer_process):
    _, stdout, stderr = installer_process

    assert (
        "Jupyter Notebook"
        and "is running" in stdout
        or "JupyterLab"
        and "is running" in stderr
    )


def test_kernel_start(installer_process):
    _, _, _ = installer_process

    response = requests.post(
        "http://localhost:8888/api/kernels", json={"name": "python3"}
    )
    assert response.status_code == 201

    json_response = response.json()
    assert json_response["execution_state"] == "starting"


def test_metrics_endpoint(installer_process):
    """Test that jupyter-resource-usage metrics endpoint is accessible."""
    _, _, _ = installer_process

    response = requests.get("http://localhost:8888/api/metrics/v1")
    assert response.status_code == 200

    json_response = response.json()
    assert "rss" in json_response, "Expected 'rss' (memory) field in metrics response"
    assert "limits" in json_response, "Expected 'limits' field in metrics response"

    assert isinstance(json_response["rss"], (int, float)), "rss should be a number"
    assert json_response["rss"] > 0, "rss (memory usage) should be greater than 0"
