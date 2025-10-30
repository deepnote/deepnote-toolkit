import json
from unittest.mock import MagicMock

import pytest

from installer.module.kernels import ensure_symlinked_python_in_kernel_spec
from installer.module.virtual_environment import VirtualEnvironment


@pytest.fixture
def mock_venv():
    """Fixture to create a mock virtual environment."""
    return MagicMock(spec=VirtualEnvironment)


def test_ensure_symlinked_python_in_kernel_spec(tmpdir, mock_venv):
    """
    Test ensure_symlinked_python_in_kernel_spec with mocked dependencies.

    Uses pytest's tmpdir fixture to simulate the filesystem.
    """

    # Create a temporary directory for the kernel spec
    kernel_dir = tmpdir.mkdir("test_kernel")
    kernel_json_path = kernel_dir.join("kernel.json")

    # Mock the content of the kernel.json file
    kernel_json_content = {
        "argv": [
            "/usr/bin/python",
            "-m",
            "ipykernel_launcher",
            "-f",
            "/root/.local/share/jupyter/runtime/python-kernel-cb1aaa14-f8e8-4a00-b2d3-5f32dddb3ea3.json",  # intentionally containing python in name
        ]
    }
    kernel_json_path.write(json.dumps(kernel_json_content))

    # Call the function under test
    ensure_symlinked_python_in_kernel_spec(
        mock_venv,
        {"test_kernel": {"resource_dir": str(kernel_dir)}},
    )

    # Read the modified kernel.json and check its content
    modified_content = json.loads(kernel_json_path.read())
    expected_modified_content = {
        "argv": [
            "python",
            "-m",
            "ipykernel_launcher",
            "-f",
            "/root/.local/share/jupyter/runtime/python-kernel-cb1aaa14-f8e8-4a00-b2d3-5f32dddb3ea3.json",
        ]
    }
    assert modified_content == expected_modified_content
