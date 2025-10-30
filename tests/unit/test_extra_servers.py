"""Test extra server functionality with the new registry-based approach."""

from unittest import mock

from deepnote_core.runtime.types import ExtraServerSpec
from installer.module.executor import run_actions_in_installer_env


def test_extra_servers_via_registry():
    """Test that extra servers are properly started via the registry."""
    # Create mock venv
    mock_venv = mock.Mock()
    mock_proc1 = mock.Mock()
    mock_proc2 = mock.Mock()

    # Configure mock to return different processes for each call
    mock_venv.start_server.side_effect = [mock_proc1, mock_proc2]

    # Create mock logger
    mock_logger = mock.Mock()

    # Create ExtraServerSpec actions
    actions = [
        ExtraServerSpec(command=["echo", "a"]),
        ExtraServerSpec(command=["echo", "b"]),
    ]

    # Run actions via the registry
    processes = run_actions_in_installer_env(mock_venv, actions, mock_logger)

    # Verify results
    assert len(processes) == 2
    assert mock_proc1 in processes
    assert mock_proc2 in processes

    # Verify the commands were executed correctly
    assert mock_venv.start_server.call_count == 2
    calls = mock_venv.start_server.call_args_list
    assert calls[0][0][0] == "echo a"  # First command
    assert calls[1][0][0] == "echo b"  # Second command


def test_extra_servers_with_env_variables():
    """Test extra servers with environment variables."""
    # Create mock venv
    mock_venv = mock.Mock()
    mock_proc = mock.Mock()
    mock_venv.start_server.return_value = mock_proc

    # Create mock logger
    mock_logger = mock.Mock()

    # Create ExtraServerSpec action with environment variables
    actions = [
        ExtraServerSpec(
            command=["python", "server.py"], env={"PORT": "8080", "API_KEY": "secret"}
        ),
    ]

    # Run actions via the registry
    processes = run_actions_in_installer_env(mock_venv, actions, mock_logger)

    # Verify results
    assert len(processes) == 1
    assert mock_proc in processes

    # Verify the command was executed with env vars prefixed
    mock_venv.start_server.assert_called_once()
    command = mock_venv.start_server.call_args[0][0]
    # The env vars should be prefixed to the command
    assert "PORT=8080" in command
    assert "API_KEY=secret" in command
    assert "python server.py" in command
