"""Pytest configuration and fixtures for unit tests."""

import os
import tempfile
from typing import Generator

import pytest


@pytest.fixture(autouse=True, scope="session")
def apply_runtime_patches() -> None:
    """Apply runtime patches once before any tests run."""
    from deepnote_toolkit.runtime_patches import apply_runtime_patches

    apply_runtime_patches()


@pytest.fixture(autouse=True)
def clean_runtime_state() -> Generator[None, None, None]:
    """Automatically clean in-memory env state and config cache before and after each test."""
    from deepnote_toolkit import env as dnenv
    from deepnote_toolkit.config import clear_config_cache

    # Clean before test
    try:
        lock = getattr(dnenv, "_STATE_LOCK", None) or getattr(dnenv, "_LOCK", None)
        state = getattr(dnenv, "_STATE", None)
        if lock and state is not None:
            with lock:
                state.clear()
    except Exception:
        pass

    # Clear config cache before test
    clear_config_cache()

    yield

    # Clean after test
    try:
        lock = getattr(dnenv, "_STATE_LOCK", None) or getattr(dnenv, "_LOCK", None)
        state = getattr(dnenv, "_STATE", None)
        if lock and state is not None:
            with lock:
                state.clear()
    except Exception:
        pass

    # Clear config cache after test
    clear_config_cache()


@pytest.fixture(autouse=True, scope="session")
def test_log_directory() -> Generator[str, None, None]:
    """Set a temporary log directory for tests to avoid permission issues."""
    # Save original value
    original_log_dir = os.environ.get("DEEPNOTE_PATHS__LOG_DIR")

    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["DEEPNOTE_PATHS__LOG_DIR"] = tmpdir
        yield tmpdir

        # Restore original value
        if original_log_dir is None:
            os.environ.pop("DEEPNOTE_PATHS__LOG_DIR", None)
        else:
            os.environ["DEEPNOTE_PATHS__LOG_DIR"] = original_log_dir
