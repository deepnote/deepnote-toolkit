"""Unified environment accessor for toolkit code.

Prefer reading configuration via deepnote_toolkit.config.get_config().
For dynamic values injected at runtime by integrations, this module keeps
an in-memory map and falls back to process environment.
"""

from __future__ import annotations

import os
import threading
from typing import Optional

_STATE: dict[str, str] = {}
_LOCK = threading.RLock()
# Public alias emphasizing this lock protects both _STATE and os.environ updates
_STATE_LOCK = _LOCK


def _get_runtime_var(name: str) -> Optional[str]:
    with _LOCK:
        return _STATE.get(name)


def _set_runtime_var(name: str, value: str) -> None:
    with _LOCK:
        _STATE[name] = value


def _clear_runtime_var(name: str) -> None:
    with _LOCK:
        _STATE.pop(name, None)


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Get env var from in-memory runtime map first, then OS environ."""
    val = _get_runtime_var(name)
    if val is not None:
        return val
    return os.environ.get(name, default)


def has_env(name: str) -> bool:
    """Return True if set in runtime map or OS environ."""
    return _get_runtime_var(name) is not None or (name in os.environ)


def set_env(name: str, value: str) -> None:
    """Set env var in both runtime map and OS environ atomically."""
    with _STATE_LOCK:
        # Use the same lock for both updates to keep them in sync
        _set_runtime_var(name, value)
        os.environ[name] = value


def unset_env(name: str) -> None:
    """Unset env var from both runtime map and OS environ atomically."""
    with _STATE_LOCK:
        # Use the same lock for both updates to keep them in sync
        _clear_runtime_var(name)
        os.environ.pop(name, None)
