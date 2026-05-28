"""Serialise / deserialise a Python globals namespace via dill.

# Save semantics

`save_checkpoint(globals_dict, store, key)` iterates the globals one-by-one
and tries to serialise each. On any individual failure (unpicklable file
handle, db connection, thread, etc.) the offending name is logged-and-
skipped; the rest of the snapshot is preserved. This is intentional — a
single unpicklable variable should not torch the entire init state.

Items skipped unconditionally:
  - dunders (`__name__`, `__builtins__`, ...) — kernel-bootstrap state we
    don't want to overwrite on restore
  - module objects — restoring them risks shadowing the freshly-booted
    kernel's own imports; the user code re-imports as needed
  - IPython / ipykernel artifacts — same risk as modules

# Restore semantics

`try_restore_checkpoint(store, key)` returns a dict (the restored globals)
on a successful read, or None if the key is absent. The caller is
responsible for *merging* the restored dict into the target namespace —
this lets the kernel's own bootstrap state survive.

Errors during dill load are NOT caught: a corrupt snapshot is a real
problem and should surface, not silently fail-open to running init.
"""

from __future__ import annotations

import logging
import types
from dataclasses import dataclass, field
from typing import Any

import dill

from deepnote_toolkit.kernel_checkpoint.store import SnapshotStore

logger = logging.getLogger(__name__)


@dataclass
class SaveReport:
    """Per-checkpoint save outcome — useful for tests and prod metrics."""

    saved_names: list[str] = field(default_factory=list)
    skipped_unpicklable: list[tuple[str, str]] = field(default_factory=list)
    skipped_by_rule: list[tuple[str, str]] = field(default_factory=list)
    bytes_written: int = 0


@dataclass
class RestoreReport:
    """Per-checkpoint restore outcome — useful for tests and prod metrics."""

    restored_names: list[str]
    bytes_read: int


_ALWAYS_SKIP_PREFIXES = ("_",)
_ALWAYS_SKIP_NAMES = {
    "In",
    "Out",
    "exit",
    "quit",
    "get_ipython",
}


def _should_skip_name(name: str) -> str | None:
    """Return a reason string when the name must be skipped, else None."""
    if name in _ALWAYS_SKIP_NAMES:
        return "ipython_artifact"
    if any(name.startswith(prefix) for prefix in _ALWAYS_SKIP_PREFIXES):
        return "dunder_or_private"
    return None


def _should_skip_value(value: Any) -> str | None:
    """Return a reason string when the value type must be skipped, else None."""
    if isinstance(value, types.ModuleType):
        return "module_object"
    return None


def save_checkpoint(globals_dict: dict[str, Any], store: SnapshotStore, key: str) -> SaveReport:
    """Snapshot the named globals to `store` under `key`.

    Returns a SaveReport so the caller can log/metric the result.
    """
    report = SaveReport()
    saved: dict[str, Any] = {}

    for name, value in globals_dict.items():
        rule_reason = _should_skip_name(name) or _should_skip_value(value)
        if rule_reason:
            report.skipped_by_rule.append((name, rule_reason))
            continue
        try:
            # Round-trip-test individually so an unpicklable value doesn't
            # corrupt the entire dict's dump later.
            dill.dumps(value)
        except Exception as exc:  # noqa: BLE001 — broad on purpose; dill raises many types
            report.skipped_unpicklable.append((name, type(exc).__name__))
            logger.info("[checkpoint] skipping unpicklable %s: %s", name, exc)
            continue
        saved[name] = value
        report.saved_names.append(name)

    payload = dill.dumps(saved)
    store.write(key, payload)
    report.bytes_written = len(payload)
    return report


def try_restore_checkpoint(store: SnapshotStore, key: str) -> tuple[dict[str, Any], RestoreReport] | None:
    """Attempt to restore from `store`. Returns (globals, report) or None if absent.

    Callers MERGE the returned dict into their target namespace — this module
    intentionally doesn't mutate any caller state.
    """
    payload = store.read(key)
    if payload is None:
        return None
    restored: dict[str, Any] = dill.loads(payload)
    return restored, RestoreReport(restored_names=list(restored.keys()), bytes_read=len(payload))
