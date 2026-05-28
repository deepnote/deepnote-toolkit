"""Kernel-state snapshot / restore for cold-start reduction.

The dominant cold-start cost in Deepnote today is re-running the init notebook
on every fresh container boot. For projects with heavy init (data loading,
client setup), this can be tens of seconds to minutes per cold start.

This module captures the kernel's globals after init completes and restores
them on the next cold boot, skipping the init re-run entirely.

# Design surface

Three layers, each with its own file:

  - `store`: a `SnapshotStore` protocol with a `LocalDiskSnapshotStore` impl
    for the POC. The S3 production impl will be a sibling later.
  - `checkpoint`: `save_checkpoint` and `try_restore_checkpoint` — the actual
    serialise/deserialise logic. Built on `dill` (already a deepnote-toolkit
    dep) to handle closures and most user-defined types.
  - `key`: `compute_checkpoint_key` — the stable cache-key composition. Any
    input change here (init source, environment) must invalidate the cache.

A `__main__` CLI exists for end-to-end testing without touching production
code paths.

# Correctness invariant

A restore is correct iff the restored namespace produces the same downstream
execution behaviour as freshly re-running init in the current environment.

The checkpoint key includes the init source hash and environment id so that
source/env changes always invalidate the cache. What the key does NOT
capture (limitations the production PR must address):

  - filesystem state in `/work` — if init read a file that has since
    changed, restore returns stale data
  - external connections (db, http clients) — they are unpicklable and get
    skipped on save; restored kernel will need to re-establish them
  - cross-Python-version restore — dill bytes from 3.11 may not load on
    3.12; the env id is expected to discriminate Python versions

# What is NOT in this PR

  - S3 backend (LocalDiskSnapshotStore only)
  - Wiring into `runtime/executor.py` init lifecycle
  - Webapp signaling
  - Per-workspace feature flag

See the [POC plan](~/.claude/plans/snapshot-restore-poc.md) for the
productionisation path.
"""

from deepnote_toolkit.kernel_checkpoint.checkpoint import (
    RestoreReport,
    SaveReport,
    save_checkpoint,
    try_restore_checkpoint,
)
from deepnote_toolkit.kernel_checkpoint.key import compute_checkpoint_key
from deepnote_toolkit.kernel_checkpoint.store import (
    LocalDiskSnapshotStore,
    SnapshotStore,
)

__all__ = [
    "LocalDiskSnapshotStore",
    "RestoreReport",
    "SaveReport",
    "SnapshotStore",
    "compute_checkpoint_key",
    "save_checkpoint",
    "try_restore_checkpoint",
]
