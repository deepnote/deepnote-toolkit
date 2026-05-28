"""Stable cache-key composition for kernel checkpoints.

The key MUST change whenever any input that influences post-init kernel state
changes. Today that's:

  - project_id        — checkpoints are project-scoped
  - init_source_hash  — re-running an updated init produces different state
  - environment_id    — pandas/numpy version bumps can change in-memory shapes

Production-shaped: a deterministic composition with all inputs visible in the
key string so debugging stale-restore incidents is one `cat` away.
"""

from __future__ import annotations


def compute_checkpoint_key(*, project_id: str, init_source_hash: str, environment_id: str) -> str:
    """Compose a stable checkpoint key from its inputs.

    Each component is required and must be a non-empty string. The format is
    `proj:<project_id>:init:<init_source_hash>:env:<environment_id>` so the
    key is human-readable when listing the snapshot store.
    """
    for name, value in (
        ("project_id", project_id),
        ("init_source_hash", init_source_hash),
        ("environment_id", environment_id),
    ):
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a non-empty string, got: {value!r}")

    return f"proj:{project_id}:init:{init_source_hash}:env:{environment_id}"
