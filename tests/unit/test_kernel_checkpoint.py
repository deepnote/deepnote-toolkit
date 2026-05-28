"""Round-trip tests for the kernel-checkpoint POC.

Each test exercises one shape of the snapshot/restore contract:
  - basic types survive
  - user-defined functions and classes survive (dill handles closures)
  - unpicklable items are skipped-not-fatal
  - dunders / modules / ipython artifacts are skipped by rule
  - key composition is stable, and any input change invalidates the key
"""

from __future__ import annotations

import pytest

from deepnote_toolkit.kernel_checkpoint import (
    LocalDiskSnapshotStore,
    compute_checkpoint_key,
    save_checkpoint,
    try_restore_checkpoint,
)


def _has_pandas() -> bool:
    try:
        import pandas  # noqa: F401
        return True
    except ImportError:
        return False


PANDAS_AVAILABLE = _has_pandas()


@pytest.fixture
def store(tmp_path) -> LocalDiskSnapshotStore:
    return LocalDiskSnapshotStore(root=tmp_path / "snap")


def test_roundtrip_primitives(store):
    globals_dict = {"x": 1, "y": "hello", "z": True, "w": None, "n": 3.14}
    save_checkpoint(globals_dict, store, "k1")

    result = try_restore_checkpoint(store, "k1")
    assert result is not None
    restored, _ = result
    assert restored == {"x": 1, "y": "hello", "z": True, "w": None, "n": 3.14}


def test_roundtrip_containers(store):
    globals_dict = {"lst": [1, 2, 3], "d": {"a": 1, "b": [True, False]}, "t": (1, 2)}
    save_checkpoint(globals_dict, store, "k1")

    restored, _ = try_restore_checkpoint(store, "k1")
    assert restored == globals_dict


def test_roundtrip_user_function(store):
    def adder(x, y):
        return x + y

    save_checkpoint({"adder": adder}, store, "k1")

    restored, _ = try_restore_checkpoint(store, "k1")
    assert restored["adder"](2, 3) == 5


def test_roundtrip_user_class_instance(store):
    class Holder:
        def __init__(self, n):
            self.n = n

        def doubled(self):
            return self.n * 2

    save_checkpoint({"obj": Holder(21)}, store, "k1")

    restored, _ = try_restore_checkpoint(store, "k1")
    assert restored["obj"].doubled() == 42


def test_unpicklable_is_skipped_rest_survives(store):
    # Generators are one of the few types dill genuinely refuses to serialise
    # (their internal frame state isn't recoverable). A real init script might
    # leak similar via a third-party library; this proves a single unpicklable
    # value doesn't take the whole snapshot down with it.
    bad = (x for x in range(3))
    next(bad)
    globals_dict = {"good": 42, "bad_gen": bad, "other_good": "ok"}
    report = save_checkpoint(globals_dict, store, "k1")

    assert "good" in report.saved_names
    assert "other_good" in report.saved_names
    assert any(name == "bad_gen" for name, _ in report.skipped_unpicklable)

    restored, _ = try_restore_checkpoint(store, "k1")
    assert restored == {"good": 42, "other_good": "ok"}


def test_modules_skipped_by_rule(store):
    import os as os_module  # noqa: PLC0415 — intentional, testing skip rule

    globals_dict = {"good": 1, "os_module": os_module}
    report = save_checkpoint(globals_dict, store, "k1")

    assert "good" in report.saved_names
    assert ("os_module", "module_object") in report.skipped_by_rule

    restored, _ = try_restore_checkpoint(store, "k1")
    assert restored == {"good": 1}


def test_dunder_names_skipped_by_rule(store):
    globals_dict = {"__name__": "__main__", "__builtins__": {}, "_private": 1, "public": 2}
    report = save_checkpoint(globals_dict, store, "k1")

    saved = set(report.saved_names)
    assert saved == {"public"}
    assert ("__name__", "dunder_or_private") in report.skipped_by_rule
    assert ("__builtins__", "dunder_or_private") in report.skipped_by_rule
    assert ("_private", "dunder_or_private") in report.skipped_by_rule


def test_ipython_artifacts_skipped(store):
    globals_dict = {"In": ["cell1"], "Out": {}, "exit": "shouldnt-survive", "real": 1}
    report = save_checkpoint(globals_dict, store, "k1")

    assert report.saved_names == ["real"]
    for name in ("In", "Out", "exit"):
        assert (name, "ipython_artifact") in report.skipped_by_rule


def test_restore_returns_none_when_key_missing(store):
    assert try_restore_checkpoint(store, "never-saved") is None


def test_restore_returns_none_for_different_key(store):
    save_checkpoint({"x": 1}, store, "key-a")
    assert try_restore_checkpoint(store, "key-b") is None


def test_report_bytes_written_matches_round_trip(store):
    save_report = save_checkpoint({"x": [1, 2, 3, 4, 5]}, store, "k1")
    restored, restore_report = try_restore_checkpoint(store, "k1")
    assert restored == {"x": [1, 2, 3, 4, 5]}
    assert save_report.bytes_written == restore_report.bytes_read
    assert save_report.bytes_written > 0


# ----- key composition -----


def test_key_is_stable_for_same_inputs():
    a = compute_checkpoint_key(project_id="p1", init_source_hash="h1", environment_id="e1")
    b = compute_checkpoint_key(project_id="p1", init_source_hash="h1", environment_id="e1")
    assert a == b


def test_key_changes_when_project_changes():
    a = compute_checkpoint_key(project_id="p1", init_source_hash="h1", environment_id="e1")
    b = compute_checkpoint_key(project_id="p2", init_source_hash="h1", environment_id="e1")
    assert a != b


def test_key_changes_when_init_source_hash_changes():
    a = compute_checkpoint_key(project_id="p1", init_source_hash="h1", environment_id="e1")
    b = compute_checkpoint_key(project_id="p1", init_source_hash="h2", environment_id="e1")
    assert a != b


def test_key_changes_when_environment_changes():
    a = compute_checkpoint_key(project_id="p1", init_source_hash="h1", environment_id="e1")
    b = compute_checkpoint_key(project_id="p1", init_source_hash="h1", environment_id="e2")
    assert a != b


def test_key_is_human_readable():
    key = compute_checkpoint_key(project_id="proj-123", init_source_hash="abc", environment_id="py311")
    assert "proj-123" in key
    assert "abc" in key
    assert "py311" in key


def test_key_rejects_empty_inputs():
    with pytest.raises(ValueError):
        compute_checkpoint_key(project_id="", init_source_hash="h", environment_id="e")
    with pytest.raises(ValueError):
        compute_checkpoint_key(project_id="p", init_source_hash="", environment_id="e")
    with pytest.raises(ValueError):
        compute_checkpoint_key(project_id="p", init_source_hash="h", environment_id="")


# ----- store implementation -----


def test_local_disk_store_atomic_overwrite(tmp_path):
    store = LocalDiskSnapshotStore(root=tmp_path / "snap")
    store.write("k", b"first")
    assert store.read("k") == b"first"
    store.write("k", b"second")
    assert store.read("k") == b"second"


def test_local_disk_store_returns_none_for_missing(tmp_path):
    store = LocalDiskSnapshotStore(root=tmp_path / "snap")
    assert store.read("never-written") is None


def test_local_disk_store_handles_keys_with_colons_and_slashes(tmp_path):
    store = LocalDiskSnapshotStore(root=tmp_path / "snap")
    key = "proj:abc/def:env:py311"
    store.write(key, b"payload")
    assert store.read(key) == b"payload"


# ----- pandas DataFrame round-trip, only if pandas is available -----


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
def test_roundtrip_pandas_dataframe(store):
    import pandas as pd

    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    save_checkpoint({"df": df}, store, "k1")
    restored, _ = try_restore_checkpoint(store, "k1")
    pd.testing.assert_frame_equal(restored["df"], df)
