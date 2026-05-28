"""End-to-end CLI for the kernel-checkpoint POC.

Two commands so the round-trip is testable without touching production code:

  $ python -m deepnote_toolkit.kernel_checkpoint save <key> <python_file>
  $ python -m deepnote_toolkit.kernel_checkpoint restore <key>

The `save` command execs the python file in a fresh namespace, then snapshots
the resulting globals. `restore` reads the snapshot back into a fresh
namespace and prints the restored names.

This is the proof-of-life that productionisation in `runtime/executor.py`
will be a wiring change, not a design change.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from deepnote_toolkit.kernel_checkpoint.checkpoint import (
    save_checkpoint,
    try_restore_checkpoint,
)
from deepnote_toolkit.kernel_checkpoint.store import LocalDiskSnapshotStore


def _cmd_save(args: argparse.Namespace) -> int:
    source = Path(args.python_file).read_text()
    namespace: dict[str, object] = {}
    exec(compile(source, args.python_file, "exec"), namespace)
    store = LocalDiskSnapshotStore(args.root)
    report = save_checkpoint(namespace, store, args.key)
    print(f"[checkpoint] saved {len(report.saved_names)} globals to {store._path_for(args.key)}")
    print(f"  bytes_written: {report.bytes_written}")
    if report.skipped_unpicklable:
        print("  skipped (unpicklable):")
        for name, kind in report.skipped_unpicklable:
            print(f"    - {name}: {kind}")
    if report.skipped_by_rule:
        print("  skipped (by rule):")
        for name, reason in report.skipped_by_rule:
            print(f"    - {name}: {reason}")
    print("  saved:")
    for name in report.saved_names:
        print(f"    - {name}")
    return 0


def _cmd_restore(args: argparse.Namespace) -> int:
    store = LocalDiskSnapshotStore(args.root)
    result = try_restore_checkpoint(store, args.key)
    if result is None:
        print(f"[checkpoint] no snapshot found for key: {args.key}")
        return 1
    globals_dict, report = result
    print(f"[checkpoint] restored {len(report.restored_names)} globals ({report.bytes_read} bytes):")
    for name in report.restored_names:
        value = globals_dict[name]
        print(f"  {name}: {type(value).__name__}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m deepnote_toolkit.kernel_checkpoint")
    parser.add_argument(
        "--root", default="/tmp/deepnote-checkpoint", help="Local-disk snapshot store root."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    save = sub.add_parser("save", help="Exec a python file and snapshot the resulting globals.")
    save.add_argument("key")
    save.add_argument("python_file")
    save.set_defaults(func=_cmd_save)

    restore = sub.add_parser("restore", help="Restore a snapshot into a fresh namespace and print names.")
    restore.add_argument("key")
    restore.set_defaults(func=_cmd_restore)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
