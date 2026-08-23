#!/usr/bin/env python3
"""QUAL-05's state-size instrument: how much keyed state the job is holding,
measured from OUTSIDE the engine.

The campaign's whole claim is that state PLATEAUS, so the number behind it
must not be something the engine reports about itself. It is taken instead
from the artefacts the engine writes for its own recovery: the file-backed
state backend serialises each subtask's keyed state to
`<dir>/checkpoint-<id>.snap`, and the campaign puts that directory on the
shared mount, so the operations host can size a checkpoint without asking
any clink process anything.

What is reported is ONE checkpoint's worth of state, not the directory's
total. The directory also holds the checkpoints being retained for
recovery, so its total grows with the retention count and would read as
state growth on a job whose state was perfectly flat.

"One checkpoint's worth" is the NEWEST SNAPSHOT IN EACH SUBTASK
DIRECTORY, summed - the same shape QUAL-04's instrument used against the
object store's per-prefix manifests. The obvious alternative, "the newest
id present in every subtask directory", is wrong in practice: retention
purges old checkpoints per subtask at slightly different moments, so at
any instant the id sets differ and the newest id they all share is a stale
one. Measured on the first local run it reported 9.5 KB against a
directory holding 494 KB.

There is no partial-checkpoint hazard to work around: the file backend
writes to a temp name and renames, so a .snap file that exists is
complete.

Refuses rather than reporting zero when it can find nothing: a size gate
that silently reads 0 on a mis-typed path is a gate that cannot fail.
"""
import argparse
import os
import re
import sys
from collections import defaultdict

SNAP_RE = re.compile(r"^checkpoint-(\d+)\.snap$")


def scan(root):
    """({subtask dir: {checkpoint id: size}}, total bytes, total files)."""
    by_dir = defaultdict(dict)
    total_bytes = 0
    total_files = 0
    for dirpath, _dirnames, filenames in os.walk(root):
        for fn in filenames:
            m = SNAP_RE.match(fn)
            if not m:
                continue
            path = os.path.join(dirpath, fn)
            try:
                size = os.path.getsize(path)
            except OSError:
                continue  # purged underneath us between listing and stat
            by_dir[dirpath][int(m.group(1))] = size
            total_bytes += size
            total_files += 1
    return by_dir, total_bytes, total_files


def newest_per_subtask(by_dir):
    """(newest id seen, subtask dirs counted, summed bytes).

    Each subtask contributes its own most recent snapshot, so a subtask
    that has not yet written checkpoint N contributes N-1 rather than
    dropping out of the measurement and halving it.
    """
    if not by_dir:
        return None
    total = 0
    newest = 0
    for _d, per_id in by_dir.items():
        cid = max(per_id)
        newest = max(newest, cid)
        total += per_id[cid]
    return newest, len(by_dir), total


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="checkpoint directory (shared mount)")
    ap.add_argument("--detail", action="store_true")
    args = ap.parse_args()

    if not os.path.isdir(args.dir):
        print(f"ckptsize: {args.dir} is not a directory", file=sys.stderr)
        return 2

    by_dir, total_bytes, total_files = scan(args.dir)
    newest = newest_per_subtask(by_dir)
    if newest is None:
        print(
            f"ckptsize: no checkpoint-<id>.snap files under {args.dir}; "
            "nothing to measure (refusing to report 0)",
            file=sys.stderr,
        )
        return 3
    cid, dirs, live = newest

    if not args.detail:
        print(live)
        return 0

    print(f"state_live_bytes={live}")
    print(f"state_live_mib={live / (1024 * 1024):.2f}")
    print(f"state_checkpoint_id={cid}")
    print(f"state_subtask_dirs={dirs}")
    print(f"state_ids_retained={len({i for per in by_dir.values() for i in per})}")
    print(f"state_dir_bytes={total_bytes}")
    print(f"state_dir_files={total_files}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
