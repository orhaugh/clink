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

What is reported is the size of ONE checkpoint, not of the directory. The
directory also holds the checkpoints being retained for recovery, so its
total grows with the retention count and would read as state growth on a
job whose state was perfectly flat. The newest COMPLETE checkpoint - the
newest id present for every subtask that any id has - is the honest
snapshot of live state at a moment.

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
    """{checkpoint_id: [file sizes]} across every subtask directory."""
    by_id = defaultdict(list)
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
                continue  # a checkpoint being written or purged underneath us
            by_id[int(m.group(1))].append(size)
            total_bytes += size
            total_files += 1
    return by_id, total_bytes, total_files


def newest_complete(by_id):
    """(id, files, bytes) for the newest id written by every subtask.

    A checkpoint mid-write has files for some subtasks and not others, and
    sizing it would read as a dip. The subtask count is taken as the widest
    any id reaches rather than configured, so the instrument does not have
    to be told the parallelism and cannot disagree with it.
    """
    if not by_id:
        return None
    expected = max(len(v) for v in by_id.values())
    complete = [i for i, v in by_id.items() if len(v) == expected]
    if not complete:
        return None
    cid = max(complete)
    return cid, expected, sum(by_id[cid])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="checkpoint directory (shared mount)")
    ap.add_argument("--detail", action="store_true")
    args = ap.parse_args()

    if not os.path.isdir(args.dir):
        print(f"ckptsize: {args.dir} is not a directory", file=sys.stderr)
        return 2

    by_id, total_bytes, total_files = scan(args.dir)
    newest = newest_complete(by_id)
    if newest is None:
        print(
            f"ckptsize: no checkpoint-<id>.snap files under {args.dir}; "
            "nothing to measure (refusing to report 0)",
            file=sys.stderr,
        )
        return 3
    cid, files, live = newest

    if not args.detail:
        print(live)
        return 0

    print(f"state_live_bytes={live}")
    print(f"state_live_mib={live / (1024 * 1024):.2f}")
    print(f"state_checkpoint_id={cid}")
    print(f"state_subtask_files={files}")
    print(f"state_ids_retained={len(by_id)}")
    print(f"state_dir_bytes={total_bytes}")
    print(f"state_dir_files={total_files}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
