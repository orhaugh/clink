#!/usr/bin/env python3
"""QUAL-05's state instrument.

The campaign's whole verdict rests on this number, so the shapes that
matter are the ones where it could quietly be wrong:

  * it must size ONE checkpoint's worth, not the directory. The directory
    also holds the checkpoints retained for recovery, so its total grows
    with the retention count and would read as state growth on a job whose
    state was perfectly flat - precisely the false FAIL this campaign
    would otherwise produce.
  * each subtask must contribute its OWN newest snapshot. Requiring one
    id shared by every subtask looks more rigorous and is worse:
    retention purges per subtask at slightly different moments, so the
    newest shared id is a stale one. On the first local run that read
    9.5 KB against a directory holding 494 KB.
  * it must REFUSE rather than report zero when it finds nothing. A gate
    that reads 0 from a mistyped path is a gate that cannot fail.
"""
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
CKPTSIZE = HERE.parent / "qual05" / "ckptsize.py"

failures = []


def check(name, ok, detail=""):
    if ok:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}{': ' + detail if detail else ''}")
        failures.append(name)


def run(d, detail=False):
    cmd = [sys.executable, str(CKPTSIZE), "--dir", str(d)]
    if detail:
        cmd.append("--detail")
    return subprocess.run(cmd, capture_output=True, text=True)


def write_ckpt(root, subtask, ckpt_id, size):
    d = root / f"subtask-{subtask}"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"checkpoint-{ckpt_id}.snap").write_bytes(b"x" * size)


with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    out = run(root)
    check("an empty directory REFUSES rather than reporting 0",
          out.returncode != 0 and out.stdout.strip() != "0",
          f"rc={out.returncode} stdout={out.stdout!r}")

with tempfile.TemporaryDirectory() as tmp:
    out = run(pathlib.Path(tmp) / "does-not-exist")
    check("a missing directory refuses", out.returncode != 0)

with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    # Two retained checkpoints, four subtasks each, 1000 bytes per file.
    for cid in (7, 8):
        for st in range(4):
            write_ckpt(root, st, cid, 1000)
    out = run(root)
    check("sizes ONE checkpoint's worth, not the whole directory",
          out.stdout.strip() == "4000",
          f"got {out.stdout.strip()!r}, expected 4000 (8000 would be the directory)")

with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    for st in range(4):
        write_ckpt(root, st, 7, 1000)
    # Two subtasks have landed checkpoint 8, two have not yet. Every
    # subtask still contributes its own newest, so nothing drops out.
    for st in range(2):
        write_ckpt(root, st, 8, 1200)
    out = run(root)
    check("a subtask still on the previous checkpoint still counts",
          out.stdout.strip() == "4400",
          f"got {out.stdout.strip()!r}, expected 4400 (2400 means the laggards were dropped)")

with tempfile.TemporaryDirectory() as tmp:
    # The shape that broke the first cut: retention has purged different
    # ids per subtask, so no id is shared by all of them.
    root = pathlib.Path(tmp)
    write_ckpt(root, 0, 9, 1000)
    write_ckpt(root, 0, 10, 1000)
    write_ckpt(root, 1, 10, 1000)
    write_ckpt(root, 1, 11, 1000)
    write_ckpt(root, 2, 11, 1000)
    write_ckpt(root, 2, 12, 1000)
    out = run(root)
    check("measures a directory where no checkpoint id is shared by every subtask",
          out.stdout.strip() == "3000",
          f"got {out.stdout.strip()!r}, expected 3000 (one newest per subtask)")

with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    for cid in (7, 8, 9):
        for st in range(2):
            write_ckpt(root, st, cid, 500)
    out = run(root, detail=True)
    kv = dict(
        line.split("=", 1) for line in out.stdout.splitlines() if "=" in line
    )
    check("--detail reports the newest checkpoint id seen",
          kv.get("state_checkpoint_id") == "9", f"got {kv.get('state_checkpoint_id')!r}")
    check("--detail reports live bytes for one checkpoint's worth alone",
          kv.get("state_live_bytes") == "1000", f"got {kv.get('state_live_bytes')!r}")
    check("--detail reports the retained count separately",
          kv.get("state_ids_retained") == "3", f"got {kv.get('state_ids_retained')!r}")
    check("--detail reports the directory total separately",
          kv.get("state_dir_bytes") == "3000", f"got {kv.get('state_dir_bytes')!r}")

with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    for st in range(3):
        write_ckpt(root, st, 4, 200)
    (root / "subtask-0" / "unrelated.txt").write_bytes(b"y" * 9999)
    out = run(root)
    check("ignores files that are not checkpoint snapshots",
          out.stdout.strip() == "600", f"got {out.stdout.strip()!r}")

print(f"\n{len(failures)} failed")
sys.exit(1 if failures else 0)
