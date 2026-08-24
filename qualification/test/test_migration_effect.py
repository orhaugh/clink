#!/usr/bin/env python3
"""QUAL-11's state-read migration gate, against synthetic state dumps.

The gate exists because the sink-observable version could be erased by an
unrelated fault (a worker kill discarding the at-least-once sink's buffer
took the evidence with it). This one reads the savepoints, so every case
below is a byte-level statement about what the migration wrote.
"""
import json
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
CHECK = HERE.parent / "qual11" / "migration_effect.py"

SENTINEL_MIN = 2**63 - 1
SENTINEL_MAX = -(2**63)


def val(*fields):
    return "0x" + b"".join(int(f).to_bytes(8, "little", signed=True) for f in fields).hex()


def dump(entries):
    return {"source": "t", "operators": [
        {"op": 1, "slots": [{"slot": "account_state", "count": len(entries),
                             "entries": [{"key_group": 0, "key": k, "value": v}
                                         for k, v in entries.items()]}]}]}


def run(v1, v2):
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        (d / "v1.json").write_text(json.dumps(dump(v1)))
        (d / "v2.json").write_text(json.dumps(dump(v2)))
        out = subprocess.run(
            [sys.executable, str(CHECK), "--v1-dump", str(d / "v1.json"),
             "--v2-dump", str(d / "v2.json"), "--out", str(d / "e.json")],
            capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
        return json.loads((d / "e.json").read_text())


failures = []


def check(name, cond, detail=""):
    if cond:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


# A clean migration of two untouched keys: count and sum carried, the new
# fields exactly the sentinels the migration writes.
r = run({"0x01": val(10, 500), "0x02": val(7, 70)},
        {"0x01": val(10, 500, SENTINEL_MIN, SENTINEL_MAX),
         "0x02": val(7, 70, SENTINEL_MIN, SENTINEL_MAX)})
check("untouched keys show the migration's exact output",
      r["carried"] == 2 and r["predicted_ok"] == 2 and r["predicted_bad"] == 0 and r["lost"] == 0, r)

# A key the job has folded into since the restore: carried, but its
# sentinels are legitimately gone, so it is not predicted-evidence.
r = run({"0x01": val(10, 500)}, {"0x01": val(14, 900, 3, 400)})
check("a key touched since the restore counts as carried, not as evidence",
      r["carried"] == 1 and r["untouched"] == 0 and r["predicted_ok"] == 0, r)

# State loss: the key is simply absent after the boundary.
r = run({"0x01": val(10, 500)}, {})
check("a key missing after the boundary is loss", r["lost"] == 1 and r["carried"] == 0, r)

# State loss wearing a healthy costume: the entry exists but the count
# went backwards (a fresh start that has already folded a few events).
r = run({"0x01": val(10, 500)}, {"0x01": val(3, 90, 1, 50)})
check("a count that went BACKWARDS is loss", r["lost"] == 1 and r["carried"] == 0, r)

# The migration ran but wrote the wrong thing into the new fields.
r = run({"0x01": val(10, 500)}, {"0x01": val(10, 500, 0, 0)})
check("an untouched key whose new fields are not the sentinels fails",
      r["predicted_bad"] == 1 and r["predicted_ok"] == 0, r)

# The migration did not run at all: the value is still 16 bytes.
r = run({"0x01": val(10, 500)}, {"0x01": val(10, 500)})
check("an unmigrated (16-byte) value is loss, not a pass",
      r["lost"] == 1 and r["predicted_ok"] == 0, r)

# The sum must carry too, not just the count.
r = run({"0x01": val(10, 500)}, {"0x01": val(10, 499, SENTINEL_MIN, SENTINEL_MAX)})
check("a carried count with a REGRESSED sum is loss", r["lost"] == 1, r)

# No untouched keys at all: carried is provable, predicted output is not,
# and the gate must not invent evidence it does not have.
r = run({"0x01": val(10, 500)}, {"0x01": val(99, 9000, 1, 900)})
check("no untouched key means no predicted-output evidence",
      r["carried"] == 1 and r["untouched"] == 0 and r["predicted_ok"] == 0
      and r["predicted_bad"] == 0, r)

print(f"\n{8 - len(failures)} passed, {len(failures)} failed")
sys.exit(1 if failures else 0)
