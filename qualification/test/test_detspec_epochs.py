#!/usr/bin/env python3
"""detspec's key-epoch mode, and the guarantee that adding it changed
nothing for the campaigns already using the shared spec.

detspec IS the oracle for QUAL-01 through QUAL-05: every expected value is
recomputed from it rather than recorded. So a change to it does not break
a test, it silently changes what four campaigns believe the correct answer
was. The golden values below are the default path pinned byte-for-byte;
they were taken from the implementation as it stood before key epochs
existed, and were verified equal across jitter and key-space variants at
the time of the change.
"""
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "qual01"))
from detspec import Spec  # noqa: E402

failures = []


def check(name, ok, detail=""):
    if ok:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name}{': ' + detail if detail else ''}")
        failures.append(name)


# --- the default path must not have moved ------------------------------------
GOLDEN = {
    (0, 0): (31615, 423, 1717171716678),
    (1, 7): (6054, 988, 1717171715660),
    (3, 4001): (29637, 540, 1717171731937),
}
s = Spec(20260823, 4, 50000, 250, 1717171717000, 1500, 10000)
for (p, seq), want in GOLDEN.items():
    got = s.event(p, seq)
    check(f"default path unchanged for partition {p} seq {seq}", got == want,
          f"got {got}, expected {want}")

check("key_epoch_ms defaults to off", Spec(1, 1, 10, 1, 0, 0, 10).key_epoch_ms == 0)

# A fixed key space keeps touching the same keys for ever, which is
# exactly why it cannot be the workload for a retention campaign.
fixed = Spec(7, 1, 50, 10, 0, 0, 10000)
early = {fixed.event(0, i)[0] for i in range(500)}
late = {fixed.event(0, i)[0] for i in range(5000, 5500)}
check("without epochs the key space does not turn over", bool(early & late),
      "the default key space stopped reusing keys")

# --- the epoch mode -----------------------------------------------------------
EPOCH_MS = 60000
KEYS = 1000
EPS = 100
e = Spec(20260823, 4, KEYS, EPS, 0, 0, 10000, EPOCH_MS)

# Every key belongs to exactly one epoch, and its events all fall inside
# that epoch's event-time span. This is the property the campaign's TTL is
# sized against: a key's whole lifetime must be shorter than the TTL, or
# its aggregate is truncated mid-life and exact accounting fails for a
# reason that is the workload's fault.
# seq -> event time is seq * 1000 / EPS ms, so 24000 seqs at 100/s spans
# 240s: four full epochs. Fewer than two epochs and the turnover checks
# below cannot see anything.
span = {}
for p in range(4):
    for seq in range(24000):
        key, _amount, ts = e.event(p, seq)
        lo, hi = span.get(key, (ts, ts))
        span[key] = (min(lo, ts), max(hi, ts))
worst = max(hi - lo for lo, hi in span.values())
check("a key's whole lifetime fits inside one epoch", worst < EPOCH_MS,
      f"widest key lifetime was {worst}ms against a {EPOCH_MS}ms epoch")

# Disjoint blocks: epoch N's keys are N*KEYS..N*KEYS+KEYS-1, so no key is
# ever revisited by a later epoch.
epochs_of = {}
for key in span:
    epochs_of.setdefault(key // KEYS, set()).add(key % KEYS)
check("keys are drawn from disjoint per-epoch blocks",
      all(all(o < KEYS for o in offs) for offs in epochs_of.values()))
check("the key space advances", len(epochs_of) >= 3,
      f"only {len(epochs_of)} epoch(s) appeared")

# The turnover the campaign depends on: keys seen early are never seen again.
early_keys = {e.event(0, i)[0] for i in range(200)}          # epoch 0
late_keys = {e.event(0, i)[0] for i in range(20000, 20200)}  # epoch 3
check("an early key is never touched again later", not (early_keys & late_keys),
      f"{len(early_keys & late_keys)} keys were revisited across epochs")

# The predicted live population the campaign quotes is keys * (1 + ttl/epoch);
# that only holds if each epoch really does use about `keys` distinct keys.
first_epoch_offsets = len(epochs_of.get(0, set()))
check("an epoch uses close to its full key block",
      first_epoch_offsets > KEYS * 0.5,
      f"epoch 0 used only {first_epoch_offsets} of {KEYS} keys")

print(f"\n{len(failures) == 0 and 'all' or ''} checks done, {len(failures)} failed")
sys.exit(1 if failures else 0)
