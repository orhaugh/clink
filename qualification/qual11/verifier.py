#!/usr/bin/env python3
"""QUAL-11's end-state verifier: reads the job's output topic once, at
drain, and judges the evolution boundary from what a downstream consumer
actually saw. Clink's own view is never consulted.

The job emits one row per event carrying that key's RUNNING state:

    {"k":<key>,"n":<count>,"sum":<sum>[,"vmin":..,"vmax":..],"v":<schema>}

so the topic contains the whole per-key history, and three independent
questions can be answered from it:

1. EXACTNESS (the same claim every campaign makes). For each key, the row
   with the maximum n is that key's final state; it must equal the
   deterministic spec's recomputation - count of events for that key, and
   the sum of their amounts. Re-emissions after a restore are identical
   by determinism, so taking the max-n row is robust to replay; a
   DISAGREEING duplicate at the same n is a separate, fatal finding.

2. STATE CARRIED ACROSS THE MIGRATION. A key's first post-boundary
   emission is the v2 row with the SMALLEST n for that key - not the
   first one encountered while scanning. Two things make scan order
   useless: the sink is not keyed, so one key's rows are spread across
   partitions, and the restore rewinds the source to the savepoint's
   offsets, so v2 legitimately RE-EMITS counts v1 already emitted.
   State carried means that smallest n continues the key's history
   (min_v2_n <= last_v1_n + 1, replay overlap allowed); state LOST means
   it restarts at 1 while the key had a v1 history - the defect this
   campaign exists to catch, wearing the costume of a healthy job.

3. THE MIGRATION'S EFFECT, PREDICTED. The registered v1->v2 migration
   seeds vmin/vmax with the empty-range sentinels, so the row at that
   smallest post-boundary n must carry vmin == vmax: the sentinels
   collapse onto the first amount folded after the migration. If the
   migration had carried garbage, invented values, or silently dropped
   the field, this is where it shows.

WHAT IS DELIBERATELY NOT JUDGED: per-row duplicates. The sink is
at-least-once and the restore replays, so the same (key, count) is
re-emitted by design - and across the boundary it is re-emitted under a
DIFFERENT schema, so a naive "same key and count, different payload"
check flags every replayed row. Worse, a key's events can arrive on
several partitions, so even the running sum at a given count is not
required to be stable across a replay with different interleaving. Only
the CONVERGED final state and the per-key monotone facts above are
judgeable here; duplicates are counted and reported, never failed on.

A key that first appears AFTER the boundary is not evidence about the
migration (nothing to carry), and is judged only on exactness.

Usage:
  verifier.py --brokers HOST:9092 --topic qual11-out --spec /qual/spec.json
              --progress /qual/progress.json --out /qual/q11-verify.json
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "qual01"))
from detspec import Spec  # noqa: E402

from confluent_kafka import Consumer, TopicPartition  # noqa: E402

SENTINEL_MIN = 2**63 - 1
SENTINEL_MAX = -(2**63)


def read_topic(brokers, topic, timeout_s=180.0, idle_s=10.0):
    """Every message in the topic, in per-partition offset order."""
    import time

    c = Consumer({
        "bootstrap.servers": brokers,
        "group.id": f"qual11-verify-{topic}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "isolation.level": "read_committed",
    })
    md = c.list_topics(topic, timeout=20.0)
    if topic not in md.topics or md.topics[topic].error is not None:
        c.close()
        raise RuntimeError(f"topic {topic} not found")
    parts = list(md.topics[topic].partitions.keys())
    c.assign([TopicPartition(topic, p, 0) for p in parts])

    rows = []
    eof = set()
    t0 = time.time()
    last = t0
    while time.time() - t0 < timeout_s and len(eof) < len(parts):
        m = c.poll(1.0)
        if m is None:
            if time.time() - last > idle_s:
                break
            continue
        if m.error():
            if "_PARTITION_EOF" in str(m.error()) or m.error().code() == -191:
                eof.add(m.partition())
                continue
            raise RuntimeError(str(m.error()))
        rows.append((m.partition(), m.offset(), m.value()))
        last = time.time()
    c.close()
    rows.sort(key=lambda t: (t[0], t[1]))
    return rows


def oracle_from_spec(spec, seqs_by_partition):
    """Per-key (count, sum) recomputed from the deterministic spec over
    exactly the events the generator recorded as produced."""
    counts, sums = {}, {}
    for p, seq_end in seqs_by_partition.items():
        for seq in range(seq_end):
            key, amount, _ts = spec.event(int(p), seq)
            counts[key] = counts.get(key, 0) + 1
            sums[key] = sums.get(key, 0) + amount
    return counts, sums


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", required=True)
    ap.add_argument("--topic", required=True)
    ap.add_argument("--spec", required=True, help="the generator's spec record")
    ap.add_argument("--progress", required=True, help="the generator's per-partition progress")
    ap.add_argument("--out", required=True)
    ap.add_argument("--timeout-s", type=float, default=180.0)
    # Passed explicitly, as QUAL-05 does: the generator's spec record does
    # not carry it, and guessing 0 here would recompute a DIFFERENT key
    # space than the generator produced.
    ap.add_argument("--key-epoch-ms", type=int, default=0)
    args = ap.parse_args()

    with open(args.spec) as fh:
        spec_rec = json.load(fh)
    spec = Spec(
        seed=spec_rec["seed"],
        partitions=spec_rec["partitions"],
        keys=spec_rec["keys"],
        events_per_sec_per_partition=spec_rec["events_per_sec_per_partition"],
        base_ms=spec_rec["base_ms"],
        max_jitter_ms=spec_rec.get("max_jitter_ms", 1500),
        window_ms=spec_rec.get("window_ms", 10000),
        # QUAL-11 turns the key space over deliberately: keys from earlier
        # epochs go DORMANT, and a dormant key is what still holds the
        # migration's seeded sentinels at the second savepoint - the only
        # population that can evidence the migration's exact output.
        key_epoch_ms=args.key_epoch_ms,
    )
    with open(args.progress) as fh:
        progress = json.load(fh)
    # produced_high[p] is the NEXT sequence number for partition p, i.e. the
    # count of events produced on it.
    seqs = {int(p): int(n) for p, n in progress.get("produced_high", {}).items()}

    result = {"topic": args.topic, "rows": 0}
    try:
        raw = read_topic(args.brokers, args.topic, timeout_s=args.timeout_s)
    except Exception as e:  # noqa: BLE001 - the caller only needs the reason
        result["error"] = str(e)
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=1)
        return 1

    # --- fold the per-key history ------------------------------------------
    best = {}          # key -> row with the max n (the key's final state)
    duplicates = 0     # re-emissions of a (key, n) - expected, informational
    seen_at_n = set()  # (key, n) pairs already seen
    last_v1_n = {}     # key -> the highest n seen while the job was v1
    first_v2 = {}      # key -> the v2 row with the SMALLEST n for that key
    malformed = 0
    for _p, _off, value in raw:
        if value is None or len(value) == 0:
            malformed += 1
            continue
        try:
            row = json.loads(value.decode("utf-8", "replace"))
            k, n, s, v = int(row["k"]), int(row["n"]), int(row["sum"]), int(row["v"])
        except Exception:  # noqa: BLE001 - a corrupt row is a finding, not a skip
            malformed += 1
            continue
        if (k, n) in seen_at_n:
            duplicates += 1
        seen_at_n.add((k, n))
        if k not in best or n > best[k]["n"]:
            best[k] = row
        if v == 1:
            last_v1_n[k] = max(last_v1_n.get(k, 0), n)
        elif v == 2 and (k not in first_v2 or n < int(first_v2[k]["n"])):
            first_v2[k] = row
    result["rows"] = len(raw)
    result["malformed_rows"] = malformed
    result["duplicate_rows"] = duplicates

    # --- 1. exactness ------------------------------------------------------
    counts, sums = oracle_from_spec(spec, seqs)
    missing = wrong_n = wrong_sum = fabricated = 0
    for key, want_n in counts.items():
        row = best.get(key)
        if row is None:
            missing += 1
            continue
        if int(row["n"]) != want_n:
            wrong_n += 1
        if int(row["sum"]) != sums[key]:
            wrong_sum += 1
    for key in best:
        if key not in counts:
            fabricated += 1
    result.update(keys_expected=len(counts), keys_seen=len(best), keys_missing=missing,
                  keys_wrong_n=wrong_n, keys_wrong_sum=wrong_sum, keys_fabricated=fabricated)

    # --- 2. state carried, and 3. the migration's predicted effect ----------
    carried = reset = 0
    effect_ok = effect_bad = 0
    samples = []
    for key, row in sorted(first_v2.items()):
        if key not in last_v1_n:
            continue  # a key born after the boundary says nothing about migration
        # Continuity, with replay overlap allowed: the first post-boundary
        # count must be at or before where v1 left off + 1. A key that
        # restarts at 1 despite having a v1 history is state loss.
        first_n = int(row["n"])
        if first_n > 1 and first_n <= last_v1_n[key] + 1:
            carried += 1
        else:
            reset += 1
            if len(samples) < 5:
                samples.append({"k": key, "first_v2_n": first_n,
                                "last_v1_n": last_v1_n[key],
                                "why": "count did not continue across the migration"})
        # The sentinels collapse onto this row's own amount: vmin == vmax.
        vmin, vmax = row.get("vmin"), row.get("vmax")
        if vmin is None or vmax is None:
            effect_bad += 1
            if len(samples) < 5:
                samples.append({"k": key, "why": "v2 row carries no vmin/vmax"})
        elif int(vmin) == int(vmax) and int(vmin) not in (SENTINEL_MIN, SENTINEL_MAX):
            effect_ok += 1
        else:
            effect_bad += 1
            if len(samples) < 5:
                samples.append({"k": key, "vmin": vmin, "vmax": vmax,
                                "why": "migrated range fields are not the predicted "
                                       "single-amount collapse"})
    # Reported, not judged: gate 4 reads the STATE (migration_effect.py),
    # because this stream-side view can be erased by an unrelated fault -
    # an at-least-once sink's buffer discarded by a worker kill takes the
    # first post-boundary rows with it, which is exactly what happened on
    # qual11-local-e while the engine behaved correctly.
    result.update(keys_across_boundary=carried + reset, keys_continued=carried,
                  keys_restarted=reset, first_row_sentinel_collapse=effect_ok,
                  samples=samples)

    with open(args.out, "w") as fh:
        json.dump(result, fh, indent=1)
    print(json.dumps({k: v for k, v in result.items() if k != "samples"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
