#!/usr/bin/env python3
"""QUAL-02 oracle. Runs on the ops host, reads the sink database
directly, and judges clink's two-phase-commit sink without asking clink
anything.

The pipeline is one row in, one row out, so exactly-once has three
independently countable failure modes in the sink table:

  duplicates   the same event_id committed more than once - the sink
               committed a transaction twice, or replayed after a
               restore without rolling back its prepared work.
  gaps         a partition's committed sequences are not a contiguous
               prefix - data was lost. Contiguity is a valid invariant
               because a Kafka partition is read in offset order by one
               source subtask, and the 2PC sink commits whole barrier
               intervals, so committed rows for a partition are always
               a prefix of what was produced.
  foreign      an event_id outside the range the generator produced -
               fabricated or mis-parsed output.

The sink table deliberately carries NO unique constraint. A primary key
on event_id would make the database reject duplicates on clink's behalf,
which would turn the defect this campaign is looking for into a failed
insert somewhere else. The oracle must be able to SEE a duplicate land.

Prepared transactions are sampled throughout, because an orphaned
prepared transaction is a production hazard in its own right: it holds
locks and blocks vacuum indefinitely. The end-state assertion (none left
after a clean stop) is made by the campaign driver, not here.

Usage:
  verifier.py --dsn postgresql://... --progress /qual/q2-progress.json \\
              --out /qual/q2-verdict.json [--interval-s 30]
"""
import argparse
import json
import os
import sys
import time

import psycopg2


def read_progress(path: str):
    """Per-partition produced high-water (exclusive) from the generator.
    Returns None until the generator has flushed its first snapshot."""
    try:
        with open(path) as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return None
    high = doc.get("produced_high") or {}
    if not high:
        return None
    return {int(k): int(v) for k, v in high.items()}


def sample(cur, produced_high: dict):
    """One judgement pass over the whole sink table.

    Returns (findings, stats). findings is a list of dicts, each one a
    concrete defect with the evidence needed to reproduce it.
    """
    # The sequence cast is guarded rather than direct. A single row whose
    # event_id has no numeric sequence - exactly the fabricated output
    # this oracle is meant to catch - makes an unguarded ::bigint throw
    # for the WHOLE query, and a judging pass that cannot run reports no
    # defects. The malformed rows are counted instead, and reported.
    cur.execute("""
        WITH parsed AS (
            SELECT event_id,
                   split_part(event_id, '-', 1) AS part,
                   CASE WHEN split_part(event_id, '-', 2) ~ '^[0-9]+$'
                        THEN split_part(event_id, '-', 2)::bigint END AS seq
            FROM public.q2_out
        )
        SELECT part,
               count(*)                                AS rows_total,
               count(DISTINCT event_id)                AS rows_distinct,
               max(seq)                                AS max_seq,
               min(seq)                                AS min_seq,
               count(*) FILTER (WHERE seq IS NULL)     AS unparseable
        FROM parsed
        GROUP BY 1
    """)
    rows = cur.fetchall()

    findings = []
    stats = {"partitions": {}, "rows_total": 0, "rows_distinct": 0, "unparseable": 0}

    for part, rows_total, rows_distinct, max_seq, min_seq, unparseable in rows:
        stats["rows_total"] += rows_total
        stats["rows_distinct"] += rows_distinct
        stats["unparseable"] += unparseable

        # "p3" -> 3. Anything else is not an id this generator emits.
        valid_token = part.startswith("p") and part[1:].isdigit()
        p = int(part[1:]) if valid_token else None
        high = produced_high.get(p) if p is not None else None

        # Anything the generator could not have written is reported and
        # excluded from the prefix arithmetic below, which is only
        # meaningful over ids this campaign actually produced.
        if not valid_token:
            findings.append({"kind": "foreign_partition", "partition_token": part,
                             "rows": rows_total,
                             "detail": "event_id does not carry a p<partition> prefix"})
            continue
        if high is None:
            findings.append({"kind": "foreign_partition", "partition_token": part,
                             "rows": rows_total,
                             "detail": "no such partition was produced"})
            continue
        if unparseable:
            findings.append({"kind": "foreign_partition", "partition_token": part,
                             "rows": unparseable,
                             "detail": "event_id carries no numeric sequence"})
            continue

        stats["partitions"][p] = {
            "rows_total": rows_total, "rows_distinct": rows_distinct,
            "min_seq": min_seq, "max_seq": max_seq, "produced_high": high,
        }

        if rows_total != rows_distinct:
            findings.append({"kind": "duplicate", "partition": p,
                             "rows_total": rows_total,
                             "rows_distinct": rows_distinct,
                             "excess": rows_total - rows_distinct})
        # A committed prefix must start at 0 and be contiguous.
        if min_seq != 0:
            findings.append({"kind": "gap", "partition": p, "detail": "prefix does not start at 0",
                             "min_seq": min_seq})
        elif rows_distinct != max_seq + 1:
            findings.append({"kind": "gap", "partition": p,
                             "expected_distinct": max_seq + 1,
                             "actual_distinct": rows_distinct,
                             "missing": (max_seq + 1) - rows_distinct})
        # NOT a finding mid-flight. produced_high comes from the
        # generator's periodic progress SNAPSHOT, which is a LOWER bound
        # on what it has actually produced - an engine keeping up with
        # the generator legitimately commits sequences the snapshot has
        # not recorded yet, and treating a lower bound as an upper bound
        # made the oracle manufacture the defect it hunts (the local rig
        # produced 32 such "foreign" findings on a run whose end state
        # was exactly complete: 411,000 produced, 411,000 committed,
        # zero duplicates). The verifier never learns when the generator
        # has stopped, so it cannot make this call at all; the campaign
        # driver's post-drain completeness step owns it, where the final
        # progress file IS authoritative.
        if max_seq >= high:
            stats["ahead_of_snapshot"] = stats.get("ahead_of_snapshot", 0) + 1

    cur.execute("SELECT count(*), coalesce(min(prepared), now()) FROM pg_prepared_xacts")
    prepared_n, prepared_oldest = cur.fetchone()
    stats["prepared_xacts"] = prepared_n
    stats["prepared_oldest"] = str(prepared_oldest)
    return findings, stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--progress", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval-s", type=int, default=30)
    args = ap.parse_args()

    verdict = {
        "campaign": "QUAL-02",
        "started_wallclock": time.time(),
        "samples": 0,
        "findings": [],          # every defect ever seen, with its sample index
        "max_rows_committed": 0,
        "max_prepared_xacts_seen": 0,
        "last_stats": None,
        "sample_errors": 0,
        "stuck": False,
    }
    consecutive_errors = 0

    # Bounded everywhere: a PAUSED server (the pg_unavailable fault) hangs
    # connections and statements rather than refusing them, and an oracle
    # that can hang is an oracle whose silence reads as judging. The
    # timeouts turn the outage into counted sample errors, which the
    # stuck detector below bounds - and the fault's dwell is capped under
    # that bound, so injected outages can never fail the oracle by
    # themselves.
    def connect():
        # 60s, not 10s: the statement timeout exists to bound a PAUSED
        # server's hang, where any finite value works - but it also bounds
        # the sample query, whose cost grows with the table. At 10s the
        # oracle cancelled its own samples once the sink passed ~3M rows on
        # cloud disks and declared itself stuck against a spotless engine.
        # The index the campaign now creates keeps the sample fast; this
        # headroom keeps a slow plan from being read as a dead oracle.
        c = psycopg2.connect(args.dsn, connect_timeout=5,
                             options="-c statement_timeout=60000")
        c.autocommit = True
        return c

    conn = connect()

    while True:
        produced_high = read_progress(args.progress)
        if produced_high is None:
            print("verifier: waiting for the generator's first progress snapshot",
                  flush=True)
            time.sleep(5)
            continue
        try:
            with conn.cursor() as cur:
                findings, stats = sample(cur, produced_high)
        except psycopg2.Error as exc:
            # A sink-database blip is a fact worth recording, not a
            # reason to stop judging or to claim a defect in clink.
            #
            # But retrying forever is its own failure: a query that can
            # never succeed - one malformed row used to be enough - turns
            # the oracle into a process that looks alive, writes a verdict
            # file with no findings, and judges nothing. That reads as a
            # clean campaign. So a persistent failure is written INTO the
            # verdict as a stuck oracle, which the summary treats as a
            # failed campaign rather than a passing one.
            verdict["sample_errors"] += 1
            consecutive_errors += 1
            verdict["last_sample_error"] = f"{exc.__class__.__name__}: {exc}"
            print(f"verifier: sample failed ({exc.__class__.__name__}): {exc}", flush=True)
            if consecutive_errors >= 10:
                verdict["stuck"] = True
                verdict["clean"] = False
                tmp = args.out + ".tmp"
                with open(tmp, "w") as f:
                    json.dump(verdict, f, indent=2)
                os.replace(tmp, args.out)
                print("verifier: 10 consecutive failed samples - the oracle cannot judge. "
                      "Recorded as stuck; this campaign has no verdict.", flush=True)
            try:
                conn.close()
            except psycopg2.Error:
                pass
            time.sleep(5)
            try:
                conn = connect()
            except psycopg2.Error:
                pass
            continue
        consecutive_errors = 0

        verdict["samples"] += 1
        verdict["last_stats"] = stats
        verdict["max_rows_committed"] = max(verdict["max_rows_committed"],
                                            stats["rows_total"])
        verdict["max_prepared_xacts_seen"] = max(verdict["max_prepared_xacts_seen"],
                                                 stats["prepared_xacts"])
        for f in findings:
            f["sample"] = verdict["samples"]
            verdict["findings"].append(f)

        verdict["clean"] = not verdict["findings"]
        tmp = args.out + ".tmp"
        with open(tmp, "w") as f:
            json.dump(verdict, f, indent=2)
        os.replace(tmp, args.out)

        print(f"verifier: sample {verdict['samples']}: "
              f"{stats['rows_total']} rows committed, "
              f"{stats['prepared_xacts']} prepared, "
              f"{len(verdict['findings'])} findings so far", flush=True)
        time.sleep(args.interval_s)


if __name__ == "__main__":
    sys.exit(main())
