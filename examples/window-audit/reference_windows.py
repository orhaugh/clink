#!/usr/bin/env python3
"""Compute the windowed job's reference result for one scope.

    clink incident-scope BUNDLE --checkpoint=N --out=scope.json
    python3 reference_windows.py --scope=scope.json --bootstrap=HOST:PORT \\
        --run-start-offset=ALL=0 --out=reference.ndjson
    clink incident-reconcile BUNDLE --checkpoint=N --reference=reference.ndjson ...

Reads every record of the job's lineage, from its start to the scope's
checkpoint, with the connection given here (never one the scope names), after
confirming the cluster and topic are the logs the job read; computes the
windows windows_transform.RULES makes of them with
clink_reference.TumblingWindows; and writes the artifact: a row per window
final at the checkpoint, and every window left out (open, or undetermined)
with why. Credentials: --kafka-config KEY=VALUE for librdkafka properties, and
--kafka-secret KEY=ENVVAR for a property whose value is read from the
environment, such as sasl.password.

A windowed scope starts at the job's own start, which its checkpoints do not
record; give where the run began reading with --run-start-offset=PARTITION=OFFSET
(or ALL=OFFSET), which the artifact records as the operator's.

Exits 0 with a complete artifact, 3 with one that says it is incomplete and
why, 2 on bad usage or a scope this cannot read.
"""

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import windows_transform as spec  # noqa: E402
from clink_reference import (ReferenceWriter, Scope, ScopeError, TumblingWindows,  # noqa: E402
                             read_scope, write_result)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--scope", required=True)
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--kafka-config", action="append", default=[], metavar="KEY=VALUE")
    ap.add_argument("--kafka-secret", action="append", default=[], metavar="KEY=ENVVAR")
    ap.add_argument("--run-start-offset", action="append", default=[], metavar="PARTITION=OFFSET")
    ap.add_argument("--deadline-s", type=float, default=300.0)
    ap.add_argument("--max-records", type=int, default=50_000_000)
    args = ap.parse_args()
    try:
        scope = Scope.load(args.scope)
    except (OSError, ValueError, ScopeError) as e:
        print("reference: %s" % e, file=sys.stderr)
        return 2
    if scope.version != 2:
        print("reference: the scope is not a windowed job's (version %d)" % scope.version,
              file=sys.stderr)
        return 2
    config = {}
    for kv in args.kafka_config:
        k, _, v = kv.partition("=")
        config[k] = v
    for kv in args.kafka_secret:
        k, _, env = kv.partition("=")
        if env not in os.environ:
            print("reference: --kafka-secret names %s, which is not set" % env, file=sys.stderr)
            return 2
        config[k] = os.environ[env]
    run_start = {}
    for kv in args.run_start_offset:
        p, _, off = kv.partition("=")
        for src in scope.sources:
            for r in src.ranges:
                if p.upper() == "ALL" or int(p) == r.partition:
                    run_start[(src.topic, r.partition)] = int(off)

    windows = TumblingWindows(scope, spec.RULES, spec.AGGREGATES)
    problems = []

    def on_record(rec):
        if problems:
            return
        try:
            windows.add(rec.partition, rec.offset, spec.event_time(rec.value), spec.select(rec.value))
        except (ValueError, KeyError, TypeError) as e:
            problems.append("record %s[%d]@%d cannot be read by the specification: %s"
                            % (rec.topic, rec.partition, rec.offset, e))

    implementation = {"name": spec.NAME, "version": spec.VERSION, "config": spec.SPEC}
    writer = ReferenceWriter(args.out, scope, implementation,
                             reader={"name": "clink_reference.read_scope", "version": "1"},
                             semantics=spec.RULES.semantics())
    try:
        read = read_scope(scope, args.bootstrap, on_record, config=config,
                          run_start_offsets=run_start, deadline_s=args.deadline_s,
                          max_records=args.max_records)
        incomplete = list(read.incomplete) + problems
        if not incomplete:
            result = windows.finish()
            write_result(writer, result, spec.output)
            bounds, records = result.source_bounds, result.records
        else:
            # Windows of part of the lineage are not computed at all.
            bounds = [{"checkpoint": lc.checkpoint, "watermark_ms": None} for lc in scope.lineage]
            records = {}
    except BaseException:
        writer.abandon()  # unfinished: clink refuses it
        raise
    complete = not incomplete
    writer.finish(complete, incomplete, read.verified, read.recorded, read.read,
                  source_bounds=bounds, records=records)
    state = "complete" if complete else "incomplete: " + "; ".join(incomplete)
    print("reference: %d record(s) read, %d final window(s), %d left out, in %.2f s, %s"
          % (read.records, writer.rows, writer.windows, read.seconds, state))
    return 0 if complete else 3


if __name__ == "__main__":
    sys.exit(main())
