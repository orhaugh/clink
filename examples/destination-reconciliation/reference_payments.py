#!/usr/bin/env python3
"""Compute the payments job's reference result for one scope.

    clink incident-scope BUNDLE --checkpoint=N --out=scope.json
    python3 reference_payments.py --scope=scope.json --bootstrap=HOST:PORT --out=reference.ndjson
    clink incident-reconcile BUNDLE --checkpoint=N --key=event_id --reference=reference.ndjson ...

Reads exactly the scope's Kafka ranges with the connection given here (never
one the scope names), after confirming the cluster and topic are the logs the
job read, applies payments_transform.transform to each record, and writes the
artifact. Credentials: --kafka-config KEY=VALUE for librdkafka properties, and
--kafka-secret KEY=ENVVAR for a property whose value is read from the
environment, such as sasl.password.

A scope that starts at the run's own start records no first offset; give it
with --run-start-offset=PARTITION=OFFSET (or ALL=OFFSET), the operator's
knowledge of where the run began, which the artifact records as such.

Exits 0 with a complete artifact, 3 with one that says it is incomplete and
why, 2 on bad usage or a scope this cannot read.
"""

import argparse
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import payments_transform  # noqa: E402
from clink_reference import ReferenceWriter, Scope, ScopeError, read_scope  # noqa: E402


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
    implementation = {"name": payments_transform.NAME, "version": payments_transform.VERSION,
                      "config": payments_transform.RULES}
    writer = ReferenceWriter(args.out, scope, implementation,
                             reader={"name": "clink_reference.read_scope", "version": "1"})
    try:
        result = read_scope(scope, args.bootstrap,
                            lambda rec: [writer.add(row) for row in payments_transform.transform(rec.value)],
                            config=config, run_start_offsets=run_start,
                            deadline_s=args.deadline_s, max_records=args.max_records)
    except BaseException:
        writer.abandon()  # unfinished: clink refuses it
        raise
    writer.finish(result.complete, result.incomplete, result.verified, result.recorded, result.read)
    state = "complete" if result.complete else "incomplete: " + "; ".join(result.incomplete)
    print("reference: %d record(s) read, %d row(s) written in %.2f s, %s"
          % (result.records, writer.rows, result.seconds, state))
    return 0 if result.complete else 3


if __name__ == "__main__":
    sys.exit(main())
