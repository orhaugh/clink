# clink-reference

Compute an independent reference result for one checkpoint of a clink job that reads Kafka and writes ClickHouse, for `clink incident-reconcile --reference` to audit the job's output and its destination against.

Destination reconciliation compares the destination with the sink's own record of its input. That shows whether the writes arrived, but a transformation that computes the wrong rows writes them faithfully, and the comparison passes. A reference computed outside the job, from the same source records, is what tells. This library does the parts that are the same for every reference: it reads exactly the scope's Kafka ranges, confirms they are the logs the job read, and writes the artifact. The transformation is yours.

## The workflow

```bash
clink incident-collect --checkpoint-dir=... --job=1 --capture-dir=... \
    --include-state --include-captures --out=bundle
clink incident-scope bundle --checkpoint=N --out=scope.json      # offline
python3 my_reference.py --scope=scope.json --bootstrap=HOST:PORT --out=reference.ndjson
clink incident-reconcile bundle --checkpoint=N --key=event_id --reference=reference.ndjson \
    --host=... --user=reconciler --password-env=CH_PASSWORD
```

`incident-scope` reads the bundle alone. The reference reads Kafka, on a connection you give it. `incident-reconcile` reads the destination. Each is a separate, explicit step, and nothing in a scope or an artifact names a command to run or a connection to make.

## Writing a reference

```python
from clink_reference import ReferenceWriter, Scope, read_scope

def transform(value):            # one record's value in, the rows the job should write out
    ...
    return [{"event_id": 7, "amount": Decimal("12.50"), ...}]

scope = Scope.load("scope.json")
writer = ReferenceWriter("reference.ndjson", scope, {"name": "my-reference", "version": "3"})
result = read_scope(scope, "broker:9092",
                    lambda rec: [writer.add(row) for row in transform(rec.value)],
                    config={"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
                            "sasl.username": "...", "sasl.password": os.environ["KAFKA_PASSWORD"]})
writer.finish(result.complete, result.incomplete, result.verified, result.recorded, result.read)
```

A row names every column of the scope's output, as Python values: `int`, `float`, `bool`, `str`, `Decimal` (or text) for decimals, `datetime` or whole ticks for times, `date` or whole days for dates, and `None`. Each is encoded to the canonical cell its column's type gives it; a value the column cannot hold exactly raises, as it is the reference's error. Copies are kept: a transformation that yields a row twice writes it twice.

`examples/destination-reconciliation/reference_payments.py` is a complete reference for that example's job, with its transformation in `payments_transform.py`.

Write the transformation from the job's specification, not its code. The audit establishes that the artifact is whole and bound to the scope; it cannot establish that the reference is independent of the job, and a reference that shares the job's logic shares its mistakes.

## A windowed job

A job of one keyed tumbling window has a version 2 scope: its ranges start at the job's own start, and it carries the window's rules as the job declares them, each completed checkpoint's source positions and the watermark the sink recorded there, and the watermark windows are final by. A window's row depends on every record before it, and on whether each arrived before its window closed, which offsets do not record. `TumblingWindows` decides that from bounds: the record's own partition and the next checkpoint's positions bound the watermark from above, from the records alone, and the watermark the sink recorded at the checkpoint before bounds it from below. A record or window the bounds cannot place is left out, with why, never guessed.

```python
from clink_reference import ReferenceWriter, Scope, TumblingWindows, WindowRules, read_scope, write_result

rules = WindowRules(size_ms=10_000, lag_ms=2_000, time_column="ts", allowed_lateness_ms=0)
windows = TumblingWindows(scope, rules, [("events", "count"), ("net", "sum")])
read = read_scope(scope, "broker:9092",
                  lambda rec: windows.add(rec.partition, rec.offset, event_time(rec.value), select(rec.value)),
                  run_start_offsets={("payments", p): 0 for p in range(3)})
result = windows.finish()
writer = ReferenceWriter("reference.ndjson", scope, {"name": "my-windows", "version": "1"},
                         semantics=rules.semantics())
write_result(writer, result, output)  # output(key, start, end, aggregates) -> the row
writer.finish(read.complete, read.incomplete, read.verified, read.recorded, read.read,
              source_bounds=result.source_bounds, records=result.records)
```

The rules are your specification, declared, not read from the job: the audit compares them with the job graph's, and a job configured otherwise fails `reference.semantics`. `event_time` gives every record's event time, the ones the transformation filters out included, since every record the source delivers advances the watermark. `select` gives the record's key and its values for each `SUM`, or `None` for a record the transformation drops. `examples/window-audit/reference_windows.py` is a complete reference for that example's job, with its specification in `windows_transform.py`.

## What a read establishes

For each source, before reading:

- the cluster's id and the topic's id are the ones the job's checkpoints recorded, so a topic deleted and created again under its name, or another cluster, is not read in its place;
- the topic's `cleanup.policy` does not compact, since compaction can remove records from a range without a trace in its offsets.

Each range `[from, to)` is read at the scope's isolation level. Under `read_committed`, transaction markers and the records of aborted transactions hold offsets that are never delivered, so a range's record count is not `to - from`, and a poll that returns nothing proves nothing. A range is complete only once the consumer has passed its end: a record at or after `to` was delivered, or the end of the log was reached at or after `to`.

The read is incomplete, with the reason, when:

- a range's start has expired from the log;
- the log now ends before the range's end;
- the cluster or topic id differs, or the cluster reports no topic id where the job recorded one;
- the topic compacts, or its policy cannot be read;
- an error comes before the end;
- the deadline or the record or byte bound is reached first.

Records already delivered are never made into a complete result. A range that starts at the run's own start has no recorded first offset; give it with `run_start_offsets`, which the artifact records as the operator's.

The artifact keeps what was verified apart from what is only recorded: the implementation's name, version and configuration, the client library, and any offsets the operator supplied.

The client is confluent-kafka, over librdkafka. That is a different binding from the job's but the same underlying client library, so a defect in that library's fetch path could affect both. The example's independent check reads its ranges with Kafka's own Java console consumer for that reason.

## The artifact

NDJSON, format `clink-reference-result`, version 1:

1. **A header**, with the scope document verbatim and the implementation.
2. **One line per row** of canonical cells, with copies kept.
3. **An end line** holding:
   - the row count;
   - SHA-256 hashes of the row lines and the header line;
   - `complete` and, if not, why;
   - what was verified, what was recorded, and each range's read.

A windowed scope's artifact is version 2. Its header adds `semantics`, the window rules the reference implements. Its body adds a `window` line for each window left out, open or undetermined, with why. Its end line adds `windows` and `windows_sha256`, `source_bounds` (the watermark the records allow at each checkpoint of the lineage) and `records` (what became of every record: folded, dropped late, undetermined or filtered).

An artifact without its end line was not finished, and clink refuses it. So does one whose hashes or count do not hold, whose scope is not the audit's, or whose cells are not canonical for their column. The hashes show the file is whole as its writer finished it, not who wrote it.

## Tests

```bash
python3 -m unittest discover -s python/clink-reference/tests   # offline; the live suite skips
scripts/reference-live.sh                                     # with Apache Kafka in Docker
```

The live suite reads ranges holding committed and aborted transactions, expired history, a topic created again, a compacting topic and a bound against `apache/kafka:3.9.1`.
