# Auditing windowed results

A runnable example of the independent audit of a windowed job: Kafka into
ten-second tumbling windows per account, written to ClickHouse, audited
against a reference computed outside the job from the job's whole lineage.
It runs on a local Coordinator and two Workers from the build, against the
destination-reconciliation example's Kafka and ClickHouse on compose, and
reuses that example's harness.

```bash
examples/window-audit/run.sh                         # every scenario: a qualification
ONLY=windows-main examples/window-audit/run.sh       # one scenario
```

It needs Docker, Python 3.10 or later with pip, and `clink_node` and `clink`
built with tests (`CLINK_BIN_DIR`, default `build/`), since the recovery
scenarios drive fault points. Everything it produces is under `WORK`, which
is the qualification bundle (see `run.sh`).

## The job

`job.sql.in`: payments from a three-partition topic, read by two source
subtasks: one owns partitions 0 and 2, the other partition 1. A subtask's
watermark is the least over the partitions it owns, each held until it
delivers. Event time is `ts`, epoch milliseconds, and the
watermark trails the least partition maximum by two seconds. Per account and
ten-second window, a row counts the purchases and refunds, sums their signed
amounts exactly (`DECIMAL(38, 2)`) and their units. Chargebacks are filtered
out, but still advance the watermark. A record for a window that has closed
is dropped. The sink is the ClickHouse native sink into a plain `MergeTree`,
at least once.

## The reference

`reference_windows.py` reads the job's lineage from Kafka with
`python/clink-reference`, on the connection it is given and never one the
scope names. `windows_transform.py` holds the specification:

- the window rules, declared (`WindowRules`) and not read from the job;
- what each record contributes;
- the row a window becomes.

`TumblingWindows` folds the records into windows. For each record it decides
from the source and the recorded watermarks whether it arrived before its
window closed; when that cannot be decided, it leaves the window out with
why. It writes a version 2 artifact: the final windows' rows, the windows
left out, the source's watermark bound at every checkpoint, and an account of
every record. Given an increment's scope (`incident-scope --since`), it starts
from its own state at the base (`--state-in`) and reads only the increment's
range; `--state-out` writes its state at the scope's checkpoint for the next
one. `examples/audit-runner/` audits this job that way while it runs.

Each scenario also checks the reference against a second derivation. The
workload generates every event with the fate the scenario designs for it:
folded, dropped because its window had closed, or filtered.
`window_workload.design_windows` computes the final windows from those fates.
The events are read back with Kafka's own console consumer, each found in the
partition it was sent to, and the two derivations must agree. Kafka's console
producer picks a partition by hashing the record's key, so the workload uses
keys whose murmur2 hash lands on each partition.

## The main timeline

Phases, each produced once the job has done what the phase needs. The driver
watches the watermark the sink records at each completed checkpoint, so a
record's fate is decided by evidence, not by how the job happened to
interleave its partitions:

1. Every partition runs steadily to E = 30 s, then each sends one event at E
   plus the lag. The watermark stops exactly at E: the window ending at E
   closes, and the one starting at E stays open. Some events are out of order
   by up to the lag less a millisecond. Window boundaries are hit exactly, an
   event at a window's first millisecond and one at its last.
2. Events for the window that closed, one at its last millisecond, are
   dropped. One at E itself is folded into the next window.
3. Partition 2 goes quiet while 0 and 1 run 28 seconds ahead. With no
   idleness, it holds the watermark at E and no window closes, though the
   subtask that owns it reads partition 0 as well. An event on
   partition 0 far behind its own partition is still folded, its window held
   open.
4. Partition 2 catches up, all three run on to E + 50 s, and the windows up
   to E + 40 s close. Then nothing more is sent, and the run goes quiet.

## Scenarios

| Scenario | What happens | Required |
| --- | --- | --- |
| `windows-main` | The main timeline, audited at the checkpoint the watermark stopped exactly at E (selected keys), in the quiet stretch (selected keys) and at the end (the table read whole). A second bundle is collected without the run's first intervals. | Every check passes at all three; the recorded watermark is exactly E; the window starting at E is open; nothing past E is final while partition 2 is quiet; the two late events are dropped and none is undetermined; the references match the design. Without the first intervals, `incident-scope` and the audit are partial (exit 3). |
| `windows-unexpected` | The main run's table with a window neither side names. | Read whole, `destination.coverage` fails with one unexpected row and everything else passes; on selected keys, a pass that says the rest of the table was not read. |
| `windows-missing-history` | Records the main run's windows depend on are deleted from the topic. | The reference says it is incomplete (exit 3), and the audit is partial with `reference.scope` unavailable. |
| `windows-recovery` | worker-1 runs the job alone and is killed in the quiet stretch, its windows half accumulated; the job restores on worker-2. | The lineage passes the restore, and the audit read whole passes: every final window exact, once. |
| `windows-crash-after-output` | worker-1 dies just after an INSERT of window rows is acknowledged and before its checkpoint completes (`clickhouse.after_end_insert`); the restored run writes those windows again. | Exit 1 with the transformation passing and duplicates only in the delivery, counted in `reconcile.rows` and `reference.destination`, put down to the restore. |
| `windows-spare-subtask` | Four source subtasks over three partitions: one owns none. | It marks its output idle, so the windows close on the other three's watermark; the audit, reading the table whole, passes. |
| `windows-empty` | Only chargebacks: no window holds a payment. | The reference has no rows, and the audit, reading the table whole, passes on an empty table. |
| `windows-wrong-time-column` | The job windows on when the device sent the event (`sent_ts`), not when it happened. | Reconciliation alone passes; the audit fails `reference.semantics` and `reference.capture`, naming the transformation, with the delivery passing. |
| `windows-wrong-sign` | The job adds refunds to the net amount. | Reconciliation alone passes; the audit fails `reference.capture`. |
| `windows-late-treatment` | The job holds windows open five seconds past their end (`allowed_lateness_ms`), folding in events the specification drops. | Reconciliation alone passes; the audit fails `reference.semantics` and `reference.capture`. |
| `windows-unsupported` | A source with `idle_timeout_ms`, which drops a quiet partition from the watermark after wall-clock time. | `incident-scope` refuses it by name (exit 3); the delivery is still checked and passes. |
| `windows-cost` | 60,000 events over ten minutes of event time, with the flight recorder off and on, three times each. | The recorder's cost and the audit's (scope, reference, audit with the whole table) are measured; the measured audit passes. |

## What it shows, and what it does not

A passing audit here establishes that the job's window rules are the
reference's. The watermark the sink recorded was never ahead of what the
source allowed, and no window closed early. Every window final at the
audited checkpoint holds exactly the reference's value, copies counted, in
the sink's input and in the table. Windows still open at the checkpoint are
not established, and neither is anything after it. The audit is as
independent as the reference: here a second derivation, from the scenario's
own design, checks the reference too.
