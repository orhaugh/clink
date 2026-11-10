# Destination reconciliation: Kafka to ClickHouse

A record-wise SQL job reads payments from Kafka, filters and projects them, and writes them to ClickHouse through the native sink. For a completed checkpoint, `clink incident-reconcile` reads ClickHouse and says whether it holds exactly the rows the job's sink received in that checkpoint's intervals. An independent check confirms the topic is still the log the job read, reads the checkpoint's source range back from it, derives the rows the job owes, and must agree with every verdict.

It also runs the independent reference audit. `reference_payments.py` computes what the job owes from the same Kafka records, through `python/clink-reference`, with its own transformation (`payments_transform.py`). `clink incident-reconcile --reference` then compares the job's captured output, the destination and that reference, relationship by relationship. Jobs with a deliberate defect show that a wrong transformation written faithfully passes reconciliation and fails the audit.

`run.sh` runs every scenario below and keeps what it produced as a qualification bundle. The workflow is described in [incident verification](../../docs/internals/incident-verification.md#destination-reconciliation) and the decisions behind it in [design record 015](../../docs/design/015-destination-reconciliation.md).

```bash
examples/destination-reconciliation/run.sh                    # every scenario, about 15 minutes
ONLY=healthy,corruption examples/destination-reconciliation/run.sh
KEEP=1 WORK=/tmp/recon examples/destination-reconciliation/run.sh
```

It needs Docker, Python 3.10+ with pip (it installs `python/clink-reference` and its Kafka client, confluent-kafka pinned at 2.16.1, into a virtual environment under `WORK`), and `clink_node` and `clink` built with tests in `CLINK_BIN_DIR` (default `build/`): the crash scenarios drive fault points, which only a test build compiles in. Kafka (`apache/kafka:3.9.1`) and ClickHouse (26.8, pinned by digest) run from `docker-compose.yml` on 127.0.0.1 only, in a compose project of their own that the run removes, volumes included. Everything it corrupts on purpose is in that disposable ClickHouse.

## The job

`job.sql.in` declares the supported shape: a Kafka JSON table, a record-wise `SELECT` (a filter that keeps about 65% of events, and a projection that derives a decimal, an integer, a double and a boolean), and a native-sink table into a plain `MergeTree` keyed by `event_id`:

```sql
INSERT INTO payments
SELECT event_id, account, kind, amount,
       CAST(amount * 100 AS BIGINT) AS amount_cents,
       CAST(CAST(amount * 100 AS BIGINT) AS DOUBLE PRECISION) * 0.029 AS fee,
       CASE WHEN amount >= 1000 THEN TRUE ELSE FALSE END AS flagged,
       note, ts AS occurred_at
FROM events
WHERE kind IN ('purchase', 'refund') AND amount >= 1.00;
```

The events are a pure function of their id (`workload.py`), with notes that include NULL, the empty string, quotes, backslashes, control characters and four-byte UTF-8. `reference_row()` writes the transformation again in Python, from an event's own fields.

Each scenario runs a Coordinator and two Workers from the build with the flight recorder armed (`--capture-dir`), checkpoints every second at parallelism 2, and collects with `--include-state --include-captures`. The job writes as `clink`, a ClickHouse user with `INSERT` and `SELECT` on the example's database and the system tables the native sink checks. The reconciliation reads as `reconciler`, which has `SELECT` on the example's tables and on `system.tables`, `system.columns` and `system.mutations`, and nothing else.

## The independent check

For every reconciliation whose result is satisfied or failed, `reconcile_demo.py`:

1. takes the scope's source range from the report: for each partition, the offsets between the Kafka positions in the snapshots of the checkpoints that bound the scope;
2. requires the log the source recorded beside them (the topic, the cluster's id and the topic's id) to be the one Kafka's own tools report now, since a topic deleted and created again holds other records at the same offsets;
3. reads exactly those offsets back from Kafka with the console consumer, and derives the rows the job owes with `reference_row()`;
4. requires them to equal the rows `incident-reconcile` expected (its `--expected-out` export), cell for cell;
5. reads the destination for the same keys with its own client and query, and counts missing, changed, unexpected and duplicated rows;
6. requires those counts to equal the reconciliation's.

Step 4 shows that the expectation, which comes from the sink's own record of its input, is the transformation of the source range and not something derived from the destination. Step 6 shows the verdict matches a second reading of the destination.

## The reference audit

For a scope, the example runs what an operator would:

```bash
clink incident-scope BUNDLE --checkpoint=N [--from-run-start] --out=scope.json
python3 reference_payments.py --scope=scope.json --bootstrap=127.0.0.1:PORT --out=reference.ndjson \
    [--run-start-offset=ALL=0]
clink incident-reconcile BUNDLE --checkpoint=N --key=event_id --reference=reference.ndjson \
    [--coverage=complete-table] --host=127.0.0.1 --user=reconciler --password-env=...
```

`reference_payments.py` reads exactly the scope's ranges after confirming the cluster and topic are the ones the job read, applies `payments_transform.transform` (the job's rules, written from its specification with Python's `json` and `decimal`), and writes the artifact. A scope from the run's start records no first offset; the example's topics are new, so it gives 0 as the operator's knowledge.

The bounded runs each write into a table of their own:

1. their events are in a topic of their own before the job starts;
2. the job reads them all, and three more checkpoints complete with nothing in them;
3. the cluster stops, and the table's parts are merged.

They are audited at the third-last checkpoint with `--coverage=complete-table`, so the bundle shows two empty intervals after it.

For each faulty job, the scenario also checks the audit from the source. It reads the scope's records back with Kafka's own console consumer (the Java client, not the reference's librdkafka). It then requires the artifact's rows to be `workload.reference_row()` of them, and `reference.capture`'s counts to be the ones the defect gives those records.

## Scenarios

| Scenario | What happens | Required |
| --- | --- | --- |
| `healthy` | The job runs and keeps writing while a bundle is collected and checkpoint N is reconciled; `incident-verify --profile=replay` runs on the bundle with the network denied; the scope is audited against the reference on selected keys, and a whole-table read is asked for | exit 0; the independent check agrees; a later checkpoint completed during the read; offline verification never fails; the audit exit 0 on selected keys; the whole-table read unavailable (exit 3), as the job is still writing |
| `identity` | `--job=2`; a checkpoint the bundle does not hold as completed; `--table` naming another table; the table dropped and recreated under its name | exit 2 each time (refused); exit 0 once the original table is back |
| `missing-evidence` | A bundle collected without `--include-captures` | exit 2: no sink interval evidence |
| `limits` | `--max-rows-per-query=1`; a server that stops answering (a proxy forwards nothing) with `--query-timeout-ms=1000` | exit 3 each time |
| `corruption` | On the disposable table, one after another: two amounts changed so the scope's count and total stay equal; a row written twice; an extra row for a key of the scope; a row deleted; a float changed in its last bits | exit 1 with exactly the cumulative missing, changed, unexpected and duplicated counts; the independent check agrees each time, and confirms the swap left count and total unchanged |
| `expired-history` | Kafka deletes the first half of the scope's range on one partition | the independent check is unavailable; the reconciliation, which reads only the bundle and the destination, still gives its verdict; the reference exits 3 (the offsets are no longer in the topic), and the audit holds `reference.scope` unavailable |
| `recreated-topic` | worker-1 runs the job alone and is killed; the topic is deleted and created again under its name, and new events written to it; worker-2 starts, and the job restores there | the checkpoint before: exit 0, and still exit 0 after, as the reconciliation reads no source; the independent check is unavailable, as the topic is another log; the reference exits 3 (deleted and created again) and the audit 3; the restored job refuses the old topic's positions, naming both topic ids, and none of the new events reaches the table |
| `audit-healthy` | A bounded run of 3,000 events into a dedicated table; the reference over the whole run; the audit with the table read whole, then on selected keys | the reference complete; exit 0 with every check passed, `destination.coverage` among them; the artifact's rows equal the independent computation; on selected keys exit 0, saying rows of other keys were not read |
| `audit-destination` | On that table: a row of a key neither the reference nor the job names; then one value changed and one row written twice | the unrelated key: read whole, exit 1 with `destination.coverage` failed and the rest passed; on selected keys, exit 0 saying the rest of the table is not established. The changed value and the duplicate: exit 1, the delivery failed (1 changed, 1 duplicated) and the transformation passed |
| `audit-refusals` | A reference computed for an earlier checkpoint; one without its end line; one whose read reached a bound; a whole read past `--max-table-rows` | exit 2, naming the checkpoint; exit 2; the reference exits 3 and the audit exits 3 with `reference.scope` unavailable; exit 3 with `destination.coverage` unavailable |
| `wrong-projection` | The job computes `amount_cents` at the wrong scale and writes it faithfully | reconciliation alone exit 0; the audit exit 1, `reference.capture` failed and `reconcile.rows` passed, the transformation named first; every owed row changed, as the source shows |
| `faulty-filter` | The job's threshold is 10.00, not 1.00, so it drops valid payments | the audit exit 1: the dropped rows missing from the job's output and the destination, as many as the source shows |
| `extra-output` | The job lets through chargebacks | the audit exit 1: those rows unexpected, as many as the source shows |
| `equal-totals` | The job writes the account in capitals: every row and amount there | the destination's count and total equal the reference's; the audit exit 1, every row changed |
| `empty-output` | A bounded run of 1,500 events the filter drops, so the expected output is genuinely empty | the reference complete and empty; on selected keys exit 3 (nothing selected, not established); read whole exit 0; a row added to the table: exit 1 |
| `truncated` | The job records at most five rows per interval (`--capture-records=5`) | exit 3: the scope is unavailable |
| `crash-after-publish`, `crash-before-publish` | worker-1 runs the job alone and exits at its 40th snapshot publish (after it, or before it); the job restores on worker-2 | the checkpoint before the crash: exit 0. The restored run's first: exit 0 or 1, whichever the destination shows, with duplicates put down to the restore. A later one: exit 0. The independent check agrees with all three |
| `crash-after-ack` | worker-1 exits right after ClickHouse acknowledged its 60th INSERT, before the sink took the acknowledgement | as above |
| `retry-dedup-log` | A proxy forwards an INSERT's rows and withholds ClickHouse's reply, then resets the connection; the sink resends in doubt under its own token. The table keeps a deduplication log (`non_replicated_deduplication_window = 1000`) | exit 0; the ledger names the resend, and the report says none is duplicated |
| `retry-no-log` | The same, into a table without a log (`non_replicated_deduplication_window = 0`) | exit 1: exactly the resent INSERT's rows duplicated, put down to the resend; the independent check agrees |
| `overhead` | 40,000 events from a pre-filled topic, once with the flight recorder off and once on | both runs land every row; the largest scope reconciles; the figures are recorded |

A restored run's first checkpoint shows the at-least-once contract at work: rows the discarded run had written after the restore point are written again. Whether there are any depends on where the crash fell, so the scenario requires the verdict to match the destination, not to be a pass.

## What a run leaves

`WORK` (a new temporary directory unless given) is the qualification bundle:

| Path | What |
| --- | --- |
| `results/summary.txt` | every step and check, and `QUALIFIED` only when every scenario ran and every check passed |
| `results/scenarios.json` | each check: scenario, what was required, what was observed, passed |
| `results/commands.json` | every `clink` and `clink_node` command, with the fault points armed |
| `results/build.txt`, `results/versions.json` | the commit, the binaries' capabilities, the ClickHouse and Kafka versions and images |
| `results/docker-compose.yml`, `results/job.sql.in` | the configuration the run used |
| `results/reports/` | every reconciliation's JSON and text report, and the offline verification's |
| `results/expected/` | each reconciliation's expected rows, as canonical cells |
| `results/independent/` | each independent check's result |
| `results/references/` | each scope document, reference artifact and the reference's output |
| `results/measurements.json` | the overhead and reconciliation cost |
| `bundles/` | every incident bundle |
| `<scenario>/` | the job, its checkpoints, captures and evidence, and the Coordinator's and Workers' logs |

## Measured

The `overhead` scenario on an Apple M2 Pro with 32 GB, Docker Desktop, sharing the machine with other work: 40,000 events from a pre-filled topic, 25,758 rows owed, parallelism 2, checkpoints every second. Each mode ran five times, alternating. The Workers' CPU and what the recorder wrote are taken once a checkpoint has completed after the last row landed, since the recorder writes an epoch when the next barrier passes. Medians:

| What | Recorder off | Recorder on |
| --- | --- | --- |
| Time from submission until every row was in ClickHouse | 1.13 s | 1.03 s |
| Both Workers' CPU time | 0.47 s (0.45 to 0.49) | 0.72 s (0.69 to 0.74), +53% |
| What the recorder wrote | nothing | 21.5 MB in 22 files |

At this size the time to land is start-up, and the recorder did not move it.

Reconciling the largest scope of the last run (one checkpoint covering all 25,758 rows) took 1.87 s: 52 bounded queries, two reads of 26 chunks of up to 1,000 keys, against a table sorted by the key. Its bundle held 21.5 MB in 34 files.

Auditing that last run whole against the reference, at its third-last checkpoint (one scope from the run's start, all 25,758 rows of 40,000 events), took:

| Step | Time | What it did |
| --- | --- | --- |
| `incident-scope` | 0.32 s | exported the scope from the bundle |
| `reference_payments.py` | 1.25 s | read the 40,000 records back from Kafka and wrote a 6.2 MB artifact |
| `incident-reconcile --reference --coverage=complete-table` | 2.11 s | the three comparisons and a whole-table read of 25,758 rows |

The reference reads every record of the scope again, so its cost grows with the source range as well as with the rows owed. A whole-table read grows with the table, which is why it applies only to a table dedicated to one bounded run.

These are one run's figures on one machine, for this job, not a benchmark. The recorder captures every operator's input and the sink's, so its cost grows with the job's operators and rows. Arm it for the jobs whose destination you will reconcile, and size `--capture-records` to the rows a checkpoint interval carries.
