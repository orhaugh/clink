# clink-audit

Audit a running clink job's completed checkpoints again and again, within bounds you set, and keep an account of what was verified, what was not and why.

Each audit is the independent verification workflow clink already has, run for one checkpoint's scope:

1. `clink incident-collect` takes the checkpoint's bundle while the job runs.
2. `clink incident-scope` writes the scope document, offline.
3. Your reference executable reads the scope's exact Kafka range and computes the rows the job owes.
4. `clink incident-verify` checks the bundle under network isolation, which the runner proves before it relies on it.
5. `clink incident-reconcile --reference` reads the ClickHouse destination, read-only, for the scope's keys, and compares the reference, the sink's captured input and the destination.

The runner adds the parts a person would otherwise do by hand: finding the checkpoints, collecting them before the Workers purge their state, bounding every step, keeping going after a crash, and recording coverage, gaps and discrepancies so that a later pass never hides an earlier failure. It is repeated, bounded auditing of the scopes it selects. It is not a continuous proof of every result.

## What it supports

Record-wise jobs from one Kafka SQL source into the ClickHouse native sink, under the rules destination reconciliation already has (a plain `MergeTree`, a key that identifies one row the job writes; see [incident verification](../../docs/internals/incident-verification.md#destination-reconciliation)). The destination is always read for the scope's keys: rows of other keys are not established, and the runner never asks for a whole-table verdict on a table that is still being written.

A windowed job is refused as unsupported. Its scope is its whole lineage, and auditing it repeatedly would read the job's whole history every time.

## Installing and running

```bash
pip install python/clink-audit          # the standard library only
clink-audit check-config --config=audit.json
clink-audit run --config=audit.json     # one cycle, for cron or another scheduler
clink-audit poll --config=audit.json    # cycles in the foreground until SIGINT or SIGTERM
clink-audit status --config=audit.json [--json]
clink-audit acknowledge --config=audit.json --audit=<id> --note="..."
clink-audit acknowledge --config=audit.json --checkpoints=12-15 --note="..."
```

Run it on the machine that runs the job: collection reads the job's checkpoint, HA and capture directories. The Workers must run with `--capture-dir`, as for any reconciliation.

| Exit | `run`, `poll`, `status` |
| --- | --- |
| 0 | healthy: every scope selected so far is verified |
| 1 | a discrepancy no one has acknowledged |
| 3 | incomplete (a gap, or an audit waiting to be tried again), behind, stale or stopped |
| 4 | another runner holds the state directory |
| 5 | no checkpoint to audit has completed yet |
| 2 | bad usage or configuration |

## The configuration

One JSON file, which you own and the runner trusts. Credentials are never in it: it names the environment variables that hold them.

```json
{
  "format": "clink-audit-config", "version": 1,
  "state_dir": "/var/lib/clink-audit/payments",
  "clink": "/usr/local/bin/clink",
  "job": {"id": 1, "checkpoint_dir": "/data/ckpt", "ha_dir": "/data/ha",
          "capture_dir": "/data/capture", "start_at": "latest"},
  "key": ["event_id"],
  "destination": {"host": "clickhouse.internal", "port": 9000, "user": "reconciler",
                  "password_env": "CH_RECONCILE_PASSWORD"},
  "reference": {
    "command": ["/opt/ref/venv/bin/python", "/opt/ref/reference_payments.py",
                "--scope={scope}", "--out={out}", "--bootstrap=kafka.internal:9092",
                "--kafka-secret=sasl.password=KAFKA_PASSWORD",
                "--max-records={max_records}", "--deadline-s={deadline_s}"],
    "env": ["KAFKA_PASSWORD"],
    "when_run_start": ["--run-start-offset=ALL=0"]
  },
  "verify": {"profile": "consistency"},
  "selection": "sequential",
  "limits": {"poll_interval_s": 10, "max_attempts": 3}
}
```

The reference command is an argument array, run without a shell. `{scope}`, `{out}`, `{max_records}`, `{max_bytes}` and `{deadline_s}` are the only placeholders, and anything else in braces is refused. `when_run_start` is appended only for a scope that starts at the run's own start, whose first offset the job's checkpoints do not record.

Each step receives the base environment (`PATH`, `HOME`, the locale, `TMPDIR`, `TZ`) and only the variables it needs: the reference gets `reference.env`, reconciliation gets `destination.password_env`, and collection, scope export and verification get none. The runner refuses a configuration that passes the destination's credential to the reference. Credential values that a child prints are replaced in the logs it keeps. Nothing a bundle, a scope, a reference artifact or the job's metadata names is ever run or connected to.

`start_at` is `latest` (the default: begin at the newest completed checkpoint, and record the earlier ones as a gap), `earliest`, or a checkpoint id.

### Limits

| Limit | Default | Bounds |
| --- | --- | --- |
| `run_seconds` | 900 | one `run`, or one cycle of `poll`; a step running at its end is stopped and resumes next time |
| `collect_timeout_s`, `scope_timeout_s`, `reference_timeout_s`, `verify_timeout_s`, `reconcile_timeout_s` | 120, 120, 600, 600, 600 | each child process, stopped with its process group |
| `bundle_bytes`, `bundle_files` | 1 GiB, 10,000 | `incident-collect --max-bytes`, `--max-files` |
| `reference_bytes` | 1 GiB | the artifact, watched while it is written, and `--max-reference-bytes` |
| `reference_rows`, `reference_records` | 10 M, 50 M | the artifact's rows and the source records it read (its end line); `{max_records}` passes the latter to the reference |
| `query_timeout_ms`, `max_rows_per_query`, `keys_per_query` | 30,000, 1 M, 10,000 | `incident-reconcile`'s own bounds |
| `disk_bytes`, `min_free_bytes` | 20 GiB, 1 GiB | the state directory, and the free space it leaves |
| `retain_passed`, `retain_passed_reports` | 3, 1,000 | passed audits whose evidence, and whose reports, are kept |
| `max_pending` | 100 | collected bundles waiting for their audit |
| `max_attempts`, `retry_backoff_s` | 3, [10, 60, 300] | attempts for a partial or failed audit, and the pause before each; an audit has at most one attempt per cycle |
| `collect_retries` | 10 | collections refused because the job moved on during the copy |
| `poll_interval_s` | 10 | the pause between `poll` cycles |
| `stale_after_s`, `backlog_warn` | 900, 10 | when the status says stale or behind |

A bound that is reached makes the audit partial or an error, never a pass.

## Selection and coverage

`sequential` (the default) audits every completed checkpoint in order. `latest-only` audits the newest and records each checkpoint it passed over as skipped, by id. Neither samples records or keys.

A cycle audits what was eligible when it began, each audit at most once: a retry waits for a later cycle. A checkpoint that completes while it runs is collected at once and audited by the next cycle. Collection cannot wait: a scope's bundle needs the snapshots of the checkpoints that bound it, and a Worker keeps only its newest `--checkpoint-num-retained` (default 2). The runner looks for new checkpoints between every step. A checkpoint whose state was purged before it was collected is a gap, recorded as expired, with the reason.

Coverage comes only from published results that passed or were empty, per log (topic, cluster id, topic id) and partition, as merged offset ranges. An empty scope is one whose source range held no record the job turns into a row: the reference computed none from the complete range, and the sink received none. A later pass never removes a gap or a discrepancy. They stay until you acknowledge them, and stay listed after, with your note.

## The state directory

| Path | What |
| --- | --- |
| `lock` | held by the one runner using the directory (`flock`; a crash releases it) |
| `state.json` | progress: audits under way and their completed steps, coverage, gaps, discrepancies |
| `status.json` | the last status, rewritten after each cycle and during one |
| `events.ndjson` | one JSON object per event (`result`, `retry`, `orphan-stopped`, `config-changed`, `job-changed`, `stopped`...), rotated at 32 MiB |
| `inflight.json` | the child process running now, so a restarted runner can stop an orphan |
| `acks/` | your acknowledgements |
| `audits/<id>/` | one audit's evidence: the bundle, scope, reference, reports, each step's output and the commands run |
| `reports/<id>.json` | one audit's result (`clink-audit-report` version 1), published by rename |

An audit's id is the checkpoint and a hash of the job and the configuration's fingerprint, so a restart publishes the same result under the same id, and a changed reference, key, destination or verdict-changing bound gives new ids. Each step's result is recorded once its output is in place. After a crash the runner stops the child it left running, publishes any audit whose report was written but not recorded, and resumes the rest at the step that was running. A discrepancy is never retried away, and unfinished work is never counted as verified.

Retention deletes only what the runner owns: an audit directory directly under `audits/`, not a link, holding the marker that names its audit, and only for audits that passed or were empty, beyond `retain_passed`. Evidence of audits that did not pass is kept. When keeping it would pass `disk_bytes` or `min_free_bytes`, the runner stops starting audits and says so (`stopped`), rather than delete it.

Retained bundles hold the job's records, its state and the reference's rows. Treat the state directory as you treat the job's own data.

## Sizing

One audit costs a collection, a reference read of the scope's source range, an offline verification and two reads of the destination a second apart. In the example's steady run on an Apple M2 Pro, with checkpoints two seconds apart, a median audit took 2.3 s of wall time (95th percentile 3.1 s), 1.3 s of it the reconciliation, and 0.9 s of CPU in its child processes. An empty scope costs about the same as a small one, so the checkpoint interval sizes the runner, not the data rate. Sequential coverage keeps up when the interval is longer than an audit. When it is shorter, the backlog grows and the status says so: lengthen the interval, raise the Workers' `--checkpoint-num-retained` so that collection, which must happen before a checkpoint's successors purge what its scope reads, has room, or use `latest-only` and accept the recorded gaps.

`examples/audit-runner/` qualifies the runner against Kafka and ClickHouse and measures its cost.
