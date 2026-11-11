# Auditing a running job repeatedly

A runnable example of `clink-audit` (`python/clink-audit`), the runner that audits a running job's completed checkpoints one after another, within bounds, and keeps an account of what was verified. It runs the destination-reconciliation example's record-wise payments job (Kafka -> clink -> ClickHouse) on a local Coordinator and two Workers from the build, keeps producing to it, and runs the runner against it the ways an operator would: polling, as a scheduler's one-shot run, killed and started again.

```bash
examples/audit-runner/run.sh                         # every scenario: a qualification
ONLY=runner-steady examples/audit-runner/run.sh      # one scenario
```

It needs Docker, Python 3.10 or later with pip, and `clink_node` and `clink` built (`CLINK_BIN_DIR`, default `build/`). It reuses `../destination-reconciliation/`: its compose file, job, harness and reference (`reference_payments.py`). Offline verification runs under the platform's network isolation, which the runner proves before relying on it. Everything a run produces is under `WORK`, which is the qualification bundle. Each scenario's directory holds the cluster's checkpoints and captures, and the runner's configuration and state directory: its reports, status, events and retained evidence.

## The runner's configuration

`audit_demo.py` writes one per scenario. The reference is `reference_payments.py`, run by the virtual environment's Python with `--scope={scope} --out={out}`, the broker's address, and the runner's bounds as `--max-records={max_records} --deadline-s={deadline_s}`. The job reads a topic created for it from its first offset, so a scope that starts at the run's start gets `--run-start-offset=ALL=0`. The destination's password is named (`password_env`), never written, and only the reconciliation receives it.

## Scenarios

| Scenario | What happens | Required |
| --- | --- | --- |
| `runner-steady` | The job runs with checkpoints every two seconds and Workers that keep ten; 9,000 events are produced at 200 a second while `clink-audit poll` runs; then the job stops and the runner finishes. | Every completed checkpoint audited exactly once, each passed or empty; the status healthy (exit 0) with no gap. An independent check reads the verified range back from Kafka with its console consumer and the destination with the administrator's client: one verified range per partition from the run's start to the last audited offset, and the rows owed equal the rows the audits compared and the rows in ClickHouse, each once. The references read each source record once. At most `retain_passed` audit directories kept. |
| `runner-defect` | The job computes `amount_cents` at the wrong scale and writes it faithfully. | Every scope with rows a discrepancy whose findings are the transformation and the reference-destination relationship, with the delivery passing; status discrepancy, exit 1. |
| `runner-corruption` | A sound job; after a first run passes, one row is changed in ClickHouse after it was written and before its checkpoint is audited; more is produced and audited. | Exactly one discrepancy, naming that row, in the delivery and not the transformation; the later scopes pass, and the status stays at discrepancy (exit 1) with the last verification after it; its evidence kept. Acknowledged, the status is healthy and the discrepancy stays listed. |
| `runner-crash` | While the job keeps producing, the polling runner is killed (SIGKILL) inside a reference, and again inside a reconciliation, and started again each time. | One report per completed checkpoint, no id twice; each killed audit unpublished at the kill and published once after; the child it left running stopped; status healthy with no gap; the independent check agrees. |
| `runner-outage` | ClickHouse is paused after checkpoints complete and before their audits read it, then resumed. | Nothing published while it is down, and the status incomplete with audits waiting to retry; after it, every scope verified, each retried report keeping its first attempt as partial with the destination unreadable, and no pass observed during the pause. |
| `runner-budget` | Rows per destination query (5), source records a reference may read (10), and a reconciliation's time (0.5 s) set below what the scopes need. | Every scope with rows partial or an error that names the bound, never a pass; status incomplete, exit 3. |
| `runner-latest-only` | Several checkpoints wait; the runner runs once with `latest-only`. | The newest verified; every older one published as skipped and listed as one gap; coverage only the audited scope's range. |
| `runner-expired` | Workers that keep two checkpoints a second apart; the runner starts after ten have completed. | Those purged before the runner began are gaps, expired, naming the purge; later ones verified; status incomplete, exit 3. |
| `runner-history-expired` | The source records of the first scopes are deleted from the topic before the runner reads them. | Those scopes partial, naming the expired or deleted offsets; the later ones verified; exit 3. |
| `runner-cost` | 30,000 events, three times each: the flight recorder off, on, and on with the runner polling throughout. | Every run lands every row; the time to land, the Workers' CPU and the runner's CPU are measured. |

## What it shows, and what it does not

A passing scope here establishes what the audit establishes for one checkpoint: the sink received exactly the reference's rows, and the destination holds them, for the scope's keys. What the runner adds is the account: which scopes were verified, which were not and why, and that a discrepancy or gap stays visible until someone acknowledges it. A scope that was skipped, expired, partial or an error is not verified, and the status says so. Rows of keys outside every scope are not established, and nothing is established after the last audited checkpoint.

The runner keeps up when one audit takes less time than a checkpoint interval, and loses nothing to the Workers' retention while each checkpoint is collected before the next ones purge the state its scope reads. `runner-steady` measures both; `runner-expired` shows what happens when they do not hold.
