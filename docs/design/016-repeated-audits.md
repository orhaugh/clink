# 016: Repeated, bounded audits of a running job

**Status:** accepted, 2026-11-11.

## The problem

Record 015 made one checkpoint's audit possible: collect a bundle, export its scope, compute a reference outside the job, verify offline and reconcile the destination. Each step is a command an operator runs by hand, for a checkpoint they chose. That answers a question about one moment. It does not tell an operator, week after week, which of a running job's checkpoints have been checked, which could not be, and whether something found last Tuesday is still unresolved.

Running the same commands from a script on a timer looks like an answer and is not one. A script that audits the newest checkpoint and reports success says nothing about the checkpoints between its runs. One that crashes mid-audit either loses the audit or reports it twice. A dependency outage looks like a failure or, worse, like nothing. And the Workers purge a checkpoint's state a few checkpoints later, so a script that falls behind silently loses the ability to audit what it skipped.

## The decision

A runner, `clink-audit` (`python/clink-audit`), orchestrates the existing checks and keeps an account of their coverage. It adds no check and decides no verdict the commands do not already give. What it adds is selection, bounds, persistence and an honest status:

- **One logical result per scope.** An audit's identity is its checkpoint, the job, and a fingerprint of everything that changes its result: the key, the destination, the reference command, the verification profile and the bounds that change a verdict. A restart publishes the same result under the same identity; a changed reference starts new identities.
- **Coverage only from results.** A scope counts as verified only when its published result passed, or was empty: its complete source range held no record the job turns into a row, which the reference and the sink's captured input both say. Everything else is a gap, by checkpoint and reason. A later pass never clears an earlier gap or discrepancy; an operator acknowledges it, with a note, and it stays listed.
- **No sampling.** Sequential selection audits every completed checkpoint. Latest-only audits the newest and records every one it passes over. Neither samples records or keys, and the destination is always read for the scope's keys: a table that is still being written never receives a whole-table verdict because the runner is automatic.
- **Bounded.** Every child has a time limit and runs in its own process group. The bundle, the reference's artifact, rows and records, the destination's queries, the state directory's disk and the retained evidence each have a bound. A bound reached makes a result partial or an error, never a pass.
- **Restartable.** One runner holds a state directory at a time. Each step's result is recorded once its output is in place, and the child running is recorded, so a restarted runner stops an orphan and resumes at the interrupted step. A discrepancy already found is not read again; a partial result or an error is tried again in a later cycle, within `max_attempts`, so an outage costs one attempt per audit per cycle.

## The contract

Record-wise jobs from one Kafka SQL source into the ClickHouse native sink, under record 015's table and key rules, and record 015's windowed shape, audited one increment at a time. A windowed job's whole lineage needs every lineage checkpoint's state, which the Workers do not keep, so repeating it is not possible on a running job, let alone cheap. Each audit after the first is instead the increment from the newest checkpoint whose reference state the runner holds (record 015, "Increments"). The reference's state is its own, computed from the records it read; the runner keeps it between audits, checks each artifact names the state it was given, and holds the state the artifact leaves only from a complete reference. A restore resumes the chain at the restore point's state; a runner that fell behind the Workers' retention leaves the chain broken, which the status says, and nothing after it is verified.

Collection is the one step that cannot wait. A scope's bundle needs the snapshots of the checkpoints that bound it, and a Worker keeps only its newest `--checkpoint-num-retained` (2 by default). So a cycle collects every newly completed checkpoint at once, and again between every step of every audit. The bundle is self-contained, and the slower steps run from it later. A cycle audits what was eligible when it began, so a scheduler's run ends. A checkpoint whose state was purged before collection is recorded as expired, not dropped.

`incident-collect --json` now writes a refusal with its kind (`changed`, `missing`, `bound`, `timeout`, `other`), so the runner acts on the refusal without reading its text.

## Considered and not done

- **A subcommand of `clink`.** The runner is orchestration of child processes with timeouts, locks and atomic files, around the operator's reference executable, which is usually Python already. It is written in Python with the standard library only, beside `python/clink-reference`, and calls the `clink` binary the operator names. The engine's commands stay the authority on every verdict.
- **A collector thread.** Collecting concurrently with the audits would collect a little sooner, at the cost of shared mutable state across threads. Checking for new checkpoints between every step keeps one thread and is measured to keep up in the example; a deployment that audits slower than its checkpoints complete raises the Workers' retention instead.
- **Re-seeding a broken chain.** A whole-lineage audit could start a new chain, but it needs the job's history from its start, which the Workers no longer keep by then. A broken chain stays broken, and says so, until a run starts from no state.
- **Folding empty scopes into the next.** A scope whose intervals held no rows is partial in reconciliation, since it establishes nothing about the destination. Extending the next scope over it would need a new scope rule in the engine. The runner instead counts such a scope as covered only when the reference read its complete range and computed no row, and the sink received none.
- **Alerts, a server, a dashboard, distributed coordination, automatic repair.** The runner writes a status file and an event log for whatever already monitors the host, and sends nothing.

## Consequences

An operator gets an answer to "which of this job's checkpoints have been checked, and what was found", with the reasons for every gap and the evidence for every failure kept. The answer is repeated, bounded auditing of the selected scopes. It is not a continuous proof of every result: a scope that was skipped, expired or partial is unverified, and the status says so until someone acknowledges it.

Sequential coverage keeps up only when an audit takes less time than a checkpoint interval. The example measures both, and the status reports a backlog that grows.
