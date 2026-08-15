# Campaign feasibility assessment

Before provisioning any infrastructure, every campaign in the
production-qualification programme was audited against the code to answer one
question: would running it produce evidence, or would it burn time and money
proving nothing?

The audit was worth more than the campaigns it was protecting. It found three
campaign-fatal blockers, two live security defects, and one real resource leak,
none of which needed a cloud rig to discover. It also established that three
campaigns cannot be run as written and had to be rescoped honestly rather than
run to produce a confident-looking null result.

This page records what was found and what was done, because the negative
results are as much a part of the qualification record as the campaigns
themselves.

## Campaign-fatal blockers found and fixed

**The chaos controller would have applied zero faults.** Its liveness gate
polled a `status` field on `GET /api/v1/jobs/:id`, and that endpoint returned
no such field - nor the `job` wrapper the code unwrapped. The gate could never
be true, so the controller would have looped on "waiting for a healthy
checkpoint" for the entire campaign, applied nothing, and produced a log full
of timeouts. Fixed on both sides: the API now reports an explicit job status
(`RUNNING` / `COMPLETED_OK` / `FAILED` / `CANCELLED`, on the same precedence
the control plane already used), which was a genuine gap in the ops API in its
own right; and the controller derives liveness from fields that have always
existed, so it works against a coordinator of either vintage. A campaign that
applies no faults now exits non-zero saying so, because a silent campaign and
a successful one must never look alike.

**A SQL job on a cluster could not enable checkpointing at all.** The
submission endpoints accepted only a state-backend URI, so no SQL cluster job
could take periodic checkpoints - and without completed checkpoints a
transactional sink never commits and a lost worker is never recovered. The
flagship exactly-once campaign could not have been started. Fixed: both
endpoints and the CLI now carry the full durability configuration, and the
coordinator classifies the campaign pipeline as end-to-end exactly-once, which
was previously unreachable.

**The restart budget would have ended the campaign after about ten faults.**
The worker-loss restart count defaults to a *lifetime* cap of ten and is never
reset, so a multi-day chaos campaign would have exhausted it in the first hour
and then measured a stopped job. The budget is now settable from the SQL
submission path, and the campaigns set it deliberately.

## Live defects found by the audit

**Postgres connections could silently downgrade to plaintext.** The connectors
passed a raw connection string to libpq with no TLS handling anywhere, and
libpq's default mode tries TLS then falls back to an unencrypted connection
without comment. An operator who assumed a managed database meant an encrypted
link had no way to discover otherwise. clink now reads the transport actually
established and refuses to continue when encryption was demanded and not
obtained, warns plainly when the link came up unencrypted with no mode stated,
and records the encrypted case so the transport is provable from the log.

**Kafka credentials could be configured and never presented.** Supplying a
SASL username and password without a security protocol left the credentials
set and the connection plaintext and unauthenticated - accepted silently by
the client library. That configuration, and TLS material paired with a
plaintext transport, are now refused before a byte reaches a broker, with the
fix named in the diagnostic. Deliberate plaintext remains available; it has to
be stated.

**Workers leaked a thread handle per completed subtask.** Finished subtasks
were never joined until the worker shut down, so each one held its stack
mapping for the process lifetime - invisible to a thread count, visible only
as slowly climbing address space under job churn. Finished handles are now
reaped, pinned by a regression test that asserts on the handle count, since
that is the only place the defect is observable.

## What the first cloud run taught

The rig itself found more harness defects than the code audit did, and every
one of them was silent.

The campaign driver hung twice on the same thing: ssh holds its channel open
until the remote command's descriptors are released, so a backgrounded process
left the driver waiting on a step it believed finished - once while the
generator it had started was already producing ninety thousand events.

The verifier had to be corrected three times before it could judge anything
honestly. A fixed consumer group meant a re-run got a partial partition
assignment and reported three hundred phantom missing results. Requiring the
consumer to sit exactly at the topic's high-water mark can never be satisfied
while a pipeline is still producing, so it deferred judgement indefinitely. And
when the consumer group had not finished assigning on the first pass, it stored
an empty offset snapshot that no later read could ever satisfy, leaving those
windows permanently unjudgeable while a quarter of a million records streamed
past.

The campaign did not own its topics, so a previous run's events - carrying a
different event-time base the oracle knew nothing about - produced 161,111
"missing" results that were never that run's to produce. Read as an engine
defect, that would have been completely wrong.

**And the faults were not landing at all.** Every chaos command was addressed
to its target's public IP. The firewall added that same night, to stop an
unauthenticated control plane facing the internet, admits ssh from the operator
and the private network only - so each command timed out and was silently
ignored while the controller wrote the fault into the evidence log and
continued. The cluster completed 1,771 checkpoints, failed none, and lost no
worker. The campaign was minutes from publishing a fault-tolerance result for a
cluster that nothing had touched.

That is the failure this programme exists to prevent, and it was caught only
because the campaign is required to prove each link before it soaks. The
controller now addresses hosts privately, refuses to continue when a fault
command fails, and confirms a kill by observing the process gone rather than
assuming it. The gate no longer accepts a recorded fault as evidence either: it
reads the engine's own count of workers lost, so a fault must be visible to
clink before anything is allowed to run unattended.

The lesson generalises beyond this harness. A qualification result is only
worth the weakest unverified assumption in the path that produced it, and every
one of these would have produced a confident, green, meaningless page.

## Campaigns rescoped

**Rolling upgrade** cannot be run as specified for compiled plugin jobs: the
plugin ABI fingerprint covers every public header, so one plugin binary cannot
satisfy both sides of a mixed-version cluster, and "replace workers
incrementally" is therefore not a supported operation for that job type. SQL
jobs carry no plugin and are the viable path. Separately, the most recent
release predates the protocol-versioning and checkpoint-integrity work
entirely, so it is not a valid upgrade source; the campaign needs a candidate
release cut first. The finding that plugin jobs upgrade by stop-and-replace is
itself a result worth publishing.

**Large state** cannot reach its headline figure on the planned rig or on the
planned backends. SQL window and aggregate operators hold hot state in memory
unless the state backend defers reads, so a hundred gigabytes of live state on
a synchronous backend measures worker memory and then runs out of it. Only the
deferring backends put that state genuinely behind the backend, and several of
them are absent from the published runtime image. The campaign is being
rescoped to the backends where the measurement means something, with the
memory arithmetic stated rather than discovered.

**TTL steady state** has no instrument for its own pass criterion: the
statistics that would show live entries, expired entries and cleanup backlog
exist internally but are not exported, and on the disk-backed backends they
are not even populated. The audit also found that the cleanup path triggers a
full compaction on every watermark, which would dominate any measurement taken
at high cardinality. Both need fixing before the campaign can produce a
meaningful envelope.

**Large DAG** scaling needs a generator that does not exist, and its results
would be shaped by operator chaining and a hard limit on keyed parallelism
rather than by thread scheduling, unless the generated graphs are built to
avoid both. The admissible graph shapes at each size are now known.

## What this means for the programme

The campaigns that remain feasible without further engineering are the Kafka
exactly-once campaign, the Postgres two-phase-commit campaign, and the
infrastructure failure matrix. Those are the ones with real fault windows,
real observability behind their criteria, and a pipeline that submits and is
classified correctly by the engine.

The rest are gated on the work above. That is the honest position, and it is a
better one than a set of green pages generated by campaigns that could not
have failed.
