# Production qualification

This section publishes the results of clink's production-qualification
programme: sustained, destructive, measured campaigns against real
infrastructure, designed to expose defects that unit and integration testing
cannot realistically find. It exists so that anyone evaluating clink can see
what has actually been demonstrated, under which conditions, and - just as
importantly - what has not.

The question every campaign answers is:

> Can clink maintain correct output and bounded recovery behaviour for days
> under repeated real infrastructure failures, large state, scaling, upgrades
> and external-system faults?

## How to read these results

Every claim on these pages falls into exactly one of four categories, and no
result is ever promoted beyond the scope actually tested:

- **Demonstrated** - behaviour proven by a completed campaign, with retained
  machine-readable evidence, at a named clink revision.
- **Tested but bounded** - proven up to a stated limit (state size, duration,
  parallelism); behaviour beyond the bound has not been measured.
- **Architecturally supported but not qualified** - the capability exists and
  is unit- and integration-tested, but was not part of a qualification
  campaign.
- **Unknown** - no evidence either way. Absence of failure is not treated as
  proof.

## Methodology

- **Independent oracle.** Correctness is never verified by clink itself. A
  workload generator gives every input record a deterministic identity, keeps
  an authoritative record of it outside clink, and an independent verifier
  computes missing, duplicate, foreign and incorrect results at the
  destination. Generator, oracle and verifier run outside the clink failure
  domain, so a clink crash cannot destroy the evidence needed to judge it.
- **Real faults, not only injected ones.** Campaigns combine clink's
  deterministic fault-injection framework with external process kills, broker
  and database restarts, network partitions, latency, packet loss, credential
  rotation and object-store throttling, targeted at observable engine states
  (for example the two-phase-commit windows around barrier, prepare, commit
  and acknowledgement).
- **Provenance.** Every campaign records the exact clink git SHA, build
  flags, compiler, dependency versions, connector capability manifest,
  cluster and cloud configuration, and start and end timestamps, under a
  unique qualification run id. The final verdict always refers to an
  immutable SHA or tag, never a moving branch.
- **Defects become permanent tests.** A defect found by qualification is
  reduced to its smallest reproduction, pinned by a deterministic regression
  test in clink's CI, fixed, and the campaign that found it is rerun. The
  defect log on each campaign page links the fixes.

## Campaigns

Results are published here as campaigns complete, each on its own page with
the standard summary block: duration, input volume, faults injected,
correctness counters, recovery and checkpoint distributions, resource peaks,
verdict, defects found and regression tests created.

| Campaign | Subject | Status |
|----------|---------|--------|
| QUAL-01 | Kafka exactly-once under a multi-day fault campaign | planned |
| QUAL-02 | PostgreSQL two-phase-commit sink under targeted crash windows | planned |
| QUAL-03 | Object-store checkpointing and transactional sinks under storage faults | planned |
| QUAL-04 | Large state (100 GB and beyond, progressive) across state backends | planned |
| QUAL-05 | State TTL steady-state under sustained high key cardinality | planned |
| QUAL-06 | Large DAG / high-parallelism scaling limits | planned |
| QUAL-07 | Cross-engine semantic comparison on deterministic workloads | planned |
| QUAL-08 | Rolling upgrade with live state | planned |
| QUAL-09 | Infrastructure failure matrix (reusable chaos controller) | planned |
| QUAL-10 | Long-running leak and resource trend campaign | planned |
| QUAL-11 | Schema evolution over long-running jobs | planned |
| QUAL-12 | Security configuration failure behaviour (no silent downgrade) | planned |

No campaign has completed yet: the programme is in its code-hardening phase,
which closes the residual findings of the adversarial external audit of
15 August 2026 before infrastructure spend begins. This page and the campaign
pages update as evidence lands; until a campaign's page exists, its subject
sits in the *unknown* or *architecturally supported but not qualified*
category, and this table is the honest statement of that.

The full production-qualification report, aggregating every campaign into a
single assessment against an immutable release candidate, will be published
here alongside the campaign pages.
