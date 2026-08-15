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

## Feasibility first

Every campaign was audited against the code before any infrastructure was
provisioned, to establish whether running it would produce evidence at all.
That audit found three campaign-fatal blockers, two live security defects and
a resource leak - none of which needed a cloud rig to find - and established
that several campaigns could not be run as written. See
[Campaign feasibility assessment](feasibility.md). The negative results there
are part of the qualification record, not a preamble to it.

## Campaigns

Results are published here as campaigns complete, each on its own page with
the standard summary block: duration, input volume, faults injected,
correctness counters, recovery and checkpoint distributions, resource peaks,
verdict, defects found and regression tests created.

| Campaign | Subject | Status |
|----------|---------|--------|
| [QUAL-01](qual-01-kafka-exactly-once.md) | Kafka exactly-once under a fault campaign | **completed - two engine defects found and fixed** |
| QUAL-02 | PostgreSQL two-phase-commit sink under targeted crash windows | feasible, not yet run |
| QUAL-09 | Infrastructure failure matrix (reusable chaos controller) | feasible, not yet run |
| QUAL-03 | Object-store checkpointing and transactional sinks under storage faults | rescoped; see feasibility |
| QUAL-04 | Large state (100 GB and beyond, progressive) across state backends | blocked; see feasibility |
| QUAL-05 | State TTL steady-state under sustained high key cardinality | blocked; see feasibility |
| QUAL-06 | Large DAG / high-parallelism scaling limits | blocked; see feasibility |
| QUAL-07 | Cross-engine semantic comparison on deterministic workloads | not yet assessed |
| QUAL-08 | Rolling upgrade with live state | rescoped; see feasibility |
| QUAL-10 | Long-running leak and resource trend campaign | blocked; see feasibility |
| QUAL-11 | Schema evolution over long-running jobs | rescoped; see feasibility |
| QUAL-12 | Security configuration failure behaviour (no silent downgrade) | two defects already found and fixed |

QUAL-01 has completed and found two genuine defects in clink, both now fixed
and pinned by regression tests: source offsets that could be silently replayed
or skipped on a plain restart, breaking exactly-once, and a configured
checkpoint interval that was ignored entirely. Its page is the model for the
rest: the defects are the headline, and the limits of what was actually
exercised are stated in the same breath as the results.

The remaining table entries say which campaigns could even produce evidence
today. Until a campaign's page exists, its subject sits in the *unknown* or
*architecturally supported but not qualified* category, and this table is the
honest statement of that.

The full production-qualification report, aggregating every campaign into a
single assessment against an immutable release candidate, will be published
here alongside the campaign pages.
