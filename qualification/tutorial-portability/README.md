# Tutorial portability check (not a qualification campaign)

Does the workflow the laptop tutorial teaches map onto a real
Coordinator/Worker deployment, or does it only hold together inside one
compose file?

`check.sh` answers that on the disposable Hetzner rig: a real Redpanda
broker speaking the Kafka protocol, ClickHouse on the ops host, one clink
Coordinator and two Workers on their own hosts, the tutorial's own
`pipeline.sql` submitted at parallelism 4, the tutorial's own workload
definition and verifier, and one Worker SIGKILLed mid-stream.

**This is not a qualification campaign, and its result must never be
presented as one.** The campaigns under `qual01/`..`qual12/` run for hours
under a chaos controller that injects faults into named windows of the
engine's own protocols, judged continuously by an oracle that recomputes
expectations from a seed, with evidence retained and a published page. This
check runs once, injects exactly one fault, and answers one question about
the tutorial's portability. It has no page under `docs/qualification/`, no
row in `qualification-plan.json`, and no place in the qualified-capability
table.

What it does share with the campaigns, deliberately:

- the same rig (`../infra/provision.sh`, `broker.yml`, `coordinator.yml`,
  `worker.yml`) rather than a second infrastructure model,
- the same teardown discipline: everything labelled `qual-run=<id>`, and
  `../infra/teardown.sh --check` afterwards,
- the sink and the verification on the ops host, outside the failure domain,
- verification that recomputes the expectation independently of the engine
  (here: the tutorial's `scripts/workload.py`, unchanged).

## Running it

```bash
RUN_ID=tutorial-dist-$(date +%Y%m%d) qualification/tutorial-portability/check.sh
scripts/qualification/destroy.sh <run-id> --yes    # then:
qualification/infra/teardown.sh --check
```

Five hosts (ops, coordinator, two workers, one broker), roughly EUR 0.28 an
hour at the instance types `provision.sh` defaults to, and the check itself
takes about twenty minutes. `SKIP_PROVISION=1` reuses a live rig.
`KEEP_RIG=1` skips nothing but reminds you louder that it bills until
destroyed.

Evidence lands in `qualification-results/<run-id>/` (gitignored): the
inventory, the image provenance, the rendered pipeline, the verifier's
output, and every container log.

## What a pass means

The same pipeline file, unchanged but for connection addresses and
parallelism, produced the same independently verified per-window aggregates
on a four-subtask distributed job as it does on a laptop, and survived a
Worker SIGKILL mid-stream by restarting from its last completed checkpoint.

It does not mean the delivery semantics changed: output into ClickHouse is
at-least-once there exactly as it is locally, and the check reports the
duplicate windows rather than hiding them.
