# Production qualification harness

Infrastructure and campaign code for the production-qualification programme.
Campaign RESULTS publish to the docs site under `docs/qualification/`; this
directory holds the machinery that produces them. Raw evidence lands in
`qualification-results/` (gitignored, retained per run).

Ground rules, non-negotiable:

- The generator, oracle and chaos controller run on the ops host, OUTSIDE the
  clink failure domain. A clink crash must never destroy the evidence needed
  to judge whether clink behaved correctly.
- Clink never verifies clink: the verifier recomputes expectations from the
  generator's deterministic specification, independently of anything the
  engine wrote.
- Every cloud resource carries `qual=1` and `qual-run=<run-id>` labels.
  `scripts/qualification/destroy.sh <run-id>` deletes by label; an orphaned
  resource is a failed test. `infra/teardown.sh --check` verifies emptiness.
- The rig runs in the Hetzner project behind hcloud context `clink-bench`
  (asserted by provision.sh before anything billable is created).

## Layout

| Path | What |
|------|------|
| `infra/provision.sh` | Create the qualification rig (ops + coordinator + workers + brokers), labelled per run |
| `infra/teardown.sh` | Label-driven teardown + `--check` |
| `infra/*.yml` | Per-role docker compose files, host-networked on the private net |
| `qual01/` | Kafka exactly-once campaign: deterministic generator, independent verifier, pipeline SQL, campaign driver |
| `chaos/chaos.py` | The reusable fault controller (QUAL-09): kills, restarts, latency, partitions, targeted 2PC-window faults |

## The QUAL-01 oracle in one paragraph

Every input event is a pure function of `(seed, partition, seq)` - key,
amount and event time are all derived by a splitmix64 hash, so the FULL
expected output (per-key, per-window counts and sums) is recomputable by
anyone holding the seed and the per-partition high-water sequences, without
trusting the generator's copy of history, let alone clink's. The generator
publishes its progress (per-partition last sequence) to the ops host's disk;
the verifier consumes the output topic with `isolation.level=read_committed`
and continuously compares closed windows against the recomputed expectation,
counting missing, duplicate, conflicting and foreign results. Correctness
configuration: the job runs with source parallelism equal to the partition
count, the configuration proven exact by the per-partition watermark work
(see `benchmarks/nexmark_compare/README.md`); the watermark lag strictly
exceeds the generator's bounded event-time jitter.
