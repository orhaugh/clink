# failover + cold-start benchmark

Measures two things the throughput benches (`inproc_compare`, `prod_compare`)
do not, and that the native, no-JVM, no-warmup design is meant to win on:

1. **Cold start** - wall-clock from a cold process launch to a running,
   producing cluster job.
2. **Failover recovery** - wall-clock from a TaskManager `SIGKILL` to the
   job processing again, restored from its last checkpoint.

This is a clink-only measurement. There is no cross-engine ratio here; the
point is that these numbers were previously unmeasured.

## What it does

The driver is a standalone C++ binary, `clink_failover_coldstart_bench`. It
spawns real `clink_node` JobManager/TaskManager processes and drives them
exactly as a deployment would, reusing the two-phase-commit example job: a
bounded slow source that checkpoints its offset, piped to a 2PC file sink
that commits on checkpoint. The committed output is the work-done proof for
both phases, and it is exactly-once by construction (offset checkpoint +
commit-on-checkpoint), so a correct run commits every record once - no
duplicates, no loss - even across the crash.

### Cold start (median over 5 fresh launches)

| phase         | what it times                                              |
|---------------|------------------------------------------------------------|
| `jm_up`       | spawn JM process -> control port accepts TCP               |
| `tm_register` | spawn TM process -> "registered" on its stdout             |
| `deploy_run`  | `clink_submit_job` start -> a small bounded job commits     |
| `total`       | JM spawn -> bounded job committed                          |

`jm_up` + `tm_register` are the native-binary cold-start edge: a JobManager
listening and a TaskManager registered in well under 100 ms each, because
there is no JVM to start or warm up. `deploy_run` is a separate number - the
cluster job-submission round-trip (deploy, per-job plugin `dlopen`,
peer-update handshake, run, 2PC commit, completion) for a trivial job - and
is dominated by deploy/coordination, not record processing.

### Failover recovery

A single TaskManager (TM-A) hosts the whole job; once it is checkpointing, a
second TaskManager (TM-B) is brought up as the recovery target, then TM-A is
`SIGKILL`ed. `recovery_ms` is the wall-clock from the kill to the first
**new** durable checkpoint (a `COMPLETED-N` marker with a higher id than any
before the crash), which proves the redeployed job is processing again.

The benchmark self-validates: it confirms the JobManager watchdog actually
logged the TM loss (`real failover: yes`), so a kill that happened to hit an
idle TM cannot be reported as a recovery, and it asserts the completed job
committed every record exactly once.

`recovery_ms` includes the TM-loss detection window. Detection is via the
JobManager watchdog (there is no connection-close fast path), so roughly
`--heartbeat-timeout-ms` of the number is the (tunable) detection delay; the
remainder is redeploy + state restore + resume. The benchmark sets the
heartbeat timeout low (1000 ms, safely above the TM's 500 ms heartbeat
interval) so the measured figure reflects the actual recovery work rather
than the conservative 5 s default.

## Building and running

The driver needs the cluster binaries and the 2PC job `.so`, all built from
the same git commit so their plugin ABI hashes match. It is wired under the
integration-test gate:

```bash
cmake -S . -B build -DCLINK_INTEGRATION_TESTS=ON -DCLINK_BUILD_ROCKSDB=ON
cmake --build build --target clink_failover_coldstart_bench -j8

# via ctest (opt-in label), or run the binary directly for the full scoreboard:
ctest --test-dir build -L benchmark -V
./build/tests/clink_failover_coldstart_bench
```

The binary prints a scoreboard and exits 0 iff both scenarios completed and
produced correct (exactly-once) output.

## Illustrative numbers

From a run in the `clink-build:latest` Docker image (Debug build; absolute
values are environment-specific - treat the shape, not the digits, as the
result):

```
COLD START (median over 5 ok runs):
  jm_up        ~50  ms
  tm_register  ~40  ms
  deploy_run   ~3.5 s   (cluster submit -> trivial job committed)
  total        ~3.6 s

FAILOVER RECOVERY:
  real failover   yes
  recovery_ms     ~2.5 s   (SIGKILL -> first new durable checkpoint)
                  of which ~1.0 s is the tunable detection window;
                  ~1.5 s is redeploy + state restore + resume
  committed       400 / 400 (exactly-once across failover: yes)
```

## Honest caveats

- Absolute numbers depend on the machine, build type (Debug here), and
  filesystem; the benchmark is for tracking clink against itself over time
  and for isolating the recovery-work component from the detection delay.
- `deploy_run` measures a full bounded-job lifecycle including the 2PC
  terminal commit and completion handshake, not just "time to first record";
  it is the cluster round-trip cost, and a candidate for future optimisation.
- The recovery path exercised is same-parallelism redeploy onto a surviving
  TaskManager (the `--max-restarts-on-tm-loss` auto-restart). JobManager HA
  takeover and rescale-on-restart are covered by the integration tests, not
  this benchmark.
