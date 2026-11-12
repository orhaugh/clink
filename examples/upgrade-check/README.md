# Validating an upgrade against retained evidence

A runnable qualification of upgrade validation: a baseline build's savepoint kept as evidence with `clink-audit upgrade record`, and candidates checked against it with `clink-audit upgrade check`. The contract is [Upgrade validation](../../docs/internals/upgrade-validation.md).

```bash
examples/upgrade-check/run.sh                        # every scenario: a qualification
QUICK=1 examples/upgrade-check/run.sh                # the bounded subset CI runs
ONLY=control,prior-release examples/upgrade-check/run.sh
scripts/upgrade-gate.sh [--full]                     # the workflow's tests, then this
```

It needs Python 3.10 or later and a built tree (`CLINK_BIN_DIR`, default `build/`) with `clink`, `clink_node` and `examples/upgrade_check_job*.so`. No services and no network: every build process runs on loopback alone, under isolation proven before anything runs. Everything a run produces is kept under `WORK`: `results/summary.txt`, every expectation and what was observed (`results/scenarios.json`), the costs (`results/costs.json`), each evidence set with its digest kept apart in `results/digests/`, each report, run and log, and every command (`results/commands.txt`).

## The job

`upgrade_job.cpp`, built against each build. A controlled source reads `segment-NNNNNN.txt` files in order, each line an event `E,<time>,<account>,<amount>`, a watermark `W,<time>` or `END`; keyed by account at parallelism 2, `uc-windows` is the engine's 10 s event-time tumbling window (count and sum) and `uc-idle` keeps `last_seen` and `idle_count` with an event-time timer at `last_seen` plus 30 s; two sinks write `windows.txt` and `idle.txt`. `test.json` is the test definition: the seeded input, the semantics, the uids and the capabilities a build must report.

`sdk/CMakeLists.txt` builds the same source against an installed clink, as a job's owner does for each release; that is how the prior release's module is built. Each `upgrade_check_job_<variant>.so` is the same source with one defect compiled in (`CLINK_UC_VARIANT`), an example artifact only:

| Variant | The defect |
| --- | --- |
| `compatible` | none: the fold written another way |
| `wrong_aggregate` | a negative amount is left out of the window's sum |
| `window_boundary` | an event exactly on a window boundary falls in the window before |
| `lost_timers` | `uc-idle` does not restore its timers |
| `schema` | `uc-windows` declares state version 2 with no migration |
| `uid_changed`, `uid_missing` | `uc-windows` has another uid, or none |
| `crash_restore`, `crash_continue` | `uc-idle`'s process is killed (SIGKILL) while restoring its timers, or 40 events after the restore |
| `memory_windows` | the accumulator type is not registered, so the windows are in memory only |
| `string_timer_keys` | the timers are keyed by the account as text |

## The prior release

v0.15.0 at bbd63094 is the baseline of the `prior-release` scenario. `run.sh` builds it from this repository's own tag into `CLINK_UC_PRIOR_DIR` (default `build/upgrade-baselines/v0.15.0`) when it is absent, after checking that the tag resolves to the pinned commit; nothing is downloaded. Building it takes a few minutes and needs the same toolchain as the tree.

## Scenarios

| Scenario | Baseline, candidate | Required |
| --- | --- | --- |
| `control` | this build, twice | exit 0, every required check passed |
| `compatible` | this build, the `compatible` variant | exit 0 |
| `prior-release` | v0.15.0, this build | exit 0; both commits named; the baseline's operator-state copies (written by every restored subtask before 04963a61) reported and left out of the logical comparison |
| each defect variant | this build, the variant | exit 1 with the check that names it: the aggregate and the boundary in `behaviour.output` and `reference.candidate` with the restore passed; lost timers, a changed or missing uid and a crash while restoring in `restore.candidate`; the schema in `schema.candidate` and as a refusal before anything ran, with no output written; a crash after the restore in `recovery.candidate` |
| `memory-windows`, `string-timer-keys` | the variant, twice | `record` flags the savepoint against the reference; the check exits 1 with `behaviour.*` passed and `reference.candidate` failed: two builds that agree and are both wrong (with text timer keys, `evidence.readable` fails too: the timers are in none of the job's encodings) |
| `fresh-run` | this build, a `clink` that drops `--restore-from-dir` | exit 1, `restore.candidate` names the position a fresh start holds |
| `wrong-digest`, `truncated-evidence`, `other-test` | this build, twice | exit 1, `evidence.integrity` failed, no build ran |
| `unsupported-version` | this build, twice | exit 3, `evidence.integrity` unsupported |
| `missing-capability` | this build, a `clink` whose manifest omits `checkpoint_meta_version` | exit 3, `builds.capabilities` unsupported |
| `other-baseline` | v0.15.0 against this build's evidence | exit 1, `builds.identity` failed |
| `timeout`, `memory-limit`, `output-limit` | this build, twice, with a bound below what a run needs | exit 3, the stopped checks unavailable and naming the bound |
| `interrupted` | this build, twice, SIGINT after 4 s | exit 3, the report says interrupted, no build process left running |

Every check that verified the evidence also shows it unchanged afterwards, and every evidence set's digest is checked again at the end of the qualification.
