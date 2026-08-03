# Fuzzing

Two halves, and the split is the whole design.

| | Discovery | Regression |
|---|---|---|
| Where | `fuzz/fuzz_<target>.cpp` | `tests/test_fuzz_corpus.cpp` |
| Needs | a clang that ships libFuzzer | nothing |
| Runs for | as long as you give it | milliseconds |
| Gates CI | no | yes |

Discovery finds a crash. The input is committed to `fuzz/corpus/<target>/`
and from then on it is replayed by the ordinary test suite, on every
platform, including builds that could not run a fuzzer at all.

That is what makes a fuzzer finding into a permanent regression test. A
fuzzer on its own does not: nobody reruns the exact input, and the next
campaign starts from a different random seed.

## Running it

```bash
scripts/fuzz.sh                     # every target, 60s each
scripts/fuzz.sh cluster_frame       # one target
scripts/fuzz.sh cluster_frame 3600  # an hour
scripts/fuzz.sh --minimise          # coverage-equivalent shrink
```

The script picks a compiler that can link libFuzzer. Apple clang cannot -
it does not ship libFuzzer - so on macOS this needs Homebrew LLVM, which
the script finds via `brew --prefix llvm`. In the project's Debian image it
needs `libclang-rt-19-dev`; `clang-tidy` pulls the compiler but not the
sanitizer runtimes.

`CLINK_BUILD_FUZZERS=ON` with a toolchain that cannot link libFuzzer is a
**hard CMake error**, not a skip. Someone who asked for fuzzers and
silently got none would believe a fuzzing gate exists.

## When a target finds something

1. libFuzzer writes `fuzz/corpus/<target>/crash-<sha1>`.
2. `git add` it.
3. `tests/test_fuzz_corpus.cpp` now replays it forever.
4. Fix the defect. The committed input is the proof it stays fixed.

## Targets

| Target | Input | Why it is untrusted |
|---|---|---|
| `cluster_frame` | one control-plane message body | arrives from an unauthenticated peer on the control port |
| `checkpoint_meta` | a checkpoint integrity sidecar | read from disk, so also whatever survived a partial write |
| `state_version_map` | packed schema-version text | read out of snapshot metadata |
| `fault_spec` | a `CLINK_FAULT_INJECT` schedule | operator input; only built when fault injection is compiled in |
| `sql_parse` | SQL text | user input by definition; only built with `CLINK_BUILD_SQL=ON` |

`cluster_frame` takes the message kind from byte 0, so a single corpus
entry can be mutated into any decoder rather than only the one it happened
to reach.

## What is and is not committed

Three kinds of file live under `fuzz/corpus/<target>/`:

- **`seed-*`** - generated at build time by `clink_fuzz_seeds`, from the
  real encoders. **Not tracked.** A committed seed goes stale the first
  time a message gains a field, and a stale seed narrows what the fuzzer
  explores without anyone noticing. Same reasoning as the guarantee
  analyser reading the capability registry instead of a literal list.
- **`crash-*`, `leak-*`, `timeout-*`, `oom-*`** - reproducers. **Tracked,
  always.**
- **anything else** - coverage inputs libFuzzer discovered. **Not
  tracked.** A minimised `cluster_frame` corpus alone is ~308 files and
  ~1.2 MB of blobs nobody can review, and the marginal coverage over the
  deterministic property tests in `tests/test_frame_robustness.cpp` is
  modest. Keep them locally, where they make your next run start warm.

## Results so far

First campaign, 2026-08-03, three targets on macOS/arm64 under
`-fsanitize=fuzzer,address,undefined`:

| Target | Executions | Findings |
|---|---|---|
| `cluster_frame` | 1,408,440 | none |
| `checkpoint_meta` | 12,882,991 | none |
| `state_version_map` | 6,559,848 | none |

20.8 million executions, no crashes. Worth reading precisely: it says the
hardening in `docs/production-hardening-plan.md` W14 holds against inputs
nobody wrote down. It does not say the decoders are correct - a fuzzer
finds crashes, not wrong answers, and 45 seconds per target is a smoke
test rather than a campaign.
