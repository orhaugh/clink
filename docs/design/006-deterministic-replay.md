# 006: Incidents replay deterministically from captured input

**Status:** adopted; determinism is a verified contract, not an aspiration.

## Context

Debugging a stateful streaming incident is uniquely hostile: the failure
depends on record order, timing, watermarks, and accumulated state, and by
the time a human looks, the input has moved on. Logs describe symptoms;
they rarely let you re-run the moment. The usual answers - replay from the
source system, or reconstruct conditions by hand - are either impossible
(the retention window closed) or unfaithful (the reconstruction is not the
incident).

The engine already had the two ingredients determinism needs: checkpoint
epochs that bound state to a known snapshot, and a single in-band stream
where records, watermarks, and barriers travel in one recorded order.

## Decision

Make the incident itself a portable artefact:

- A flight recorder captures, per checkpoint epoch, exactly what each
  operator consumed - records, watermarks, barriers, in order - into a
  versioned `.cap` event stream.
- `clink replay` re-executes an operator (or a whole job epoch) over exactly
  those events, offline, starting from the epoch's state snapshot, and
  verifies the output byte-identically. Determinism is enforced by the
  `--verify` gate, not assumed.
- An incident can be frozen: replay bundles export as self-contained
  regression tests, so the bug that happened in production becomes a
  permanent test case.
- Captures push to and fetch from shared storage, so the artefact that
  reproduces the incident travels to whoever debugs it.

## Consequences

- The debugging loop becomes: fetch the capture, replay locally, fix, keep
  the replay as the regression test. No staging cluster, no source-retention
  race, no "cannot reproduce".
- Operators must stay deterministic given identical input order and state -
  wall-clock reads, unordered iteration, and hidden randomness in the
  processing path are bugs by contract. The verify gate makes violations
  visible instead of latent.
- The trade-off accepted: capture is bounded work on the hot path and real
  storage per epoch. It is scoped by configuration rather than free, and
  that cost is judged worth a reproducible incident every time it is paid.

See [Replay determinism](../internals/replay-determinism.md) for the `.cap`
format and the verification contract.
