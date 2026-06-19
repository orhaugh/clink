# tools/

Operational and developer tooling for clink.

This directory is intentionally empty for now. Things that will land here:

- A `clink-cli` for inspecting a running engine (DAG topology, operator
  metrics, last completed checkpoint id).
- Codegen for state-backend serializers once the keyed-state API stabilizes.
- A minimal log-grepper for the engine's structured-log output.
