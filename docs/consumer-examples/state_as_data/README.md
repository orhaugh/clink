# State as data: a running job's state as an open dataset

clink's keyed operator state is not locked inside a proprietary backend. A
checkpoint (or a live job) exports to a plain **Arrow / Parquet** file or an
**Apache Iceberg** snapshot, so you can point DuckDB, pyarrow, Spark, or a
notebook straight at a job's internal state - inventory the live keys, detect
key-group skew, join the keys to your reference data, and time-travel the state
as Iceberg snapshots - with no bespoke state API and no engine running.

This example runs the whole path end to end.

## Run it

Build a clink with the SQL frontend, then run the script:

```bash
cmake -S . -B build -DCLINK_BUILD_SQL=ON
cmake --build build --target clink --parallel 10
pip install pyarrow duckdb   # pyarrow required; duckdb optional (SQL path)
bash docs/consumer-examples/state_as_data/run.sh
```

`run.sh`:

1. Runs a keyed `GROUP BY` (per-user `SUM`) with checkpointing. The aggregate
   operator keeps one state entry per user id; a checkpoint persists it.
2. Exports that keyed state two ways: `clink state-export --format=parquet`
   (one Parquet file) and `--format=iceberg` (one snapshot of an Iceberg table,
   which accumulates a new snapshot on every re-export - that is your state
   time-travel).
3. Runs SQL over the state *in-process* with `clink state-query` - the same
   engine, no export needed.
4. Runs the same analytics *from outside* the engine with `analyze.py`: DuckDB
   straight over the exported Parquet (pyarrow fallback if DuckDB is absent),
   joining the state's keys to a reference table and reporting key-group skew.

Output (the DuckDB path):

```
live keys joined to the reference table (DuckDB over the Parquet):
  alice    user 0    -> key_group 47
  bob      user 1    -> key_group 124
  carol    user 10   -> key_group 36
...
key-group skew (entries per key_group, top 5):
  key_group 17  : 12 keys
...
```

## The dataset

`clink state-export` writes one row per keyed state entry:

| column | meaning |
|---|---|
| `op_id` | the operator whose state this is |
| `key_group` | the key group (0..127) - which subtask owns the key |
| `slot` | the named state slot (here `agg`) |
| `user_key` | the decoded key - here the user id - so it joins to reference data |
| `value_bytes` | the raw operator accumulator bytes |

The **key dimension is immediately useful**: which keys are live, how the
keyspace is spread across subtasks (skew / hot-key detection), and a join to
your users / accounts table. The `value_bytes` is the operator's raw
accumulator; decoding it to a logical value (for a `SUM`, the running total)
needs that operator's accumulator schema - a typed-value projection is a
documented follow-on. `clink state-query` runs SQL over the same state in
`op_id, key_group, slot, user_key, key_int, value, value_int` form (with the
key and value rendered as int64 when they are eight bytes).

The `--format=arrow` variant is the canonical, restorable snapshot; `parquet`
and `iceberg` are the analytics projections. The format contract is
[`docs/internals/state-snapshot-format.md`](../../internals/state-snapshot-format.md),
and the state backends and export surfaces are in
[`docs/internals/state-and-backends.md`](../../internals/state-and-backends.md).

## Live jobs

The same export works against a *running* job without a checkpoint:
`clink state-export --job=<id> [--coordinator=host:port]` (and `state-query --job=<id>`)
fetch the live keyed state over the Coordinator's export route - a per-subtask
atomic view rather than a checkpoint-consistent cut.
