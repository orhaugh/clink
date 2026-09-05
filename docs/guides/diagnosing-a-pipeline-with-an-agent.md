---
title: Diagnosing a pipeline with an agent
description: "clink's diagnostic surface as MCP tools: an agent walks a real incident from a wrong total to the record behind it, using replay, capture inspection and state tools, read-only."
---

# Diagnosing a pipeline with an agent

clink ships an MCP server, `clink-mcp`, that exposes the engine's diagnostic
primitives as tools any MCP client can call: checkpoint and savepoint
inspection, the record-capture flight recorder and deterministic replay,
lineage, queryable state, configuration lint and `EXPLAIN`. Nothing in it is
new engine capability. What is new is that an agent can hold all of it at
once, and the primitives happen to be exactly the ones a diagnosis needs.

The claim is deliberately modest. A clink pipeline is **diagnosable by an
agent**: given a wrong number in the output, an agent with these tools can
find the record that produced it, reproduce the emission byte for byte, and
freeze the incident as a regression test. It is not self-healing. The server
is read-only by construction. No tool submits, cancels, stops, rescales,
savepoints or commits anything, and the one tool that writes to disk,
`replay`, writes only to the paths its caller names. Deciding what to do
about what the agent found stays with you.

This page is client-neutral. It shows the tool calls and what they return;
which client issues them, and how it words its reasoning, is up to you.

## Set up

You need a `clink` binary (a release binary, or `build/clink` from a build)
and Python 3.10 or later. The server is not yet on PyPI; install it from a
checkout:

```bash
pip install ./python/clink-mcp
```

Register it with your MCP client. The registration is the same shape for
every client that speaks MCP, a command and its arguments:

```json
{
  "mcpServers": {
    "clink": {
      "command": "clink-mcp",
      "args": ["--clink", "/path/to/clink"]
    }
  }
}
```

Add `"--coordinator", "http://127.0.0.1:8081"` to the arguments when there is
a running cluster to look at; without it the live-cluster tools return a
clear error and everything that reads artefacts still works. The transport
is stdio, which is what MCP clients spawn.

## The incident

A running total of spend per user, fed from a file of orders. One order was
typed as 4990000 instead of 4990. The generator writes the data and the job,
deterministically, into a directory of your choosing:

```bash
python3 examples/agent-diagnosis/make_incident.py /tmp/incident
clink run /tmp/incident/job.sql \
    --checkpoint-dir=/tmp/incident/ckpt --checkpoint-interval-ms=100 \
    --capture-dir=/tmp/incident/capture --capture-records=100000
```

The job is three statements:

```sql
CREATE TABLE orders (usr TEXT, ts BIGINT, amount BIGINT)
  WITH (connector='file', path='/tmp/incident/orders.ndjson', format='json');
CREATE TABLE spend (usr TEXT, total BIGINT)
  WITH (connector='file', path='/tmp/incident/spend.ndjson', format='json');
INSERT INTO spend SELECT usr, SUM(amount) AS total FROM orders GROUP BY usr;
```

The sink receives every update, so the last line for a user is the user's
current total. Seven users end the ten minutes between 12,000 and 21,000.
The eighth does not:

```
$ grep dana /tmp/incident/spend.ndjson | tail -1
{"total":5009143,"usr":"dana"}
```

That line is the symptom. Everything below is what an agent does with it,
given only the checkpoint directory and the capture tree the run left behind.
The `diagnose_incident` prompt the server ships gives a client this plan
verbatim; the calls and outputs are real, from the run above.

## The diagnosis

**1. Trust the artefacts before reading them.**

```
checkpoint_verify(checkpoint_dir="/tmp/incident/ckpt")
-> exit_code 0, result: {"checked": 3, "invalid": 0, ...}
```

Three subtask snapshots, every payload matching its integrity sidecar.

**2. What was recorded.**

```
capture_cat(capture_dir="/tmp/incident/capture")
->  op-15069194450962045214/subtask-1/epoch-1.cap  seen=4801
    op-15539002981765036910/subtask-1/epoch-1.cap  seen=4801
    op-5824225372086884863/subtask-1/epoch-1.cap  seen=4801
```

Three operators captured their input for the one epoch this bounded run
had, 4,801 records each, none truncated. Each operator directory carries an
`op.json` naming its type; the aggregate is `op-5824225372086884863`
(`aggregate_row`). A longer-running job has one epoch per checkpoint, and
`state_diff` between consecutive checkpoints is how an agent narrows a
change to an epoch before opening it.

**3. Reproduce the moment.**

```
replay(capture_dir="/tmp/incident/capture", checkpoint_dir="/tmp/incident/ckpt",
       epoch="1", op="op-5824225372086884863", out="/tmp/incident/emissions.ndjson")
->  replay: op 5824225372086884863 (aggregate_row) subtask 1
    input: .../epoch-1.cap (format v2: 4801 records stored, 4801 seen, 2 watermarks, 0 clock advances)
    fresh state
    wrote 4801 emissions to /tmp/incident/emissions.ndjson
```

The operator is rebuilt from its captured sidecar and fed exactly the events
it consumed, offline, and every emission it made is written out one per
line. The agent now searches that file for the first total of dana's at or
above five million:

```
search_file(path="/tmp/incident/emissions.ndjson",
            pattern='"total":[5-9]\\d{6},"usr":"dana"', max_matches=1, context=1)
->  line 3373: {"total":5003471,"usr":"dana"}
    context 3372: {"total":13186,"usr":"hana"}
```

Emission 3,373 is the moment. dana's previous total was 13,471; one record
took it to 5,003,471, a jump of exactly 4,990,000.

**4. Find the record.**

```
capture_cat(file="/tmp/incident/capture/op-5824225372086884863/subtask-1/epoch-1.cap",
            max_rows=0, grep='"amount":4990000')
->  match_count 1
    line 3375: "{"__key":...,"amount":4990000,"usr":"dana"}"
```

The captured input has the record: user dana, amount 4990000. The two-line
offset between the emission and the record is the epoch dump's two header
lines. An agent that wants the order's timestamp reads it from the source
file with `search_file` the same way.

**5. Check the reproduction is exact.**

```
replay(capture_dir=..., checkpoint_dir=..., epoch="1", verify=true)
->  replay: 3 captured operator(s), epoch 1, verifying determinism (2 runs each)
      op 5824225372086884863 (aggregate_row) subtask 1: 4801 records, 2 watermarks, 0 clock advances -> 4801 emissions [deterministic]
      ...
    deterministic: every replayed operator byte-identical across 2 runs
```

Two replays of every captured operator produced byte-identical emissions,
so the emission stream above is the operator's behaviour, not an artefact of
replaying it. `replay_diff` over two `out` files says the same thing about
two dumps, and locates the first divergence when they differ, which is how a
candidate fix (`replay` with `plugin`) is compared against the shipped
build over the same input.

**6. Keep it.**

```
replay(capture_dir=..., checkpoint_dir=..., epoch="1", op="op-5824225372086884863",
       emit_test="/tmp/incident/bundle")
->  capture + state + golden.ndjson (4801 emissions) + replay_regression_test.cpp
```

The bundle is self-contained: the epoch's capture, the operator's starting
state, the golden emissions and a generated test whose body is one call to
the engine's replay-regression helper. Added to a test target, it passes
forever on a correct build and locates the first divergence on a regressing
one. Whether the right fix is validating amounts at the source, a bound in
the query, or a correction to the order is a decision for the people who own
the pipeline; the agent has given them the record, the moment and a test.

## What the other tools add

The walkthrough used five tools. The rest fill in the surrounding picture:

- `state_cat` and `state_query` read a checkpoint's keyed state, the latter
  as SQL over a table named `state` (add `LIMIT` to any `ORDER BY`). For a
  running total they show the accumulator per user; for a job whose state is
  the story, they are the first stop.
- `state_diff` compares two checkpoints or savepoints: which keys appeared,
  vanished or changed. Decisive when a key should not have appeared, or when
  two savepoints straddle an upgrade.
- `explain` prints the optimised logical plan of a SQL script with its row
  estimates, without running it; `lint` reports configuration that would be
  accepted and then ignored, or that contradicts itself, without a cluster.
  On the incident job with an interval but no checkpoint directory, lint
  says: *a checkpoint interval of 100ms was set but checkpoint_dir is empty,
  so NO checkpoint will ever be taken.*
- With `--coordinator`, `jobs`, `job`, `job_graph`, `job_operators` and
  `job_lineage` read a running cluster (lineage includes column-level lineage
  for SQL jobs), `logs` reads the coordinator's log ring, and
  `queryable_state_lookup` and `queryable_state_scan` read a running job's
  live keyed state, which a SQL `GROUP BY` binds automatically under the
  slot `agg`. `state_query` with `job_id` runs SQL over that live state.
- `clink_capabilities` says what the binary itself can do: connectors
  compiled in and their declared delivery guarantees.

Two ergonomics the server absorbs so an agent does not have to learn them:
it accepts a checkpoint **root** wherever the CLI wants the generation
directory beneath it, and the `op-<id>` directory names `capture_cat` lists
wherever the CLI wants the bare id. Every CLI-backed answer carries the exit
code, stdout and stderr, so a non-zero exit is data the agent reads, not an
exception that ends the conversation; a missing artefact or an unconfigured
coordinator comes back as an error result with the reason.

## Where this sits

The server runs with the privileges of the user who started it and reads
whatever that user can read. Run it as the user you would give a shell to,
and point `--coordinator` only at a coordinator you would let that user
query. It never reaches a route or a verb that changes a job, so the worst a
confused agent can do is read the wrong file.

The tools are the engine's own. The flight recorder and replay are
documented in [replay determinism](../internals/replay-determinism.md) and
[fault tolerance and rescale](../internals/fault-tolerance-and-rescale.md);
the state tools in [state and backends](../internals/state-and-backends.md);
lineage in [data lineage](../internals/data-lineage.md); the HTTP routes in
[distributed runtime](../internals/distributed-runtime.md). The server's own
reference, including how to run its tests, is
[`python/clink-mcp/README.md`](https://github.com/orhaugh/clink/blob/main/python/clink-mcp/README.md).
