# clink-mcp

An MCP server over clink's diagnostic surface. It exposes what the engine
already ships for diagnosing a pipeline - checkpoint and savepoint inspection,
the record-capture flight recorder and deterministic replay, lineage,
queryable state, configuration lint and `EXPLAIN` - as tools any MCP client
can call. It adds no engine feature, and it changes nothing about a running
job: every tool is read-only except `replay`, which writes only to the paths
its caller names.

The published walkthrough, client-neutral, is
[Diagnosing a pipeline with an agent](https://orhaugh.github.io/clink/guides/diagnosing-a-pipeline-with-an-agent/).

## Install

Not yet on PyPI. From a checkout:

```bash
pip install ./python/clink-mcp
```

Python 3.10 or later; the only dependency is the MCP Python SDK (`mcp>=2`).
The server needs a `clink` binary (a release binary, or `build/clink` from a
build) and, for the live-cluster tools, a coordinator's HTTP address.

## Run

```bash
clink-mcp --clink /path/to/clink                                  # artefacts only
clink-mcp --clink /path/to/clink --coordinator http://127.0.0.1:8081   # plus a live cluster
```

The transport is stdio, which is what MCP clients spawn. `CLINK_BIN` and
`CLINK_COORDINATOR_URL` are read when the flags are absent; `--timeout`
bounds any one CLI run or HTTP request (default 300 s). A client
registration is the same for every client that speaks MCP:

```json
{
  "mcpServers": {
    "clink": {
      "command": "clink-mcp",
      "args": ["--clink", "/path/to/clink", "--coordinator", "http://127.0.0.1:8081"]
    }
  }
}
```

## Tools

| Tool | Wraps | Needs |
|---|---|---|
| `clink_capabilities` | `clink --capabilities-json` | binary |
| `explain` | `clink run -e <sql> --explain` | binary |
| `lint` | `clink lint <flags>` | binary (a cluster only with `--from-job`) |
| `checkpoint_verify` | `clink checkpoint-verify --json` | checkpoint directory |
| `state_cat` | `clink state-cat --json` | a snapshot file, or a checkpoint directory and id |
| `state_diff` | `clink state-diff --json` | two snapshot files, or a checkpoint directory and two ids |
| `state_query` | `clink state-query` (SQL over a snapshot's keyed state) | a snapshot, a checkpoint, or a running job |
| `capture_cat` | `clink capture-cat`, with an optional regex filter over the dump | capture tree or epoch file |
| `replay` | `clink replay` (`--op`, `--verify`, `--out`, `--plugin`, `--emit-test`) | capture tree and checkpoint directory |
| `replay_diff` | `clink replay-diff` | two emission dumps |
| `search_file` | a regex search over a text file, with context | any file |
| `jobs`, `job`, `job_graph`, `job_operators`, `job_lineage` | `GET /api/v1/jobs...` | coordinator |
| `cluster`, `health`, `logs` | `GET /api/v1/{cluster,workers,health,logs}` | coordinator |
| `queryable_state_lookup`, `queryable_state_scan` | `GET /api/v1/queryable_state/job/...` | coordinator |

One prompt, `diagnose_incident`, gives a client the standard diagnosis order
over a checkpoint directory and a capture tree.

The server accepts a checkpoint **root** wherever the CLI wants the
generation directory beneath it (`<root>/v1`), and accepts the `op-<id>`
directory names `capture_cat` lists wherever the CLI wants the bare id.
Anticipated failures (a missing artefact, no coordinator configured, a
timeout) come back as error results with the reason; the CLI's exit code,
stdout and stderr are part of every answer, so a non-zero exit is data, not
an exception.

## What it does not do

It never submits, cancels, stops, rescales, savepoints or commits a job; no
tool reaches a route or verb that could. It runs with the privileges of the
user who started it and reads whatever that user can read, so run it as the
user you would give a shell to, and point `--coordinator` only at a
coordinator you would let that user query. It proves nothing on its own: the
tools give an agent the same evidence an engineer would gather, in the same
form.

## Tests

```bash
pip install ./python/clink-mcp pytest
CLINK_BIN=build/clink pytest python/clink-mcp/tests -q
```

The suite generates the incident from the guide, runs it with the real
binary, starts the server over stdio as a client would, and exercises every
tool against what the run left behind. It skips when `CLINK_BIN` is unset.
