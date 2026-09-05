"""The clink MCP server: clink's diagnostic surface as MCP tools.

Every tool wraps something the engine already ships - a ``clink`` CLI verb or
a coordinator HTTP route - and returns its output as structured content. The
server adds no engine feature. It is read-only by construction: nothing here
submits, cancels, stops, rescales, savepoints or commits a job, and the one
tool that writes to disk, ``replay``, writes only to the paths its caller
names (``out`` and ``emit_test``). What a client can do with these tools is
diagnose a pipeline. Fixing it stays a human decision.

Two surfaces:

- CLI-backed tools run the ``clink`` binary (``Config.clink_bin``) as a
  subprocess and parse its output (``--json`` where the verb offers it,
  NDJSON for ``state-query``, text otherwise). They need no cluster: they
  read checkpoint directories, savepoint files and capture trees.
- HTTP-backed tools call a running coordinator's ``/api/v1`` routes
  (``Config.coordinator_url``) and are disabled, with a clear error, when no
  coordinator URL is configured.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

READ_ONLY = ToolAnnotations(
    read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False
)
WRITES_NAMED_PATHS = ToolAnnotations(
    read_only_hint=False, destructive_hint=False, idempotent_hint=True, open_world_hint=False
)

INSTRUCTIONS = """\
clink is a stream processing engine. These tools inspect a clink job's durable
artefacts and, when a coordinator URL is configured, its live state. They are
read-only: nothing here changes a running job. Diagnosing an incident usually
runs: checkpoint_verify on the checkpoint directory; state_diff between two
checkpoints to find which key's state moved unexpectedly (or state_query for
SQL over one checkpoint's state); capture_cat on the capture tree to list the
recorded epochs, then on the epoch that spans the change to find the record;
replay of that operator over that epoch, with out set, to reproduce the exact
emission; and replay with emit_test to freeze the incident as a regression
test. explain and lint work on SQL and configuration without a cluster.
"""


@dataclass(frozen=True)
class Config:
    """How the server reaches the engine.

    clink_bin: path to the ``clink`` CLI. coordinator_url: base URL of a
    coordinator's HTTP API (``http://host:port``), or None to disable the
    live-cluster tools. timeout_s: the bound on any one CLI run or request.
    """

    clink_bin: str
    coordinator_url: str | None = None
    timeout_s: float = 300.0


class ClinkError(ToolError):
    """A tool could not produce an answer: the binary is missing, a call timed out, a
    coordinator refused, or the arguments named no artefact. Raised as a ToolError so
    the message reaches the client verbatim as an error result rather than as the
    SDK's generic crash text."""


_NOISE_MARKERS = ("[registry.replace]",)


def _quiet(stderr: str) -> str:
    """Drop the CLI's routine registration chatter; keep everything that could be a cause."""
    return "\n".join(
        line for line in stderr.splitlines() if not any(m in line for m in _NOISE_MARKERS)
    ).strip()


def _run(cfg: Config, args: list[str]) -> dict[str, Any]:
    """Run one CLI verb. Never raises on a non-zero exit: the exit code is part of the answer."""
    try:
        proc = subprocess.run(  # noqa: S603 - the binary is the configured clink CLI
            [cfg.clink_bin, *args],
            capture_output=True,
            text=True,
            timeout=cfg.timeout_s,
            check=False,
        )
    except FileNotFoundError as e:
        raise ClinkError(f"clink binary not found at {cfg.clink_bin}") from e
    except subprocess.TimeoutExpired as e:
        raise ClinkError(f"clink {' '.join(args[:2])} exceeded {cfg.timeout_s:g}s") from e
    return {
        "command": ["clink", *args],
        "exit_code": proc.returncode,
        "stdout": proc.stdout,
        "stderr": _quiet(proc.stderr),
    }


def _json_from(run: dict[str, Any]) -> dict[str, Any]:
    """Parse a ``--json`` verb's stdout; a verb that printed no JSON returns its text."""
    text = run["stdout"].strip()
    if text:
        try:
            run["result"] = json.loads(text)
            del run["stdout"]
        except json.JSONDecodeError:
            pass
    return run


def _ndjson_rows(run: dict[str, Any]) -> dict[str, Any]:
    rows: list[Any] = []
    for line in run["stdout"].splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            rows.append({"unparsed": line})
    run["rows"] = rows
    run["row_count"] = len(rows)
    del run["stdout"]
    return run


def _get(cfg: Config, path: str, params: dict[str, Any] | None = None) -> Any:
    """GET a coordinator route; JSON bodies come back parsed, others as text."""
    if not cfg.coordinator_url:
        raise ClinkError(
            "no coordinator configured: start the server with --coordinator "
            "http://host:port (or CLINK_COORDINATOR_URL) to use the live-cluster tools"
        )
    url = cfg.coordinator_url.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=cfg.timeout_s) as resp:  # noqa: S310 - configured base URL
            body = resp.read().decode("utf-8", errors="replace")
            ctype = resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise ClinkError(f"GET {url} -> {e.code}: {detail.strip()[:2000]}") from e
    except urllib.error.URLError as e:
        raise ClinkError(f"GET {url} failed: {e.reason}") from e
    if "json" in ctype or body[:1] in "{[":
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            pass
    return {"text": body}


def _state_root(checkpoint_dir: str) -> str:
    """The directory the state verbs read: a checkpoint root holds one generation
    directory per topology (``v1``, ``v2``, ...) with the subtask snapshots
    beneath; the verbs take the generation directory. Given the root, pick the
    highest generation; given a generation directory (or anything else), pass it
    through unchanged."""
    try:
        gens = [
            (int(name[1:]), name)
            for name in os.listdir(checkpoint_dir)
            if re.fullmatch(r"v[0-9]+", name) and os.path.isdir(os.path.join(checkpoint_dir, name))
        ]
    except OSError:
        return checkpoint_dir
    if not gens:
        return checkpoint_dir
    return os.path.join(checkpoint_dir, max(gens)[1])


def _op_id(op: str) -> str:
    """``replay --op`` takes the bare operator id; capture_cat lists directories named op-<id>."""
    return op[3:] if op.startswith("op-") else op


def _host_port(cfg: Config) -> str:
    if not cfg.coordinator_url:
        raise ClinkError("no coordinator configured (see --coordinator)")
    parsed = urllib.parse.urlparse(cfg.coordinator_url)
    if not parsed.hostname or not parsed.port:
        raise ClinkError(f"coordinator URL needs host and port: {cfg.coordinator_url}")
    return f"{parsed.hostname}:{parsed.port}"


def build_server(cfg: Config) -> MCPServer:  # noqa: C901 - one registration per tool
    """Assemble the server. Tools close over ``cfg``; nothing else is global."""
    server = MCPServer(
        "clink",
        title="clink diagnostics",
        instructions=INSTRUCTIONS,
        version="0.1.0",
    )

    # ------------------------------------------------------------------ static
    @server.tool(annotations=READ_ONLY, structured_output=True)
    def clink_capabilities() -> dict[str, Any]:
        """What this clink binary can do: connectors compiled in, their declared
        delivery guarantees, and build surfaces. Read it first when a
        connector or guarantee is in question."""
        return _json_from(_run(cfg, ["--capabilities-json"]))

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def explain(sql: str) -> dict[str, Any]:
        """Print the optimised logical plan of a SQL script without running it.
        `sql` is a complete script: the CREATE TABLE statements plus the INSERT
        INTO ... SELECT (or bare SELECT) to explain. Each plan node carries its
        estimated output rows; a scan with no declared statistics is flagged.
        Nothing runs and no sink is written."""
        return _run(cfg, ["run", "-e", sql, "--explain"])

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def lint(flags: list[str]) -> dict[str, Any]:
        """Check a job or node configuration for settings that would be accepted
        and then ignored, or that contradict each other. `flags` are the same
        flags `clink run` and `clink_node` take, e.g. ["--checkpoint-dir=/x",
        "--checkpoint-interval-ms=0"]; use ["--from-job=host:port/<id>"] to lint
        a running job's deployed configuration. Exit code 1 means submission
        would refuse the configuration. No cluster is contacted unless
        --from-job is given."""
        return _run(cfg, ["lint", *flags])

    # ------------------------------------------------------- durable artefacts
    @server.tool(annotations=READ_ONLY, structured_output=True)
    def checkpoint_verify(checkpoint_dir: str) -> dict[str, Any]:
        """Verify a checkpoint directory's integrity: every snapshot payload
        against its sidecar, recursively. Run this before trusting any other
        reading of the directory."""
        return _json_from(_run(cfg, ["checkpoint-verify", "--dir", checkpoint_dir, "--json"]))

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def state_cat(
        file: str | None = None,
        checkpoint_dir: str | None = None,
        checkpoint_id: int | None = None,
        max_rows: int = 50,
    ) -> dict[str, Any]:
        """Dump the keyed state held in one checkpoint or savepoint: operators,
        slots, keys and values. Give either `file` (a .snap or .arrows file) or
        `checkpoint_dir` plus `checkpoint_id` (every subtask's file for that id,
        merged). `max_rows` bounds the entries shown per slot; 0 shows all."""
        args = ["state-cat"]
        if file:
            args += [f"--file={file}"]
        elif checkpoint_dir and checkpoint_id is not None:
            args += [f"--dir={_state_root(checkpoint_dir)}", f"--id={checkpoint_id}"]
        else:
            raise ClinkError("state_cat needs file, or checkpoint_dir and checkpoint_id")
        args += ["--json", f"--max-rows={max_rows}"]
        return _json_from(_run(cfg, args))

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def state_diff(
        a: str | None = None,
        b: str | None = None,
        checkpoint_dir: str | None = None,
        from_id: int | None = None,
        to_id: int | None = None,
        max_rows: int = 20,
    ) -> dict[str, Any]:
        """Compare the keyed state of two checkpoints or savepoints: which keys
        appeared, vanished or changed, per operator and slot, with samples.
        This is the first move when an aggregate looks wrong: diff consecutive
        checkpoints until the change appears. Give either two files `a` and
        `b`, or `checkpoint_dir` with `from_id` and `to_id`. Exit code 0 means
        identical, 1 means different (as diff(1) does)."""
        args = ["state-diff"]
        if a and b:
            args += [f"--a={a}", f"--b={b}"]
        elif checkpoint_dir and from_id is not None and to_id is not None:
            args += [f"--dir={_state_root(checkpoint_dir)}", f"--from={from_id}", f"--to={to_id}"]
        else:
            raise ClinkError("state_diff needs a and b, or checkpoint_dir with from_id and to_id")
        args += ["--json", f"--max-rows={max_rows}"]
        return _json_from(_run(cfg, args))

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def state_query(
        sql: str,
        from_file: str | None = None,
        checkpoint_dir: str | None = None,
        checkpoint_id: int | None = None,
        job_id: int | None = None,
    ) -> dict[str, Any]:
        """Run SQL over keyed state, exposed as a table named `state` with
        columns op_id, key_group, slot, user_key, key_int, value, value_int.
        Read one snapshot with `from_file`, a whole checkpoint with
        `checkpoint_dir` plus `checkpoint_id`, or a RUNNING job's live state with
        `job_id` (needs the configured coordinator). Example:
        SELECT user_key, value_int FROM state WHERE slot = 'agg' ORDER BY
        value_int DESC LIMIT 5."""
        args = ["state-query"]
        if from_file:
            args += [f"--from={from_file}"]
        elif checkpoint_dir and checkpoint_id is not None:
            args += [f"--dir={_state_root(checkpoint_dir)}", f"--id={checkpoint_id}"]
        elif job_id is not None:
            args += [f"--job={job_id}", f"--coordinator={_host_port(cfg)}"]
        else:
            raise ClinkError(
                "state_query needs from_file, checkpoint_dir and checkpoint_id, or job_id"
            )
        args += [f"--sql={sql}"]
        return _ndjson_rows(_run(cfg, args))

    # ------------------------------------------------------ capture and replay
    @server.tool(annotations=READ_ONLY, structured_output=True)
    def capture_cat(
        file: str | None = None,
        capture_dir: str | None = None,
        max_rows: int | None = None,
        grep: str | None = None,
        max_matches: int = 200,
    ) -> dict[str, Any]:
        """Inspect the record-capture flight recorder. With `capture_dir`, list
        every captured operator, subtask and epoch with its record count and
        whether the epoch was truncated at the capture cap. With `file` (one
        epoch-N.cap), dump the captured events: records, watermarks and timer
        firings, in the order the operator consumed them. `max_rows` bounds the
        dump (0 = every event). `grep` keeps only the dump lines matching that
        regular expression (up to `max_matches`, each with its 1-based line
        number), which is how to find the record behind a bad value without
        reading the whole epoch."""
        args = ["capture-cat"]
        if file:
            args += [f"--file={file}"]
        elif capture_dir:
            args += [f"--dir={capture_dir}"]
        else:
            raise ClinkError("capture_cat needs file or capture_dir")
        if max_rows is not None:
            args += [f"--max-rows={max_rows}"]
        run = _run(cfg, args)
        if grep:
            try:
                pattern = re.compile(grep)
            except re.error as e:
                raise ClinkError(f"grep is not a valid regular expression: {e}") from e
            lines = run["stdout"].splitlines()
            hits = [
                {"line": i, "text": text}
                for i, text in enumerate(lines, start=1)
                if pattern.search(text)
            ]
            run["header"] = "\n".join(lines[:2])
            run["matches"] = hits[:max_matches]
            run["match_count"] = len(hits)
            del run["stdout"]
        return run

    @server.tool(annotations=WRITES_NAMED_PATHS, structured_output=True)
    def replay(
        capture_dir: str,
        checkpoint_dir: str,
        epoch: str,
        op: str | None = None,
        verify: bool = False,
        out: str | None = None,
        plugin: str | None = None,
        emit_test: str | None = None,
    ) -> dict[str, Any]:
        """Replay a captured epoch offline and deterministically. `epoch` is a
        checkpoint id or "final". With `op` (an operator id, or the op-<id>
        directory name capture_cat lists), rebuild that one operator from its captured sidecar,
        restore its state from the checkpoint before the epoch and feed it the
        recorded events; without `op`, replay every captured operator and print
        a per-operator summary. `verify` runs the replay twice and compares
        emissions byte for byte. `out` writes every emission to that file, one
        per line, for replay_diff. `plugin` dlopens a candidate job build first
        (a cross-version A/B). `emit_test` materialises a self-contained
        regression bundle at that directory. This tool writes only to `out` and
        `emit_test`; it never touches the job or the checkpoint directory."""
        args = [
            "replay",
            f"--capture-dir={capture_dir}",
            f"--checkpoint-dir={checkpoint_dir}",
            f"--epoch={epoch}",
        ]
        if op:
            args.append(f"--op={_op_id(op)}")
        if verify:
            args.append("--verify")
        if out:
            args.append(f"--out={out}")
        if plugin:
            args.append(f"--plugin={plugin}")
        if emit_test:
            args.append(f"--emit-test={emit_test}")
        return _run(cfg, args)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def replay_diff(a: str, b: str) -> dict[str, Any]:
        """Diff two replay emission dumps (files written by replay with `out`)
        and report the first divergence. The cross-version A/B: replay the same
        epoch with and without a candidate plugin and diff the dumps."""
        return _run(cfg, ["replay-diff", a, b])

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def search_file(
        path: str, pattern: str, max_matches: int = 50, context: int = 0
    ) -> dict[str, Any]:
        """Search a text file (an emission dump written by replay, a source file,
        a sink file, a log) for lines matching a regular expression. Returns up
        to `max_matches` matches with 1-based line numbers, each with `context`
        lines around it, plus the file's line count. Read-only; exists so a
        client without its own file tools can read what replay wrote."""
        try:
            pat = re.compile(pattern)
        except re.error as e:
            raise ClinkError(f"pattern is not a valid regular expression: {e}") from e
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                lines = f.read().splitlines()
        except OSError as e:
            raise ClinkError(f"cannot read {path}: {e}") from e
        matches: list[dict[str, Any]] = []
        for i, text in enumerate(lines, start=1):
            if pat.search(text):
                lo, hi = max(1, i - context), min(len(lines), i + context)
                matches.append(
                    {
                        "line": i,
                        "text": text,
                        "context": [
                            {"line": j, "text": lines[j - 1]} for j in range(lo, hi + 1) if j != i
                        ]
                        if context
                        else [],
                    }
                )
                if len(matches) >= max_matches:
                    break
        return {"path": path, "line_count": len(lines), "matches": matches}

    # ------------------------------------------------------------ live cluster
    @server.tool(annotations=READ_ONLY, structured_output=True)
    def cluster() -> dict[str, Any]:
        """The coordinator's view of the cluster: workers, slots, leadership."""
        return {"cluster": _get(cfg, "/api/v1/cluster"), "workers": _get(cfg, "/api/v1/workers")}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def health() -> dict[str, Any]:
        """The coordinator's health endpoint."""
        return {"health": _get(cfg, "/api/v1/health")}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def jobs() -> dict[str, Any]:
        """List active and recently completed jobs with their state and
        checkpoint progress."""
        return {"jobs": _get(cfg, "/api/v1/jobs")}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def job(job_id: int) -> dict[str, Any]:
        """One job's status: state, restart attempts, latest completed and
        confirmed checkpoints, errors."""
        return {"job": _get(cfg, f"/api/v1/jobs/{job_id}")}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def job_graph(job_id: int) -> dict[str, Any]:
        """The job's operator graph as deployed: operators, edges, parallelism."""
        return {"graph": _get(cfg, f"/api/v1/jobs/{job_id}/graph")}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def job_operators(job_id: int) -> dict[str, Any]:
        """Live per-operator overlays for a running job: records in and out,
        backpressure, checkpoint alignment, per subtask."""
        return {"operators": _get(cfg, f"/api/v1/jobs/{job_id}/operators")}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def job_lineage(job_id: int) -> dict[str, Any]:
        """The job's data lineage: source and sink datasets and the edges
        between them, with column-level lineage for SQL jobs."""
        return {"lineage": _get(cfg, f"/api/v1/jobs/{job_id}/lineage")}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def logs(level: str = "info", limit: int = 200) -> dict[str, Any]:
        """The coordinator's in-memory log ring: the last `limit` lines at or
        above `level` (debug, info, warning, error)."""
        return {"logs": _get(cfg, "/api/v1/logs", {"level": level, "limit": limit})}

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def queryable_state_lookup(job_id: int, role: str, slot: str, key: str) -> dict[str, Any]:
        """Point lookup into a running job's live keyed state: the current
        value for `key` in `slot` of operator `role`, served by whichever
        worker owns the key. A SQL GROUP BY binds its live aggregates to slot
        "agg" automatically."""
        return {
            "entry": _get(
                cfg,
                f"/api/v1/queryable_state/job/{job_id}/op/{role}/json/{slot}",
                {"key": key},
            )
        }

    @server.tool(annotations=READ_ONLY, structured_output=True)
    def queryable_state_scan(job_id: int, role: str, slot: str, limit: int = 100) -> dict[str, Any]:
        """Scan a running job's live keyed state for one slot across every
        subtask, up to `limit` entries. The response says whether it was
        truncated."""
        return {
            "scan": _get(
                cfg,
                f"/api/v1/queryable_state/job/{job_id}/op/{role}/json/{slot}/scan",
                {"limit": limit},
            )
        }

    # ------------------------------------------------------------------ prompt
    @server.prompt()
    def diagnose_incident(checkpoint_dir: str, capture_dir: str, symptom: str) -> str:
        """A diagnosis plan for a clink job whose output looks wrong, over its
        checkpoint directory and capture tree."""
        return (
            f"A clink job produced output that looks wrong: {symptom}\n\n"
            f"Its checkpoint directory is {checkpoint_dir} and its record-capture tree is "
            f"{capture_dir}. Diagnose it with the clink tools, read-only, in this order:\n"
            "1. checkpoint_verify on the checkpoint directory. Stop if it reports damage.\n"
            "2. capture_cat with capture_dir to list the captured operators and epochs; note "
            "which epochs are truncated.\n"
            "3. state_diff between consecutive checkpoint ids until the key whose state moved "
            "unexpectedly appears, or state_query over one checkpoint with SQL when the "
            "symptom names a key.\n"
            "4. capture_cat on the epoch file that spans the change, and find the record(s) "
            "responsible.\n"
            "5. replay that operator over that epoch with out set, and show the emission that "
            "carries the wrong value.\n"
            "6. If asked to keep the incident as a test, replay with emit_test.\n"
            "Report what you found, the record or records responsible, and the exact tool "
            "calls a reviewer can repeat. Do not propose changing the running job; that is "
            "the operator's decision."
        )

    return server
