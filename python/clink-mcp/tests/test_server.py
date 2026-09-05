"""clink-mcp end to end: a real incident, a real server over stdio, a real client.

The fixture is the walkthrough's incident in miniature: a running total per
user over a file of orders in which one order carries a fat-fingered amount.
The job runs embedded with checkpointing and the flight recorder armed, then
the server is started over stdio exactly as an MCP client would start it, and
every tool is exercised against what the run left behind. Skips unless
CLINK_BIN names a clink binary, because the server is a thin layer over it.
"""

from __future__ import annotations

import json
import os
import random
import subprocess
import sys
from pathlib import Path

import anyio
import pytest
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

CLINK_BIN = os.environ.get("CLINK_BIN")

pytestmark = pytest.mark.skipif(
    not CLINK_BIN or not os.path.exists(CLINK_BIN),
    reason="CLINK_BIN must name a built clink binary",
)

FAT_FINGER = 4_990_000  # meant to be 4,990


@pytest.fixture(scope="module")
def incident(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Path]:
    """Run the incident job once per module and hand back its artefacts."""
    root = tmp_path_factory.mktemp("incident")
    rng = random.Random(7)
    users = ["ana", "bo", "chen", "dana", "eli", "fay", "gus", "hana"]
    rows = []
    for sec in range(600):
        for u in users:
            rows.append(
                {"usr": u, "ts": sec * 1000 + rng.randint(0, 999), "amount": rng.randint(5, 60)}
            )
    rows.append({"usr": "dana", "ts": 421_420, "amount": FAT_FINGER})
    rows.sort(key=lambda r: r["ts"])
    orders = root / "orders.ndjson"
    orders.write_text("".join(json.dumps(r) + "\n" for r in rows))
    spend = root / "spend.ndjson"
    job = root / "job.sql"
    job.write_text(
        "CREATE TABLE orders (usr TEXT, ts BIGINT, amount BIGINT)\n"
        f"  WITH (connector='file', path='{orders}', format='json');\n"
        "CREATE TABLE spend (usr TEXT, total BIGINT)\n"
        f"  WITH (connector='file', path='{spend}', format='json');\n"
        "INSERT INTO spend SELECT usr, SUM(amount) AS total FROM orders GROUP BY usr;\n"
    )
    ckpt, capture = root / "ckpt", root / "capture"
    proc = subprocess.run(
        [
            CLINK_BIN,
            "run",
            str(job),
            f"--checkpoint-dir={ckpt}",
            "--checkpoint-interval-ms=100",
            f"--capture-dir={capture}",
            "--capture-records=100000",
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert spend.exists(), "the job wrote no sink output"
    agg_dirs = [
        d
        for d in capture.iterdir()
        if (d / "subtask-1" / "op.json").exists()
        and json.loads((d / "subtask-1" / "op.json").read_text())["op_type"] == "aggregate_row"
    ]
    assert len(agg_dirs) == 1, "expected exactly one captured aggregate operator"
    return {
        "root": root,
        "job": job,
        "ckpt": ckpt,
        "capture": capture,
        "agg": agg_dirs[0],
        "orders": orders,
    }


def call(tool: str, **arguments):
    """Start the server over stdio, call one tool, and return the CallToolResult."""

    async def go():
        params = StdioServerParameters(
            command=sys.executable, args=["-m", "clink_mcp", "--clink", CLINK_BIN]
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                return await session.call_tool(tool, arguments)

    return anyio.run(go)


def test_lists_every_tool_and_marks_only_replay_as_writing():
    async def go():
        params = StdioServerParameters(
            command=sys.executable, args=["-m", "clink_mcp", "--clink", CLINK_BIN]
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                init = await session.initialize()
                tools = (await session.list_tools()).tools
                prompts = (await session.list_prompts()).prompts
                return init.server_info.name, tools, prompts

    name, tools, prompts = anyio.run(go)
    assert name == "clink"
    names = {t.name for t in tools}
    assert {
        "clink_capabilities",
        "explain",
        "lint",
        "checkpoint_verify",
        "state_cat",
        "state_diff",
        "state_query",
        "capture_cat",
        "replay",
        "replay_diff",
        "search_file",
        "jobs",
        "job",
        "job_graph",
        "job_operators",
        "job_lineage",
        "cluster",
        "health",
        "logs",
        "queryable_state_lookup",
        "queryable_state_scan",
    } <= names
    writers = {
        t.name for t in tools if t.annotations is not None and t.annotations.read_only_hint is False
    }
    assert writers == {"replay"}, "every tool but replay is declared read-only"
    assert {p.name for p in prompts} == {"diagnose_incident"}


def test_capabilities_and_explain_need_no_cluster(incident):
    caps = call("clink_capabilities").structured_content
    assert caps["exit_code"] == 0
    assert "connectors" in caps["result"]
    plan = call("explain", sql=incident["job"].read_text()).structured_content
    assert plan["exit_code"] == 0
    assert "Aggregate" in plan["stdout"], plan


def test_lint_reports_an_ignored_setting():
    # An interval with no checkpoint directory is accepted and then ignored;
    # lint's whole purpose is to say so.
    out = call("lint", flags=["--checkpoint-interval-ms=100"]).structured_content
    text = (out["stdout"] + out["stderr"]).lower()
    assert "checkpoint" in text, out


def test_checkpoint_verify_and_state_reads(incident):
    verify = call("checkpoint_verify", checkpoint_dir=str(incident["ckpt"])).structured_content
    assert verify["exit_code"] == 0
    assert verify["result"]["invalid"] == 0 and verify["result"]["checked"] >= 1

    # The checkpoint ROOT is accepted; the server resolves the generation directory.
    cat = call(
        "state_cat", checkpoint_dir=str(incident["ckpt"]), checkpoint_id=1, max_rows=0
    ).structured_content
    assert cat["exit_code"] == 0, cat
    slots = {sl["slot"] for op in cat["result"]["operators"] for sl in op["slots"]}
    assert "agg" in slots
    keys = {
        e["key"] for op in cat["result"]["operators"] for sl in op["slots"] for e in sl["entries"]
    }
    assert any("dana" in k for k in keys)

    rows = call(
        "state_query",
        checkpoint_dir=str(incident["ckpt"]),
        checkpoint_id=1,
        sql="SELECT slot, user_key FROM state WHERE user_key LIKE '%dana%' LIMIT 5",
    ).structured_content
    assert rows["exit_code"] == 0, rows
    assert rows["row_count"] == 1 and rows["rows"][0]["slot"] == "agg"


def test_capture_finds_the_fat_fingered_record(incident):
    listing = call("capture_cat", capture_dir=str(incident["capture"])).structured_content
    assert listing["exit_code"] == 0
    assert incident["agg"].name in listing["stdout"]
    epoch = incident["agg"] / "subtask-1" / "epoch-1.cap"
    hits = call(
        "capture_cat", file=str(epoch), max_rows=0, grep=f'"amount":{FAT_FINGER}'
    ).structured_content
    assert hits["match_count"] == 1, hits
    assert '"usr":"dana"' in hits["matches"][0]["text"]


def test_replay_reproduces_the_jump_and_verifies(incident):
    out = incident["root"] / "emissions.ndjson"
    rep = call(
        "replay",
        capture_dir=str(incident["capture"]),
        checkpoint_dir=str(incident["ckpt"]),
        epoch="1",
        op=incident["agg"].name,  # the op-<id> directory name is accepted
        out=str(out),
    ).structured_content
    assert rep["exit_code"] == 0, rep
    assert out.exists()

    # dana's running total jumps by the fat-fingered amount in one emission: the
    # first emission at or above five million is the moment, and the ones before
    # it (any user's) are ordinary totals.
    found = call(
        "search_file",
        path=str(out),
        pattern=r'"total":[5-9]\d{6},"usr":"dana"',
        max_matches=1,
        context=1,
    ).structured_content
    assert found["matches"], found
    jump = found["matches"][0]
    assert json.loads(jump["text"])["total"] >= 5_000_000
    assert all(
        json.loads(c["text"])["total"] < 1_000_000
        for c in jump["context"]
        if c["line"] < jump["line"]
    )

    verify = call(
        "replay",
        capture_dir=str(incident["capture"]),
        checkpoint_dir=str(incident["ckpt"]),
        epoch="1",
        verify=True,
    ).structured_content
    assert verify["exit_code"] == 0 and "deterministic" in verify["stdout"], verify

    out2 = incident["root"] / "emissions2.ndjson"
    call(
        "replay",
        capture_dir=str(incident["capture"]),
        checkpoint_dir=str(incident["ckpt"]),
        epoch="1",
        op=incident["agg"].name,
        out=str(out2),
    )
    diff = call("replay_diff", a=str(out), b=str(out2)).structured_content
    assert diff["exit_code"] == 0 and "identical" in diff["stdout"], diff


def test_anticipated_failures_reach_the_client_verbatim():
    bad = call("state_cat")
    assert bad.is_error
    assert "state_cat needs" in bad.content[0].text
    no_cluster = call("jobs")
    assert no_cluster.is_error
    assert "no coordinator configured" in no_cluster.content[0].text
