#!/usr/bin/env python3
"""QUAL-09's controller obligations, tested without a rig.

The infra faults are the first whose LEFTOVERS poison everything after
them - a filler holding the state volume at ENOSPC, a worker clock left
stepped - so the controller grew a revert registry drained on any exit.
These tests drive the fault functions against a fake Rig that records
every ssh command, and pin:

  * each infra fault registers its revert BEFORE applying and clears it
    after its own in-band revert (the healthy path leaves nothing to
    drain);
  * drain_reverts runs whatever is outstanding and records it - the
    interrupted-mid-fault path;
  * a fault on the environment's skip list runs NO commands and leaves a
    record saying so (a silent skip would let a local PASS impersonate
    cloud coverage);
  * every fault's record carries its ENGAGEMENT evidence fields - an
    infra fault that cannot say whether it bit proves nothing;
  * the infra mandatory list keeps kill_worker first (the verification
    gate reads the worker-loss counter), drops skipped faults, and keeps
    the slowest fault last so a curtailed pre-pass loses repetition, not
    the campaign's subject.
"""
import json
import pathlib
import random
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "chaos"))
import chaos as chaos_mod  # noqa: E402

# The infra faults hold their windows open with real sleeps; a unit test
# does not wait out a 420s partition.
chaos_mod.time.sleep = lambda _s: None


class FakeResult:
    def __init__(self, stdout=""):
        self.stdout = stdout
        self.returncode = 0


class FakeRig:
    """Records every ssh command; answers date/df probes with a script."""

    def __init__(self):
        self.commands = []  # (host_name, command)
        self.clock_offset = 0  # applied to the worker host's date +%s

    def hosts(self, role):
        if role == "worker":
            return [{"name": "w0", "public_ip": "1.1.1.1", "private_ip": "10.0.0.4"}]
        if role == "ops":
            return [{"name": "ops", "public_ip": "1.1.1.2", "private_ip": "10.0.0.9"}]
        if role == "coordinator":
            return [{"name": "coord", "public_ip": "1.1.1.3", "private_ip": "10.0.0.2"}]
        return []

    def ssh(self, host, command):
        self.commands.append((host["name"], command))
        if "date +%s" in command and "date -s" not in command:
            base = 1_000_000
            return FakeResult(str(base + (self.clock_offset if host["name"] == "w0" else 0)))
        if command.startswith("date -s") or " date -s" in command:
            return FakeResult("")
        if "df -B1" in command:
            return FakeResult("42")
        return FakeResult("")


def make_chaos(rig, log_path, metric_values=None):
    c = chaos_mod.Chaos(rig, "http://coord:8095", "1", str(log_path), "test",
                        random.Random(7), twopc_points=("p",))
    vals = list(metric_values or [0.0, 0.0])
    c.coordinator_metric_ = lambda _name: vals.pop(0) if vals else 0.0
    return c


def records(log_path):
    out = []
    p = pathlib.Path(log_path)
    if not p.exists():
        return out
    for line in p.read_text().splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


failures = []


def check(name, cond, detail=""):
    if cond:
        print(f"  PASS {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


with tempfile.TemporaryDirectory() as tmp:
    log = pathlib.Path(tmp) / "chaos.jsonl"

    # --- disk_pressure: revert in-band, engagement recorded ---------------
    rig = FakeRig()
    c = make_chaos(rig, log, metric_values=[3.0, 5.0])  # failed ckpts 3 -> 5
    c.disk_pressure("RUNNING", 9)
    cmds = [cmd for _, cmd in rig.commands]
    check("disk_pressure fills with the named filler",
          any(".chaos-fill" in cmd and "dd " in cmd for cmd in cmds))
    check("disk_pressure removes the filler in-band",
          any(cmd.startswith("rm -f /qual/state/.chaos-fill") for cmd in cmds))
    check("disk_pressure leaves nothing to drain", not c.pending_reverts,
          f"pending={c.pending_reverts}")
    rel = [r for r in records(log) if r["fault"] == "disk_pressure_released"]
    check("disk_pressure records engagement from the failed-ckpt counter",
          rel and rel[0]["engaged"] is True and rel[0]["ckpt_failed_after"] == 5.0)

    # --- clock_step: skip list is loud, never silent -----------------------
    log2 = pathlib.Path(tmp) / "chaos2.jsonl"
    rig2 = FakeRig()
    c2 = make_chaos(rig2, log2)
    c2.skip_faults = {"clock_step"}
    c2.clock_step("RUNNING", 9)
    check("a skipped clock_step runs no command", not rig2.commands,
          f"ran={rig2.commands}")
    skipped = [r for r in records(log2) if r["fault"] == "clock_step_skipped"]
    check("a skipped clock_step is recorded", bool(skipped))

    # --- clock_step: steps, measures, restores -----------------------------
    log3 = pathlib.Path(tmp) / "chaos3.jsonl"
    rig3 = FakeRig()
    c3 = make_chaos(rig3, log3)
    orig_ssh = rig3.ssh

    def stepping_ssh(host, command):
        res = orig_ssh(host, command)
        if "date -s @" in command and "+" in command:
            rig3.clock_offset = 60
        if "makestep" in command or "timesyncd" in command:
            rig3.clock_offset = 0
        return res

    rig3.ssh = stepping_ssh
    c3.clock_step("RUNNING", 9)
    cmds3 = [cmd for _, cmd in rig3.commands]
    check("clock_step steps the clock", any("date -s @" in cmd for cmd in cmds3))
    check("clock_step resyncs afterwards",
          any("makestep" in cmd or "timesyncd" in cmd for cmd in cmds3))
    check("clock_step leaves nothing to drain", not c3.pending_reverts)
    stepped = [r for r in records(log3) if r["fault"] == "clock_step"]
    restored = [r for r in records(log3) if r["fault"] == "clock_restored"]
    check("clock_step records the measured offset as engagement",
          stepped and stepped[0]["engaged"] is True
          and stepped[0]["measured_offset_during"] == 60)
    check("clock_restored records the post-resync offset",
          restored and restored[0]["measured_offset_after"] == 0)

    # --- partition_sustained: engagement from the loss counter -------------
    log4 = pathlib.Path(tmp) / "chaos4.jsonl"
    rig4 = FakeRig()
    c4 = make_chaos(rig4, log4, metric_values=[1.0, 2.0])  # lost 1 -> 2
    c4.sustained_partition_s = 5
    orig4 = rig4.ssh

    def docker_up_ssh(host, command):
        res = orig4(host, command)
        if "docker ps" in command:
            return FakeResult("up")
        return res

    rig4.ssh = docker_up_ssh
    c4.partition_sustained("RUNNING", 9)
    cmds4 = [cmd for _, cmd in rig4.commands]
    check("partition_sustained applies the DROP rule",
          any("iptables -A OUTPUT" in cmd for cmd in cmds4))
    check("partition_sustained removes the rule in-band",
          any("iptables -D OUTPUT" in cmd for cmd in cmds4))
    check("partition_sustained leaves nothing to drain", not c4.pending_reverts)
    healed = [r for r in records(log4) if r["fault"] == "partition_sustained_healed"]
    check("partition_sustained records loss-counter engagement and the rejoin",
          healed and healed[0]["engaged"] is True and healed[0]["rejoined"] is True)

    # --- drain_reverts: the interrupted-mid-fault path ----------------------
    log5 = pathlib.Path(tmp) / "chaos5.jsonl"
    rig5 = FakeRig()
    c5 = make_chaos(rig5, log5)
    ops = rig5.hosts("ops")[0]
    c5.register_revert_(ops, "rm -f /qual/state/.chaos-fill", "disk_pressure")
    c5.register_revert_(ops, "chronyc -a makestep", "clock_step")
    c5.drain_reverts()
    cmds5 = [cmd for _, cmd in rig5.commands]
    check("drain runs every outstanding revert",
          any("rm -f /qual/state/.chaos-fill" in cmd for cmd in cmds5)
          and any("makestep" in cmd for cmd in cmds5))
    drained = [r for r in records(log5) if r["fault"] == "revert_drained"]
    check("drain records what it reverted",
          {d["reverted"] for d in drained} == {"disk_pressure", "clock_step"})
    check("drain empties the registry", not c5.pending_reverts)

    # --- the infra mandatory list -------------------------------------------
    m = chaos_mod.mandatory_faults_infra(("p1", "p2"), set())
    check("infra mandatory leads with kill_worker", m[0] == "kill_worker")
    check("infra mandatory covers all three infra faults",
          {"disk_pressure", "clock_step", "partition_sustained"} <= set(m))
    check("the slowest infra fault sits last of the three",
          m.index("partition_sustained") > m.index("disk_pressure")
          and m.index("partition_sustained") > m.index("clock_step"))
    m2 = chaos_mod.mandatory_faults_infra(("p1",), {"clock_step"})
    check("a skipped fault leaves the mandatory list", "clock_step" not in m2)

print(f"\n{len(failures)} failure(s)" if failures else "\nall controller checks passed")
sys.exit(1 if failures else 0)
