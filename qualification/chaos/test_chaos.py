#!/usr/bin/env python3
"""Functional tests for the chaos controller, with no rig.

Every defect this controller has had was found by running it against paid
infrastructure, one per provision-build-run cycle, because a campaign is a
serial pipeline and each run buys exactly one fact. The controller had no
tests; the engine it was testing had two thousand. This closes that.

The seam is Rig.ssh. A FakeRig simulates the hosts - container up/down,
which image each came back on, and command failures - so every fault path
can be driven, including the ones that only appear when something goes
wrong. The coordinator is a stub too, so the liveness gate is exercised
without a cluster.

Run:  python3 qualification/chaos/test_chaos.py
"""
import json
import os
import random
import sys
import tempfile
import types

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import chaos as C  # noqa: E402


class FakeResult:
    def __init__(self, stdout="", returncode=0):
        self.stdout = stdout
        self.stderr = ""
        self.returncode = returncode


class FakeRig:
    """A rig in a dict. Containers have a running flag and an image."""

    def __init__(self, inventory_path=None, key_file=None):
        self.inv = {"hosts": [
            {"name": "ops", "role": "ops", "public_ip": "10.0.0.1", "private_ip": "10.0.0.1"},
            {"name": "coord", "role": "coordinator", "public_ip": "10.0.0.2",
             "private_ip": "10.0.0.2"},
            {"name": "w1", "role": "worker", "public_ip": "10.0.0.3", "private_ip": "10.0.0.3"},
            {"name": "w2", "role": "worker", "public_ip": "10.0.0.4", "private_ip": "10.0.0.4"},
            {"name": "b1", "role": "broker", "public_ip": "10.0.0.5", "private_ip": "10.0.0.5"},
        ]}
        self.key_file = key_file
        # host name -> {container: {"up": bool, "image": str, "pid": int}}
        self.state = {
            "coord": {"clink-coordinator": {"up": True, "image": "clink:qual", "pid": 101}},
            "w1": {"clink-worker": {"up": True, "image": "clink:qual", "pid": 201}},
            "w2": {"clink-worker": {"up": True, "image": "clink:qual", "pid": 202}},
            "b1": {"redpanda": {"up": True, "image": "redpanda:v24", "pid": 301}},
            "ops": {},
        }
        self.next_pid = 1000
        self.commands = []
        # Command substrings that should fail, to drive the error paths.
        self.fail_on = []
        # If set, a `compose up` brings the container back on this image
        # instead of the one it went down with.
        self.restart_image = None
        # If set, `compose up` does not bring the container back at all.
        self.restart_broken = False
        # Simulate a runtime policy that wrongly replaces every worker when
        # the coordinator is restarted.
        self.restart_workers_with_coordinator = False

    def hosts(self, role):
        return [h for h in self.inv["hosts"] if h["role"] == role]

    def ssh(self, host, command):
        self.commands.append((host["name"], command))
        for pat in self.fail_on:
            if pat in command:
                raise C.ChaosCommandFailed(f"{host['name']}: `{command}` exited 1: simulated")
        conts = self.state.setdefault(host["name"], {})

        if "docker inspect" in command and ".State.Pid" in command:
            for n, details in conts.items():
                if n in command:
                    return FakeResult(str(details["pid"] if details["up"] else 0))
            return FakeResult("0")

        if "docker ps" in command:
            name = None
            for n in conts:
                if f"name={n}" in command:
                    name = n
                    break
            if name is None or not conts[name]["up"]:
                return FakeResult("")
            if "{{.Image}}" in command:
                return FakeResult(f"{name} {conts[name]['image']}")
            if "{{.Status}}" in command:
                return FakeResult("Up 3 minutes")
            return FakeResult(name)

        if "docker kill" in command:
            for n in conts:
                if n in command:
                    conts[n]["up"] = False
                    conts[n]["pid"] = 0
            return FakeResult("")

        if "docker compose" in command and "up -d" in command:
            target = ("clink-coordinator" if "coordinator.yml" in command
                      else "clink-worker" if "worker.yml" in command else None)
            if target and not self.restart_broken:
                conts.setdefault(target, {"up": False, "image": "clink:qual", "pid": 0})
                conts[target]["up"] = True
                self.next_pid += 1
                conts[target]["pid"] = self.next_pid
                if self.restart_image:
                    conts[target]["image"] = self.restart_image
                if target == "clink-coordinator" and self.restart_workers_with_coordinator:
                    for worker_host in ("w1", "w2"):
                        self.next_pid += 1
                        self.state[worker_host]["clink-worker"]["pid"] = self.next_pid
            return FakeResult("")

        if "docker restart" in command:
            for n in conts:
                if n in command:
                    conts[n]["up"] = True
                    self.next_pid += 1
                    conts[n]["pid"] = self.next_pid
            return FakeResult("")

        return FakeResult("")


def make_chaos(rig, log_path, state=("RUNNING", 10)):
    ch = C.Chaos(rig, "http://stub:8095", "1", log_path, "run-test", random.Random(7))
    ch.job_state = lambda: state           # type: ignore[assignment]
    ch.await_healthy_checkpoint = lambda since, timeout_s=600: since + 1  # type: ignore
    ch._fault_surface = False              # never arm a real fault point
    return ch


RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, ok, detail))
    print(f"  {'PASS' if ok else 'FAIL'} {name}" + (f" - {detail}" if not ok and detail else ""))


def read_log(path):
    if not os.path.exists(path):
        return []
    return [json.loads(l) for l in open(path) if l.strip()]


def test_worker_kill_records_and_restores(tmp):
    rig = FakeRig()
    log = os.path.join(tmp, "a.jsonl")
    ch = make_chaos(rig, log)
    ch.kill_worker("RUNNING", 5)
    kinds = [e["fault"] for e in read_log(log)]
    check("worker kill records the kill and the restart",
          "worker_sigkill" in kinds and "worker_restart" in kinds, str(kinds))
    up = [c["clink-worker"]["up"] for h, c in rig.state.items() if "clink-worker" in c]
    check("worker kill leaves every worker running afterwards", all(up), str(up))


def test_a_kill_that_does_not_land_raises(tmp):
    """The fabricated-fault defect: every command timed out against a
    firewalled address while the log recorded the faults as applied."""
    rig = FakeRig()
    rig.fail_on = ["docker kill"]
    ch = make_chaos(rig, os.path.join(tmp, "b.jsonl"))
    try:
        ch.kill_worker("RUNNING", 5)
        check("a kill whose command fails raises", False, "no exception")
    except C.ChaosCommandFailed:
        check("a kill whose command fails raises", True)


def test_a_worker_that_does_not_come_back_raises(tmp):
    rig = FakeRig()
    rig.restart_broken = True
    ch = make_chaos(rig, os.path.join(tmp, "c.jsonl"))
    try:
        ch.kill_worker("RUNNING", 5)
        check("a worker that never returns raises", False, "no exception")
    except C.ChaosCommandFailed:
        check("a worker that never returns raises", True)


def test_a_worker_back_on_a_different_image_raises(tmp):
    """The near miss: a restart that lost its environment brought a worker
    back on the published image instead of the build under test. Had it
    merely started, the campaign would have measured two versions at once."""
    rig = FakeRig()
    ch = make_chaos(rig, os.path.join(tmp, "d.jsonl"))
    ch.kill_worker("RUNNING", 5)          # first sighting records the image
    rig.restart_image = "ghcr.io/orhaugh/clink-runtime:main"
    try:
        ch.kill_worker("RUNNING", 6)
        check("a container back on a different image raises", False, "no exception")
    except C.ChaosCommandFailed as exc:
        check("a container back on a different image raises", "not clink:qual" in str(exc)
              or "came back as" in str(exc), str(exc)[:80])


def test_coordinator_kill_leaves_workers_running(tmp):
    """A coordinator fault must exercise in-process worker recovery. Restarting
    workers here would hide a regression behind container-level recovery."""
    rig = FakeRig()
    ch = make_chaos(rig, os.path.join(tmp, "e.jsonl"))
    ch.kill_coordinator("RUNNING", 9)
    up = [rig.state[h]["clink-worker"]["up"] for h in ("w1", "w2")]
    check("coordinator kill leaves workers running", all(up), str(up))
    kinds = [e["fault"] for e in read_log(os.path.join(tmp, "e.jsonl"))]
    check("coordinator kill does not record worker restarts",
          not any("worker_restart" in kind for kind in kinds), str(kinds))
    restart = next(e for e in read_log(os.path.join(tmp, "e.jsonl"))
                   if e["fault"] == "coordinator_restart")
    check("coordinator kill records stable worker PIDs",
          restart["worker_pids_before"] == restart["worker_pids_after"], str(restart))


def test_coordinator_kill_rejects_worker_pid_churn(tmp):
    rig = FakeRig()
    rig.restart_workers_with_coordinator = True
    ch = make_chaos(rig, os.path.join(tmp, "e2.jsonl"))
    try:
        ch.kill_coordinator("RUNNING", 9)
        check("coordinator kill rejects worker PID churn", False, "no exception")
    except C.ChaosCommandFailed as exc:
        check("coordinator kill rejects worker PID churn", "PID changed" in str(exc), str(exc))


def test_loop_survives_a_failed_fault_and_reports_zero(tmp):
    """Two defects at once: the controller died on its first failed fault and
    left an hour of undisturbed soak looking like a fault campaign, and a run
    that applies nothing must never exit zero."""
    rig = FakeRig()
    rig.fail_on = ["docker kill"]        # every fault fails
    log = os.path.join(tmp, "f.jsonl")
    inv = os.path.join(tmp, "inv.json")
    json.dump(rig.inv, open(inv, "w"))

    orig_rig, orig_sleep = C.Rig, C.time.sleep
    C.Rig = lambda *a, **k: rig          # type: ignore[assignment]
    C.time.sleep = lambda s: None        # type: ignore[assignment]
    orig_state = C.Chaos.job_state
    orig_await = C.Chaos.await_healthy_checkpoint
    C.Chaos.job_state = lambda self: ("RUNNING", 10)      # type: ignore[assignment]
    C.Chaos.await_healthy_checkpoint = lambda self, since, timeout_s=600: since + 1  # type: ignore
    argv = sys.argv
    sys.argv = ["chaos.py", "--inventory", inv, "--log", log,
                "--coordinator-url", "http://stub:8095", "--job-id", "1",
                "--run-id", "t", "--profile", "steady", "--duration-s", "600", "--seed", "3"]
    try:
        rc = C.main()
    finally:
        C.Rig, C.time.sleep, sys.argv = orig_rig, orig_sleep, argv
        C.Chaos.job_state = orig_state
        C.Chaos.await_healthy_checkpoint = orig_await

    check("a controller whose faults all fail exits non-zero", rc != 0, f"rc={rc}")
    entries = read_log(log)
    failed = [e for e in entries if e.get("fault") == "fault_failed"]
    check("each failed fault is recorded as evidence", len(failed) >= 1, str(len(failed)))
    # The contract is the CONSECUTIVE-failure ceiling, not a total. The
    # counter resets whenever a fault lands, so a mix of working and broken
    # faults legitimately records more than five before five fail in a row -
    # asserting on the total instead was this test being wrong, not the
    # controller.
    # The contract is recorded explicitly by the controller, so assert on
    # that rather than on log adjacency: other records (a twopc fault noting
    # its surface is unavailable) legitimately interleave with the failures.
    streak = [e.get("consecutive_failures") for e in failed]
    check("it stops on five consecutive failures, not on a total",
          rc == 2 and streak and max(streak) == 5 and streak[-1] == 5,
          f"rc={rc} streak={streak[-8:]}")


def main():
    print("chaos controller functional tests (no rig)")
    with tempfile.TemporaryDirectory() as tmp:
        test_worker_kill_records_and_restores(tmp)
        test_a_kill_that_does_not_land_raises(tmp)
        test_a_worker_that_does_not_come_back_raises(tmp)
        test_a_worker_back_on_a_different_image_raises(tmp)
        test_coordinator_kill_leaves_workers_running(tmp)
        test_coordinator_kill_rejects_worker_pid_churn(tmp)
        test_loop_survives_a_failed_fault_and_reports_zero(tmp)
    bad = [r for r in RESULTS if not r[1]]
    print(f"\n{len(RESULTS) - len(bad)}/{len(RESULTS)} passed")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
