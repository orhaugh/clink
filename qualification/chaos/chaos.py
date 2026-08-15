#!/usr/bin/env python3
"""The qualification chaos controller (QUAL-09).

Runs on the ops host, outside the clink failure domain, and applies real
infrastructure faults to the rig over SSH. Every action is recorded as one
JSON line with the state it observed BEFORE acting, so a defect found later
can be tied to the exact fault that produced it.

Design rules this encodes:

  * Faults are triggered against OBSERVABLE state, not arbitrary sleeps. A
    worker kill waits until the coordinator reports the job RUNNING and a
    checkpoint has completed since the last fault, so "killed during steady
    state" means it, and the recovery clock starts from a known point.
  * Internal fault injection (CLINK_FAULT_INJECT) and external
    infrastructure faults are BOTH used: the first reaches 2PC windows no
    external kill can hit reliably, the second is the only thing that
    proves real process/infra loss. Neither substitutes for the other.
  * Every action records checkpoint_id and the pre-fault state, so a
    campaign's timeline is reconstructible from the log alone.
  * The controller never touches anything unlabelled: hosts come from the
    inventory file the campaign driver wrote from `hcloud ... -l qual-run=`.

Usage:
  chaos.py --inventory /qual/inventory.json --log /qual/chaos.jsonl \
           --coordinator-url http://10.20.1.3:8095 --job-id 1 \
           [--profile steady|aggressive|twopc] [--duration-s N] [--seed N]
"""
import argparse
import json
import random
import subprocess
import sys
import time
import urllib.error
import urllib.request

SSH = ["ssh", "-n", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
       "-o", "BatchMode=yes"]


class ChaosCommandFailed(RuntimeError):
    """A fault could not be applied. Never swallowed: a controller that
    keeps going after a failed command manufactures evidence."""


class Rig:
    def __init__(self, inventory_path: str, key_file: str):
        with open(inventory_path) as f:
            self.inv = json.load(f)
        self.key_file = key_file

    def hosts(self, role: str):
        return [h for h in self.inv["hosts"] if h["role"] == role]

    def ssh(self, host: dict, command: str):
        """Run a command on a rig host and REFUSE to continue if it fails.

        Two things here were the difference between a campaign and a
        fiction. The address is the PRIVATE one, because the rig's
        firewall admits ssh from the operator's laptop and from the
        private network only - the ops host reaching a worker's public
        address times out, which is exactly what happened: every fault
        command silently timed out for a whole run while the log
        recorded them as applied, the job never missed a checkpoint, and
        the campaign would have published a fault-tolerance result for a
        cluster nothing had touched.

        And a non-zero exit is now an exception rather than a shrug. A
        chaos controller that cannot apply a fault must stop, not
        continue writing evidence about faults that did not happen.
        """
        cmd = SSH + ["-i", self.key_file, f"root@{host['private_ip']}", command]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            raise ChaosCommandFailed(
                f"{host['name']}: `{command}` exited {result.returncode}: "
                f"{(result.stderr or result.stdout).strip()[:300]}")
        return result


class Chaos:
    def __init__(self, rig: Rig, coordinator_url: str, job_id: str,
                 log_path: str, run_id: str, rng: random.Random):
        self.rig = rig
        self.coordinator_url = coordinator_url.rstrip("/")
        self.job_id = job_id
        self.log_path = log_path
        self.run_id = run_id
        self.rng = rng
        self._fault_surface = None
        # (host, container) -> the image it was first seen running, so a
        # restart that brings it back on a different build is caught.
        self.container_images: dict = {}

    # --- observation -----------------------------------------------------

    def job_state(self):
        """(status, latest_completed_checkpoint_id) or (None, None).

        GET /api/v1/jobs/:id returns a FLAT object - there is no "job"
        wrapper - and it gained an explicit `status` only alongside this
        controller. Both facts were learned the expensive way: the first
        version of this method unwrapped a "job" key that does not exist
        and read a "status" field that did not either, so the liveness
        gate below could never be true, no fault was ever applied, and a
        campaign would have burned its whole rig budget logging nothing.
        Liveness is therefore ALSO derived from fields that have always
        existed, so this works against a coordinator of either vintage.
        """
        try:
            with urllib.request.urlopen(
                    f"{self.coordinator_url}/api/v1/jobs/{self.job_id}", timeout=10) as r:
                job = json.load(r)
        except (urllib.error.URLError, OSError, ValueError):
            return None, None
        status = job.get("status")
        if not status:
            running = (bool(job.get("tasks"))
                       and not job.get("completion_signalled")
                       and not job.get("cancel_requested")
                       and not job.get("errors"))
            status = "RUNNING" if running else "NOT_RUNNING"
        return status, job.get("latest_completed_checkpoint_id", 0)

    def await_healthy_checkpoint(self, since_ckpt: int, timeout_s: int = 600):
        """Block until the job is running AND has completed a checkpoint
        newer than `since_ckpt`. Returns the new checkpoint id, or None on
        timeout - which is itself a finding, logged by the caller."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            status, ckpt = self.job_state()
            if status == "RUNNING" and ckpt and ckpt > since_ckpt:
                return ckpt
            time.sleep(5)
        return None

    # --- recording -------------------------------------------------------

    def record(self, target: str, fault: str, state_before, checkpoint_id,
               extra=None):
        entry = {
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "target": target,
            "fault": fault,
            "state_before": state_before,
            "checkpoint_id": checkpoint_id,
            "qualification_run_id": self.run_id,
        }
        if extra:
            entry.update(extra)
        with open(self.log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        print(f"chaos: {entry['fault']} -> {entry['target']} "
              f"(ckpt {checkpoint_id})", flush=True)

    # --- faults ----------------------------------------------------------

    def assert_container_running(self, host: dict, name: str):
        """Prove a killed process came back, AS THE BUILD UNDER TEST.

        A rig left one worker short makes every measurement after it a
        different experiment - and a rig where the worker came back on a
        different image is worse, because it still looks whole. That is not
        hypothetical: a restart that lost its environment brought a worker
        back on the published :main image instead of the campaign's, and
        only the fact that it also lost its coordinator address, and so
        exited, stopped the campaign from measuring a cluster running two
        versions of clink at once.

        So the image is recorded on the first sighting of each container
        and every later restart is checked against it.
        """
        image = None
        for _ in range(20):
            res = self.rig.ssh(
                host, f"docker ps --filter name={name} --format '{{{{.Names}}}} {{{{.Image}}}}'")
            out = (res.stdout or "").strip()
            if name in out:
                parts = out.split()
                image = parts[1] if len(parts) > 1 else "unknown"
                break
            time.sleep(3)
        if image is None:
            raise ChaosCommandFailed(f"{host['name']}: {name} did not come back after a restart")

        key = (host["name"], name)
        expected = self.container_images.get(key)
        if expected is None:
            self.container_images[key] = image
        elif image != expected:
            raise ChaosCommandFailed(
                f"{host['name']}: {name} came back as {image}, not {expected}. The rig is now "
                "running more than one build of clink and every measurement after this point "
                "would be of a cluster that does not exist anywhere else.")

    def assert_container_gone(self, host: dict, name: str):
        """Prove the container is actually down. `docker kill` on an
        already-dead or misnamed container is a no-op, and a fault that
        did not happen must never be recorded as one."""
        res = self.rig.ssh(host, f"docker ps --filter name={name} --format '{{{{.Names}}}}'")
        if name in (res.stdout or ""):
            raise ChaosCommandFailed(
                f"{host['name']}: {name} is still running after a kill - the fault did not land")

    def kill_worker(self, state, ckpt):
        host = self.rng.choice(self.rig.hosts("worker"))
        self.rig.ssh(host, "docker kill -s SIGKILL clink-worker")
        self.assert_container_gone(host, "clink-worker")
        self.record(host["name"], "worker_sigkill", state, ckpt)
        # Restart after a beat so the cluster can actually recover; the
        # campaign is about recovery, not permanent capacity loss.
        time.sleep(self.rng.uniform(5, 20))
        self.rig.ssh(host, "cd /qual && docker compose -f worker.yml up -d")
        self.assert_container_running(host, "clink-worker")
        self.record(host["name"], "worker_restart", "killed", ckpt)

    def kill_coordinator(self, state, ckpt):
        host = self.rig.hosts("coordinator")[0]
        self.rig.ssh(host, "docker kill -s SIGKILL clink-coordinator")
        self.assert_container_gone(host, "clink-coordinator")
        self.record(host["name"], "coordinator_sigkill", state, ckpt)
        time.sleep(self.rng.uniform(5, 15))
        self.rig.ssh(host, "cd /qual && docker compose -f coordinator.yml up -d")
        self.assert_container_running(host, "clink-coordinator")
        self.record(host["name"], "coordinator_restart", "killed", ckpt)

    def restart_broker(self, state, ckpt):
        host = self.rng.choice(self.rig.hosts("broker"))
        self.rig.ssh(host, "docker restart redpanda")
        self.record(host["name"], "broker_restart", state, ckpt)

    def network_latency(self, state, ckpt):
        host = self.rng.choice(self.rig.hosts("worker"))
        delay = self.rng.choice([50, 150, 400])
        dur = self.rng.uniform(30, 120)
        self.rig.ssh(host, f"tc qdisc add dev eth0 root netem delay {delay}ms 20ms")
        self.record(host["name"], "network_latency", state, ckpt,
                    {"delay_ms": delay, "duration_s": round(dur, 1)})
        time.sleep(dur)
        # `|| true` on the REMOVAL only: the qdisc may legitimately be
        # absent if the add was rolled back. Everything that APPLIES a
        # fault above is intolerant, so a fault can never be recorded
        # without having been applied.
        self.rig.ssh(host, "tc qdisc del dev eth0 root || true")
        self.record(host["name"], "network_latency_cleared", "delayed", ckpt)

    def packet_loss(self, state, ckpt):
        host = self.rng.choice(self.rig.hosts("worker"))
        loss = self.rng.choice([1, 5, 15])
        dur = self.rng.uniform(30, 120)
        self.rig.ssh(host, f"tc qdisc add dev eth0 root netem loss {loss}%")
        self.record(host["name"], "packet_loss", state, ckpt,
                    {"loss_pct": loss, "duration_s": round(dur, 1)})
        time.sleep(dur)
        self.rig.ssh(host, "tc qdisc del dev eth0 root || true")
        self.record(host["name"], "packet_loss_cleared", "lossy", ckpt)

    def partition_worker_from_coordinator(self, state, ckpt):
        host = self.rng.choice(self.rig.hosts("worker"))
        coord_ip = self.rig.hosts("coordinator")[0]["private_ip"]
        dur = self.rng.uniform(20, 90)
        self.rig.ssh(host, f"iptables -A OUTPUT -d {coord_ip} -j DROP")
        self.record(host["name"], "partition_from_coordinator", state, ckpt,
                    {"duration_s": round(dur, 1)})
        time.sleep(dur)
        self.rig.ssh(host, f"iptables -D OUTPUT -d {coord_ip} -j DROP || true")
        self.record(host["name"], "partition_healed", "partitioned", ckpt)

    def broker_unavailable(self, state, ckpt):
        """Every broker down briefly: the source and the transactional
        sink both lose their cluster, which is the condition the resume
        and in-doubt paths exist for."""
        dur = self.rng.uniform(15, 60)
        for host in self.rig.hosts("broker"):
            self.rig.ssh(host, "docker stop redpanda")
        self.record("all-brokers", "kafka_unavailable", state, ckpt,
                    {"duration_s": round(dur, 1)})
        time.sleep(dur)
        for host in self.rig.hosts("broker"):
            self.rig.ssh(host, "docker start redpanda")
        self.record("all-brokers", "kafka_restored", "down", ckpt)

    def fault_surface_present(self) -> bool:
        """Whether the deployed build has fault-injection points compiled
        in. A shipped runtime image does NOT: every CLINK_FAULT_POINT
        compiles to nothing, so arming CLINK_FAULT_INJECT is a silent
        no-op and a campaign would report 2PC-window faults it never
        injected. Read from the build's own capability manifest."""
        if self._fault_surface is None:
            host = self.rig.hosts("coordinator")[0]
            res = self.rig.ssh(
                host, "docker exec clink-coordinator clink --capabilities-json || true")
            try:
                doc = json.loads(res.stdout)
                self._fault_surface = bool(
                    (doc.get("build") or doc).get("fault_injection", False))
            except (ValueError, AttributeError):
                self._fault_surface = False
        return self._fault_surface

    def twopc_window_fault(self, state, ckpt):
        """Crash a worker INSIDE a named 2PC window, using the engine's
        own deterministic fault points - the windows an external kill
        cannot hit reliably. The worker is restarted with the armed
        variable, takes the fault on the next checkpoint, and comes back
        clean so the next fault is independent."""
        if not self.fault_surface_present():
            # Recorded, not skipped silently: a campaign whose evidence
            # omits this cannot tell "the 2PC windows survived" from "the
            # 2PC windows were never tested".
            self.record("cluster", "twopc_window_fault_unavailable", state, ckpt,
                        {"note": "the deployed build has no fault-injection surface "
                                 "(build it with CLINK_ENABLE_FAULT_INJECTION=ON); "
                                 "targeted 2PC-window faults were NOT injected"})
            # Fall back to a real external kill so the tick is not wasted.
            self.kill_worker(state, ckpt)
            return
        point = self.rng.choice([
            "sink.before_prepare",
            "sink.after_prepare",
            "coordinator.before_completed_marker",
            "coordinator.after_completed_marker",
            "sink.before_commit",
            "sink.after_external_commit",
        ])
        is_coordinator = point.startswith("coordinator.")
        host = (self.rig.hosts("coordinator")[0] if is_coordinator
                else self.rng.choice(self.rig.hosts("worker")))
        compose = "coordinator.yml" if is_coordinator else "worker.yml"
        service = "clink-coordinator" if is_coordinator else "clink-worker"
        self.record(host["name"], f"twopc_arm:{point}", state, ckpt)
        self.rig.ssh(host, f"docker kill -s SIGKILL {service}")
        self.rig.ssh(
            host,
            f"cd /qual && CLINK_FAULT_INJECT='{point}=exit:9@1' "
            f"docker compose -f {compose} up -d")
        # Let it take the fault, then bring it back WITHOUT the arm so the
        # next window is reached in a clean process.
        time.sleep(self.rng.uniform(30, 90))
        self.rig.ssh(host, f"cd /qual && docker compose -f {compose} up -d")
        self.record(host["name"], f"twopc_recovered:{point}", "armed", ckpt)


PROFILES = {
    # Weightings, not schedules: each pick is independent, so a campaign's
    # fault mix is reproducible from (profile, seed) alone.
    "steady": [
        ("kill_worker", 5), ("kill_coordinator", 1), ("restart_broker", 2),
        ("network_latency", 3), ("packet_loss", 2),
        ("partition_worker_from_coordinator", 2), ("broker_unavailable", 1),
        ("twopc_window_fault", 3),
    ],
    "aggressive": [
        ("kill_worker", 8), ("kill_coordinator", 3), ("restart_broker", 3),
        ("network_latency", 2), ("packet_loss", 2),
        ("partition_worker_from_coordinator", 3), ("broker_unavailable", 2),
        ("twopc_window_fault", 5),
    ],
    "twopc": [("twopc_window_fault", 1)],
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--log", required=True)
    ap.add_argument("--coordinator-url", required=True)
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--key-file", default="/root/.ssh/id_ed25519")
    ap.add_argument("--profile", default="steady", choices=sorted(PROFILES))
    ap.add_argument("--duration-s", type=int, default=0)
    ap.add_argument("--min-gap-s", type=int, default=120,
                    help="minimum settle time between faults; recovery must "
                         "have a chance to complete or the campaign measures "
                         "cascading failure rather than recovery")
    ap.add_argument("--seed", type=int, default=1)
    args = ap.parse_args()

    rig = Rig(args.inventory, args.key_file)
    rng = random.Random(args.seed)
    chaos = Chaos(rig, args.coordinator_url, args.job_id, args.log,
                  args.run_id, rng)

    weighted = []
    for name, weight in PROFILES[args.profile]:
        weighted.extend([name] * weight)

    started = time.time()
    last_ckpt = 0
    faults = 0
    consecutive_failures = 0
    while True:
        if args.duration_s and time.time() - started >= args.duration_s:
            break
        # Trigger against observable state: wait for a checkpoint newer
        # than the last fault's, so every fault lands in steady state and
        # the recovery it causes is attributable.
        ckpt = chaos.await_healthy_checkpoint(last_ckpt)
        if ckpt is None:
            chaos.record("job", "healthy_checkpoint_timeout", "unknown", last_ckpt,
                         {"note": "no new checkpoint within timeout - the job did "
                                  "not recover from the previous fault"})
            time.sleep(60)
            continue
        last_ckpt = ckpt
        status, _ = chaos.job_state()
        fault = rng.choice(weighted)
        # One fault that cannot be applied must not end the campaign's
        # fault generation. It did exactly that on QUAL-01: the first
        # worker kill was applied, its restart came back misconfigured and
        # exited, the assertion raised, and the controller died - leaving
        # an hour of "soak" with nothing touching the cluster and a
        # campaign that looked like it had been under fault the whole
        # time. Record the failure as evidence, keep going, and let the
        # consecutive-failure ceiling below decide when the rig itself is
        # too broken to be worth continuing against.
        try:
            getattr(chaos, fault)(status, ckpt)
            faults += 1
            consecutive_failures = 0
        except ChaosCommandFailed as exc:
            consecutive_failures += 1
            chaos.record("controller", "fault_failed", status, ckpt,
                         {"attempted": fault, "error": str(exc),
                          "consecutive_failures": consecutive_failures})
            print(f"chaos: {fault} could not be applied ({exc}); "
                  f"{consecutive_failures} in a row", flush=True)
            if consecutive_failures >= 5:
                print("chaos: FIVE FAULTS IN A ROW COULD NOT BE APPLIED - stopping. The rig "
                      "is not in a state this controller can act on, and continuing would "
                      "produce a soak with no faults in it.", flush=True)
                return 2
        time.sleep(args.min_gap_s + rng.uniform(0, args.min_gap_s))
    print(f"chaos: {faults} faults applied", flush=True)
    if faults == 0:
        # The failure mode this controller has already had once: a gate
        # that never fires looks exactly like a quiet campaign. Never
        # again silently.
        print("chaos: NO FAULTS WERE APPLIED - the campaign proved nothing. "
              "Check that the job is running and that the coordinator URL is "
              "reachable from the ops host.", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
