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
import os
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
                 log_path: str, run_id: str, rng: random.Random,
                 twopc_points=None):
        self.rig = rig
        self.coordinator_url = coordinator_url.rstrip("/")
        self.job_id = job_id
        self.log_path = log_path
        self.run_id = run_id
        self.rng = rng
        # The 2PC points this CAMPAIGN can actually fire. The full class
        # list is the Kafka set; a campaign whose sink family lacks a point
        # (QUAL-02: the CommittingSink path has no commit receipt, so
        # sink.between_commit_and_receipt exists only in the Kafka sink)
        # must exclude it, or --ensure-coverage chases an unfireable point
        # and the summariser's required-coverage gate can never PASS.
        self.twopc_points = tuple(twopc_points) if twopc_points else self.TWOPC_POINTS
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

    def container_pid(self, host: dict, name: str):
        """Return a running container's host PID, refusing an absent/invalid value."""
        res = self.rig.ssh(host, f"docker inspect --format '{{{{.State.Pid}}}}' {name}")
        try:
            pid = int((res.stdout or "").strip())
        except ValueError as exc:
            raise ChaosCommandFailed(
                f"{host['name']}: {name} returned an invalid container PID") from exc
        if pid <= 0:
            raise ChaosCommandFailed(f"{host['name']}: {name} is not running")
        return pid

    def worker_pids(self):
        """Current worker container PIDs by host name. Captured before any
        coordinator disturbance so the stability gate has evidence to
        compare against, whatever code path caused the restart."""
        return {w["name"]: self.container_pid(w, "clink-worker")
                for w in self.rig.hosts("worker")}

    def assert_worker_pids_stable(self, before: dict, context: str):
        """Prove every worker survived `context` in process. This is THE
        stable-PID gate; every path that disturbs the coordinator must go
        through it. Run C's targeted coordinator faults bypassed it, so the
        campaign had no stable-PID evidence for exactly the events that
        went wrong."""
        after = {}
        for w in self.rig.hosts("worker"):
            self.assert_container_running(w, "clink-worker")
            pid = self.container_pid(w, "clink-worker")
            after[w["name"]] = pid
            if pid != before.get(w["name"]):
                raise ChaosCommandFailed(
                    f"{w['name']}: clink-worker PID changed across {context} "
                    f"({before.get(w['name'])} -> {pid}); the campaign did not "
                    "exercise in-process control-session recovery")
        return after

    def await_container_exit(self, host: dict, name: str, expect_code: int,
                             attempts: int = 60):
        """Block until `name` exits, and prove it exited with the ARMED
        fault's code. This is what makes a targeted fault real evidence: an
        armed point that is unreachable in the deployed pipeline leaves the
        process running forever, and before this check existed the campaign
        recorded such arms as coverage. sink.before_prepare was exactly
        that: armed four times against a kafka pipeline whose sink never
        fired it."""
        for _ in range(attempts):
            res = self.rig.ssh(
                host,
                f"docker inspect --format '{{{{.State.Status}}}} {{{{.State.ExitCode}}}}' "
                f"{name} || echo absent 0")
            out = (res.stdout or "").strip()
            if out.startswith("exited"):
                try:
                    code = int(out.split()[1])
                except (IndexError, ValueError):
                    code = -1
                if code != expect_code:
                    raise ChaosCommandFailed(
                        f"{host['name']}: {name} exited {code}, not the armed fault's "
                        f"{expect_code}; the process died for a different reason and the "
                        "targeted window was not exercised")
                return
            time.sleep(3)
        raise ChaosCommandFailed(
            f"{host['name']}: {name} never exited - the armed fault point did not fire "
            "and this fault has produced no coverage")

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
        worker_pids_before = self.worker_pids()
        self.rig.ssh(host, "docker kill -s SIGKILL clink-coordinator")
        self.assert_container_gone(host, "clink-coordinator")
        self.record(host["name"], "coordinator_sigkill", state, ckpt)
        time.sleep(self.rng.uniform(5, 15))
        self.rig.ssh(host, "cd /qual && docker compose -f coordinator.yml up -d")
        self.assert_container_running(host, "clink-coordinator")

        # Workers remain running. Each one fences and drains the dead control
        # session, then reconnects to this coordinator under its new epoch.
        # Restarting them here would conceal a regression in that contract.
        worker_pids_after = self.assert_worker_pids_stable(
            worker_pids_before, "coordinator restart")
        self.record(host["name"], "coordinator_restart", "killed", ckpt,
                    {"worker_pids_before": worker_pids_before,
                     "worker_pids_after": worker_pids_after})

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

    def pg_unavailable(self, state, ckpt):
        """The Postgres server frozen briefly - QUAL-02's decisive
        composition. A prepared transaction outlives its session, so a
        worker-loss recovery mid-outage meets a server that cannot answer
        COMMIT PREPARED or the open()-time reconcile; the sink must retry
        through the restart cycle and never resolve a gid blind. Pause,
        not stop: instant, and connections hang the way the local gate
        (APostgresOutageDuringRecoveryStaysExactlyOnce) models."""
        # Postgres rides the OPS host on the QUAL-02 rig (outside the clink
        # failure domain, next to the oracle). The fault targets the
        # container by name, so the host role is wherever compose put it.
        hosts = self.rig.hosts("postgres") or self.rig.hosts("ops")
        if not hosts:
            self.record("cluster", "pg_unavailable_no_host", state, ckpt,
                        {"note": "no postgres or ops host in this rig's "
                                 "inventory; fault skipped"})
            return
        # Capped at 40s: the verifier declares itself STUCK - a failed
        # campaign - after 10 consecutive failed samples (~50s with its 5s
        # retry cadence plus a 10s statement timeout). The fault must
        # never be able to fail the oracle on its own; the composition it
        # exists for (recovery meeting a dead server) forms well inside
        # 40 seconds, as the local gate proves in an 8s dwell.
        dur = self.rng.uniform(20, 40)
        for host in hosts:
            self.rig.ssh(host, "docker pause qual02-postgres")
        self.record("postgres", "pg_unavailable", state, ckpt,
                    {"duration_s": round(dur, 1)})
        time.sleep(dur)
        for host in hosts:
            self.rig.ssh(host, "docker unpause qual02-postgres")
        self.record("postgres", "pg_restored", "down", ckpt)

    def s3_unavailable(self, state, ckpt):
        """The object store frozen briefly - QUAL-03's decisive
        composition. A staged multipart upload outlives its uploader, so
        a worker-loss recovery mid-outage meets a store that cannot
        answer CompleteMultipartUpload for the restored handle; the sink
        must retry through the restart cycle and converge on the heal
        with the pane committed exactly once. Pause, not stop: instant,
        and connections hang the way the local gate
        (AnS3OutageDuringRecoveryStaysExactlyOnce) models."""
        # MinIO rides the OPS host on the QUAL-03 rig (outside the clink
        # failure domain, next to the oracle). The fault targets the
        # container by name, so the host role is wherever compose put it.
        hosts = self.rig.hosts("minio") or self.rig.hosts("ops")
        if not hosts:
            self.record("cluster", "s3_unavailable_no_host", state, ckpt,
                        {"note": "no minio or ops host in this rig's "
                                 "inventory; fault skipped"})
            return
        # Capped at 40s for the same oracle-budget reason as
        # pg_unavailable: the QUAL-03 verifier declares itself STUCK
        # after 10 consecutive failed samples, and this fault must never
        # be able to fail the oracle on its own. The composition it
        # exists for forms well inside 40 seconds, as the local gate
        # proves in an 8s dwell.
        dur = self.rng.uniform(20, 40)
        for host in hosts:
            self.rig.ssh(host, "docker pause qual03-minio")
        self.record("minio", "s3_unavailable", state, ckpt,
                    {"duration_s": round(dur, 1)})
        time.sleep(dur)
        for host in hosts:
            self.rig.ssh(host, "docker unpause qual03-minio")
        self.record("minio", "s3_restored", "down", ckpt)

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

    TWOPC_POINTS = (
        "sink.before_prepare",
        "sink.after_prepare",
        "coordinator.before_completed_marker",
        "coordinator.after_completed_marker",
        "sink.before_commit",
        # The ack window: broker committed, nothing durable recorded yet.
        # qual01-20260818d's duplicates rode exactly this gap; recovery must
        # prove the commit over the wire (idempotent re-EndTxn).
        "sink.between_commit_and_receipt",
        "sink.after_external_commit",
    )

    def twopc_window_fault(self, state, ckpt, point=None):
        """Crash a process INSIDE a named 2PC window, using the engine's
        own deterministic fault points - the windows an external kill
        cannot hit reliably.

        Arming requires a restart (CLINK_FAULT_INJECT is read at process
        start), so the sequence is inherently a double death: the running
        process is killed, its ARMED replacement recovers the job, runs
        until it reaches the point, and dies there; only then is the clean
        process started. The armed incarnation is therefore a short-lived
        ghost that redeploys, consumes input and leaves durable artefacts
        (checkpoints, markers, prepared transactions) behind - run C's
        corruption came out of exactly that window, so it is kept as a
        FEATURE of this fault, verified rather than accidental:

          * the armed process must EXIT WITH THE FAULT'S CODE, proving the
            named point actually fired (an unreachable point previously
            recorded coverage it never had);
          * a coordinator-side fault must leave every worker PID stable
            across all three coordinator incarnations, the same gate
            kill_coordinator enforces;
          * every phase is recorded, so the evidence distinguishes the
            kill, the ghost's death at the point, and the clean recovery.
        """
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
        if point is None:
            point = self.rng.choice(list(self.twopc_points))
        is_coordinator = point.startswith("coordinator.")
        compose = "coordinator.yml" if is_coordinator else "worker.yml"
        service = "clink-coordinator" if is_coordinator else "clink-worker"
        # Sink points only fire in a process that actually hosts a sink
        # subtask, and placement decides that - so a single randomly chosen
        # worker can be an inert arm (run d armed the same sinkless worker
        # twice). Walk the candidates until the point fires; a coordinator
        # point has exactly one candidate.
        if is_coordinator:
            candidates = [self.rig.hosts("coordinator")[0]]
        else:
            candidates = list(self.rig.hosts("worker"))
            self.rng.shuffle(candidates)
        worker_pids_before = self.worker_pids() if is_coordinator else None
        fired_host = None
        for host in candidates:
            self.record(host["name"], f"twopc_arm:{point}", state, ckpt)
            self.rig.ssh(host, f"docker kill -s SIGKILL {service}")
            self.assert_container_gone(host, service)
            self.rig.ssh(
                host,
                f"cd /qual && CLINK_FAULT_INJECT='{point}=exit:9@1' "
                f"docker compose -f {compose} up -d")
            fired = False
            try:
                # The armed incarnation recovers and must DIE AT THE POINT.
                # exit:9 observed is the proof; anything else (still
                # running, another exit code) means this process did not
                # exercise the window.
                self.await_container_exit(host, service, expect_code=9)
                fired = True
                self.record(host["name"], f"twopc_fired:{point}", "armed", ckpt,
                            {"exit_code": 9})
            except ChaosCommandFailed:
                # An inert candidate, not a failure of the fault: the point
                # lives in a process this host does not run (a sink subtask
                # placed elsewhere). Recorded below; the walk continues.
                pass
            finally:
                # THE ARM IS REMOVED ON EVERY PATH, fired or not, before
                # anything else can raise. Run d left a never-fired arm
                # running on a worker; a later redeploy placed a sink there,
                # the stale arm detonated mid-recovery of a DIFFERENT fault,
                # and the abort that followed also skipped the coordinator's
                # own unarmed restart - a decapitated cluster from one
                # missing cleanup. A short deliberate outage applies only
                # after a genuine fire; an inert candidate is restored
                # immediately.
                if fired:
                    time.sleep(self.rng.uniform(5, 15))
                self.rig.ssh(host, f"cd /qual && docker compose -f {compose} up -d")
                self.assert_container_running(host, service)
            if fired:
                fired_host = host
                break
            self.record(host["name"], f"twopc_arm_inert:{point}", "armed", ckpt,
                        {"note": "the point did not fire in this process "
                                 "(no hosting subtask); disarmed and restored"})
        if fired_host is None:
            raise ChaosCommandFailed(
                f"{point} did not fire on any candidate host - the armed fault point is "
                "unreachable in this deployment and has produced no coverage")
        extra = {}
        if worker_pids_before is not None:
            # The gate runs AFTER the unarmed restore above, so a violation
            # can no longer strand a dead coordinator; it still fails the
            # fault loudly.
            extra = {"worker_pids_before": worker_pids_before,
                     "worker_pids_after": self.assert_worker_pids_stable(
                         worker_pids_before, f"the {point} fault")}
        self.record(fired_host["name"], f"twopc_recovered:{point}", "armed", ckpt, extra)


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


def oracle_error_total(verdict_path: str):
    """The verifier's combined error count, or None when no verdict is
    readable yet. Any non-zero value means the campaign is ALREADY dirty:
    every further fault changes state that the diagnosis will need frozen,
    which is how run C spent ten more minutes mutating a broken cluster
    before anyone looked."""
    try:
        with open(verdict_path) as f:
            v = json.load(f)
    except (OSError, ValueError):
        return None
    return sum(int(v.get(k) or 0)
               for k in ("missing", "duplicate", "conflicting", "incorrect", "foreign"))


# Every fault a PASS verdict requires. The summariser refuses PASS unless
# each of these left evidence of having actually happened (for the 2PC
# points: twopc_fired, not merely twopc_arm).
# kill_worker MUST lead: the campaign's pre-soak verification gate
# confirms the first fault through the coordinator's worker-loss counter,
# and a worker kill's record and its engine-visible loss land together.
# qual01-smoke-b led with a 2PC point instead, whose arm RECORD precedes
# the fire (and the loss) by many seconds - the gate read that gap as the
# fabricated-fault defect and refused to soak. The 2PC points come second:
# each needs an arm, a commit actually passing the point, a process death
# and a full recovery - the slowest, most placement-sensitive coverage in
# the set - and qual01-smoke-a proved that scheduling them LAST runs a
# short soak out of clock with the commit-side points unfired. The quick
# infra kill/restarts close. Order is scheduling, not contract - the
# summariser's coverage gate is the contract.
def mandatory_faults(twopc_points):
    # kill_worker leads (the verification gate reads the worker-loss
    # counter), then the campaign's fireable 2PC points, then the infra
    # faults - the QUAL-01 smoke-b/c ordering lessons, kept per campaign.
    return ("kill_worker",) + tuple(f"twopc:{p}" for p in twopc_points) + (
        "kill_coordinator",
        "restart_broker",
        "network_latency",
        "partition_worker_from_coordinator",
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inventory", required=True)
    ap.add_argument("--log", required=True)
    ap.add_argument("--coordinator-url", required=True)
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--key-file", default="/root/.ssh/id_ed25519")
    ap.add_argument("--profile", default="steady", choices=sorted(PROFILES))
    ap.add_argument("--extra-faults", default="",
                    help="comma-separated extra fault actions this campaign "
                         "schedules (weight 2 each) and REQUIRES for "
                         "coverage - e.g. pg_unavailable for QUAL-02, whose "
                         "decisive composition is the external server down "
                         "during a recovery")
    ap.add_argument("--twopc-points", default="",
                    help="comma-separated 2PC points this campaign's sink "
                         "family can actually fire (default: the full Kafka "
                         "set). Coverage and the mandatory schedule follow "
                         "this list - naming an unfireable point makes PASS "
                         "unreachable, omitting a fireable one untests it.")
    ap.add_argument("--duration-s", type=int, default=0)
    ap.add_argument("--min-gap-s", type=int, default=120,
                    help="minimum settle time between faults; recovery must "
                         "have a chance to complete or the campaign measures "
                         "cascading failure rather than recovery")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--verdict", default="",
                    help="path to the verifier's verdict.json; when set, the "
                         "controller stops injecting the moment any oracle "
                         "error counter is non-zero (exit 3), freezing the "
                         "cluster for diagnosis")
    ap.add_argument("--stop-file", default="",
                    help="finish cleanly when this file appears (default: "
                         "<log>.stop). Checked between faults, never mid-"
                         "fault, so tc rules and armed points are always "
                         "cleared before exit. A file because the spawn "
                         "discipline starts this process with SIGINT "
                         "ignored; see qual01/verifier.py's docstring.")
    ap.add_argument("--ensure-coverage", action="store_true",
                    help="before the weighted-random loop, apply every "
                         "mandatory fault once in a fixed order, so a PASS "
                         "never depends on the dice having rolled them")
    args = ap.parse_args()

    rig = Rig(args.inventory, args.key_file)
    rng = random.Random(args.seed)
    twopc_points = (tuple(p for p in args.twopc_points.split(",") if p)
                    if args.twopc_points else None)
    chaos = Chaos(rig, args.coordinator_url, args.job_id, args.log,
                  args.run_id, rng, twopc_points=twopc_points)

    weighted = []
    for name, weight in PROFILES[args.profile]:
        weighted.extend([name] * weight)
    # Campaign-specific extras recur through the soak, not only in the
    # coverage pre-pass: QUAL-02's pg_unavailable must keep composing with
    # whatever the dice put next to it, exactly as QUAL-01's broker faults
    # did when they found the fencing defect.
    for name in (f for f in args.extra_faults.split(",") if f):
        weighted.extend([name] * 2)

    # The deterministic coverage pre-pass, then the weighted-random soak.
    # Run C became dirty before random selection ever reached the ordinary
    # kill_coordinator action; a verdict must never depend on that luck.
    schedule = []
    if args.ensure_coverage:
        # Extras go straight after the 2PC points, BEFORE the generic
        # infra faults. The order encodes priority: whatever sits at the
        # end is what a short or curtailed soak fails to cover, and a
        # campaign's decisive composition must not be the casualty.
        # QUAL-02's is pg_unavailable - the external transaction manager
        # down while a recovery needs it - which a 10-minute local soak
        # never reached while network_latency and a partition did.
        extra = tuple(f for f in args.extra_faults.split(",") if f)
        base = mandatory_faults(chaos.twopc_points)
        infra = ("kill_coordinator", "restart_broker", "network_latency",
                 "partition_worker_from_coordinator")
        head = tuple(f for f in base if f not in infra)
        tail = tuple(f for f in base if f in infra)
        for name in head + extra + tail:
            schedule.append(name)

    started = time.time()
    last_ckpt = 0
    faults = 0
    consecutive_failures = 0
    stop_file = args.stop_file or (args.log + ".stop")
    while True:
        if os.path.exists(stop_file):
            print("chaos: stop requested; finishing cleanly", flush=True)
            break
        if args.duration_s and time.time() - started >= args.duration_s:
            break
        if args.verdict:
            dirty = oracle_error_total(args.verdict)
            if dirty:
                chaos.record("oracle", "oracle_dirty_stop", "dirty", last_ckpt,
                             {"error_total": dirty,
                              "note": "the verifier reports oracle errors; no further "
                                      "faults will be injected so the cluster state "
                                      "stays diagnosable"})
                print(f"chaos: ORACLE DIRTY ({dirty} errors) - stopping fault "
                      "injection to freeze the evidence", flush=True)
                return 3
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
        scheduled = schedule.pop(0) if schedule else None
        fault = scheduled if scheduled is not None else rng.choice(weighted)
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
            if fault.startswith("twopc:"):
                chaos.twopc_window_fault(status, ckpt, point=fault.split(":", 1)[1])
            else:
                getattr(chaos, fault)(status, ckpt)
            faults += 1
            consecutive_failures = 0
        except ChaosCommandFailed as exc:
            consecutive_failures += 1
            if scheduled is not None:
                # A mandatory fault that could not be applied is retried at
                # the end of the pre-pass; a PASS without it is impossible
                # (the summariser's coverage gate), so giving up silently
                # here would only defer the failure to the verdict.
                schedule.append(scheduled)
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
