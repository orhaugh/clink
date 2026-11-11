#!/usr/bin/env python3
"""The audit-runner example's driver (see README.md; run.sh runs it).

Each scenario runs the destination-reconciliation example's record-wise job
on a fresh local cluster from the build, against Kafka and ClickHouse on
compose, keeps producing, and runs `clink-audit` (python/clink-audit) against
the running job: as `poll`, as repeated `run`, killed and started again, with
a dependency paused, with bounds that cannot be met. Each scenario then holds
the runner's reports and status against what it did to the job and against
an independent check that reads the source back from Kafka and the
destination with the administrator's own client.

Python 3 standard library only; Docker for the services.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
RECONCILE = HERE.parent / "destination-reconciliation"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(RECONCILE))
import reconcile_demo  # noqa: E402
from reconcile_demo import COMMANDS, Failed, logged  # noqa: E402

AUDIT_PASSWORD_ENV = "CLINK_AUDIT_DESTINATION_PASSWORD"


class Demo(reconcile_demo.Demo):
    scenario_module = "audit_scenarios"

    def __init__(self, args):
        super().__init__(args)
        # The interpreter with clink-audit, clink-reference and the Kafka
        # client installed (run.sh makes the virtual environment).
        self.audit_python = args.audit_python

    def runner_config(self, cluster, name="audit", **over):
        """A runner's configuration for this cluster's job, under the
        cluster's directory. Credentials are named, never written."""
        limits = {"poll_interval_s": 1, "retry_backoff_s": [3, 6], "max_attempts": 3,
                  "stale_after_s": 120, "backlog_warn": 20, "run_seconds": 600,
                  "query_timeout_ms": 10000}
        limits.update(over.pop("limits", {}))
        cfg = {
            "format": "clink-audit-config", "version": 1,
            "state_dir": str(cluster.dir / ("%s-state" % name)),
            "clink": str(self.bin / "clink"),
            "job": {"id": 1, "checkpoint_dir": str(cluster.dir / "checkpoints"),
                    "ha_dir": str(cluster.dir / "ha"), "capture_dir": str(cluster.dir / "capture"),
                    "start_at": "earliest"},
            "key": ["event_id"],
            "destination": {"host": "127.0.0.1", "port": self.services.ch_port, "user": "reconciler",
                            "password_env": AUDIT_PASSWORD_ENV},
            "reference": {
                "command": [self.reference_python, str(RECONCILE / "reference_payments.py"),
                            "--scope={scope}", "--out={out}",
                            "--bootstrap=127.0.0.1:%d" % self.services.kafka_port,
                            "--max-records={max_records}", "--deadline-s={deadline_s}"],
                # The job reads a topic created for it from its first offset.
                "when_run_start": ["--run-start-offset=ALL=0"]},
            "verify": {"profile": "consistency"},
            "selection": "sequential",
            "limits": limits,
        }
        for k, v in over.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
        path = cluster.dir / ("%s-config.json" % name)
        path.write_text(json.dumps(cfg, indent=1))
        return path

    def _audit_env(self):
        return dict(os.environ, **{AUDIT_PASSWORD_ENV: reconcile_demo.RECONCILE_PASSWORD,
                                   "PYTHONDONTWRITEBYTECODE": "1"})

    def audit(self, config, command="run", extra=(), timeout=900, label=""):
        """Runs `clink-audit <command>` to its end; returns (exit, output)."""
        args = [self.audit_python, "-m", "clink_audit", command, "--config=%s" % config, *extra]
        r = subprocess.run(logged(args, label or command), capture_output=True, text=True,
                           env=self._audit_env(), timeout=timeout)
        out = (r.stdout + r.stderr).strip()
        log = Path(config).with_name(Path(config).stem + "-runs.log")
        with open(log, "a") as f:
            f.write("$ %s\n%s\nexit %d\n\n" % (" ".join(args[2:]), out, r.returncode))
        return r.returncode, out

    def audit_spawn(self, config, command="poll", label=""):
        args = [self.audit_python, "-m", "clink_audit", command, "--config=%s" % config]
        log = open(Path(config).with_name(Path(config).stem + "-poll.log"), "a")
        return subprocess.Popen(logged(args, label or command), stdout=log, stderr=subprocess.STDOUT,
                                env=self._audit_env(), start_new_session=True)

    def status(self, config):
        code, out = self.audit(config, "status", ["--json"], label="status")
        try:
            return code, json.loads(out[out.index("{"):])
        except ValueError:
            raise Failed("clink-audit status did not answer JSON: %s" % out[-500:])

    @staticmethod
    def reports(state_dir):
        d = Path(state_dir) / "reports"
        return sorted((json.loads(p.read_text()) for p in d.glob("*.json")),
                      key=lambda r: r["checkpoint"]) if d.is_dir() else []

    @staticmethod
    def events(state_dir, kind=None):
        p = Path(state_dir) / "events.ndjson"
        out = [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []
        return [e for e in out if kind is None or e["kind"] == kind]

    @staticmethod
    def stop_poll(proc, timeout=120):
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
        try:
            return proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            return proc.wait()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--project", default="clink-audit-runner")
    ap.add_argument("--kafka-port", type=int, default=29392)
    ap.add_argument("--clickhouse-port", type=int, default=29300)
    ap.add_argument("--rate", type=int, default=200)
    ap.add_argument("--only", default="")
    ap.add_argument("--reference-python", required=True)
    ap.add_argument("--audit-python", required=True)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    demo = Demo(args)
    try:
        return demo.run()
    finally:
        (demo.results / "commands.json").write_text(json.dumps(COMMANDS, indent=2))
        if not args.keep:
            demo.services.down()


if __name__ == "__main__":
    sys.exit(main())
