#!/usr/bin/env python3
"""The state-inspection example's driver (see README.md; run.sh runs it with the
Python of a virtual environment holding pyarrow and duckdb).

Each scenario runs the example job (accounts_job.cpp) on a local Coordinator
and Workers from the build against Kafka on compose, exports completed
checkpoints with `clink state-export --checkpoint-dir`, and reads, queries and
compares them with `clink state-query` and `clink state-diff` under network
isolation. Every dataset it relies on is checked against the expected state,
which this script computes on its own: it reads the source positions the
dataset records, reads those records back from Kafka, and folds them with its
own model of the job; it decodes the dataset's entries with its own knowledge
of the job's two codecs (an 8-byte little-endian int64 key and value).
"""
import argparse
import json
import os
import platform
import re
import shutil
import signal
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "examples" / "destination-reconciliation"))
sys.path.insert(0, str(REPO / "python" / "clink-audit"))
import duckdb  # noqa: E402
import pyarrow  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from clink_audit.isolation import Isolation, LoopbackIsolation  # noqa: E402
from reconcile_demo import COMMANDS, Failed, Services, free_port, logged  # noqa: E402

PARTITIONS = 2
OFFSET_KEY = b"__kafka_off__:"


# ---- Kafka ------------------------------------------------------------------------

class Kafka(Services):
    """The compose stack's Kafka alone."""

    def up(self):
        self.compose("up", "-d", "--wait", "kafka", timeout=900)

    def create(self, topic):
        self.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server=kafka:9092", "--create",
                   "--topic=%s" % topic, "--partitions=%d" % PARTITIONS, "--replication-factor=1")

    def send(self, topic, records):
        """records: (account, kind, amount); keyed by account, so one
        account's records are in one partition, in order."""
        text = "".join("%d\t%d,%s,%d\n" % (a, a, k, n) for a, k, n in records)
        self.kafka("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server=kafka:9092",
                   "--topic=%s" % topic, "--property", "parse.key=true", "--property",
                   "key.separator=\t", input_text=text)

    def send_json(self, topic, lines):
        self.kafka("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server=kafka:9092",
                   "--topic=%s" % topic, input_text="".join(line + "\n" for line in lines))

    def end_offsets(self, topic):
        r = self.kafka("/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server=kafka:9092",
                       "--topic=%s" % topic)
        out = {}
        for line in r.stdout.split():
            t, p, o = line.rsplit(":", 2)
            if t == topic:
                out[int(p)] = int(o)
        return out


# ---- the job's state, decoded and folded here ------------------------------------------

def int64(b):
    if len(b) != 8:
        raise Failed("an entry of %d bytes where the job's codec writes 8" % len(b))
    return struct.unpack("<q", b)[0]


def read_dataset(path):
    """dataset.json, the accounts' state {account: (count, balance)} and the
    source's positions {partition: next offset}, read with pyarrow.

    The operators are the ones the dataset names from the job graph when it
    holds the graph; a dataset of a finished job's last checkpoint does not
    (its manifest is gone), and then they are the operators that hold what
    this job's operators hold: the count and balance slots, and the Kafka
    positions."""
    doc = json.loads((path / "dataset.json").read_text())
    ops = {o["graph_id"]: int(o["op_id"]) for o in doc["operators"] if o.get("graph_id")}
    rows = pq.read_table(path / "entries.parquet").to_pylist()
    if "accounts" not in ops or "source" not in ops:
        holders = {}
        for r in rows:
            if r["scope"] == "keyed" and r["slot"] in ("count", "balance"):
                holders.setdefault("accounts", set()).add(r["op_id"])
            elif r["scope"] == "operator" and r["user_key"].startswith(OFFSET_KEY):
                holders.setdefault("source", set()).add(r["op_id"])
        for name, held in holders.items():
            if len(held) != 1:
                raise Failed("%s: %d operators hold the %s's state" % (path, len(held), name))
            ops.setdefault(name, held.pop())
    accounts, source = ops.get("accounts"), ops.get("source")
    slots = {}
    offsets = {}
    for r in rows:
        if r["op_id"] == accounts and r["scope"] == "keyed":
            slots.setdefault(int64(r["user_key"]), {})[r["slot"]] = int64(r["value"])
        elif r["op_id"] == source and r["scope"] == "operator" and r["user_key"].startswith(OFFSET_KEY):
            offsets[int(r["user_key"][len(OFFSET_KEY):])] = int64(r["value"])
    state = {a: (s.get("count"), s.get("balance")) for a, s in slots.items()}
    return doc, state, offsets


def apply(state, value, scale):
    parts = value.split(",")
    if len(parts) != 3:
        return
    try:
        account, kind, amount = int(parts[0]), parts[1], int(parts[2])
    except ValueError:
        return
    if account <= 0 or kind not in ("deposit", "withdraw", "close"):
        return
    if kind == "close":
        state.pop(account, None)
        return
    count, balance = state.get(account, (0, 0))
    state[account] = (count + 1, balance + (amount if kind == "deposit" else -amount) * scale)


def fold(by_partition, scale):
    state = {}
    for values in by_partition.values():
        for v in values:
            apply(state, v, scale)
    return state


def expected_changes(a, b):
    """Per slot, what a comparison of two states must find."""
    out = {}
    for slot, i in (("count", 0), ("balance", 1)):
        ka, kb = set(a), set(b)
        both = ka & kb
        out[slot] = {"added": len(kb - ka), "removed": len(ka - kb),
                     "changed": sum(1 for k in both if a[k][i] != b[k][i]),
                     "unchanged": sum(1 for k in both if a[k][i] == b[k][i])}
    return out


# ---- a cluster ------------------------------------------------------------------------

class Cluster:
    """A Coordinator and two Workers from the build, for one scenario."""

    def __init__(self, demo, name, topic, retained=100000, interval_ms=1000, schema=1):
        self.demo = demo
        self.name = name
        self.topic = topic
        self.dir = demo.work / name
        for d in ("ha", "checkpoints", "logs", "datasets", "probes"):
            (self.dir / d).mkdir(parents=True, exist_ok=True)
        self.control = free_port()
        self.http = free_port()
        self.procs = {}
        self.retained = retained
        self.interval_ms = interval_ms
        self.schema = schema
        self.next_worker = 1

    def env(self):
        return dict(os.environ, CLINK_SI_BROKERS="127.0.0.1:%d" % self.demo.kafka.kafka_port,
                    CLINK_SI_TOPIC=self.topic, CLINK_SI_SCHEMA=str(self.schema),
                    CLINK_SI_PARALLELISM="2")

    def spawn(self, name, args):
        log = open(self.dir / "logs" / ("%s.log" % name), "w")
        self.procs[name] = subprocess.Popen(
            logged([str(self.demo.bin / "clink_node"), *args], "%s %s" % (self.name, name)),
            stdout=log, stderr=subprocess.STDOUT, env=self.env())

    def await_http(self, port, what):
        import urllib.request
        for _ in range(200):
            try:
                urllib.request.urlopen("http://127.0.0.1:%d/api/v1/health" % port, timeout=1)
                return
            except Exception:
                time.sleep(0.1)
        raise Failed("%s did not come up" % what)

    def worker(self):
        n = self.next_worker
        self.next_worker += 1
        http = free_port()
        self.spawn("worker-%d" % n,
                   ["--role=worker", "--id=worker-%d" % n, "--coordinator-host=127.0.0.1",
                    "--coordinator-port=%d" % self.control, "--port=%d" % free_port(),
                    "--data-host=127.0.0.1", "--slots=16", "--http-port=%d" % http,
                    "--checkpoint-num-retained=%d" % self.retained])
        self.await_http(http, "worker-%d" % n)
        return "worker-%d" % n

    def start(self):
        self.spawn("coordinator", ["--role=coordinator", "--bind-host=127.0.0.1",
                                   "--port=%d" % self.control, "--http-port=%d" % self.http,
                                   "--ha-dir=%s" % (self.dir / "ha")])
        self.await_http(self.http, "the Coordinator")
        self.worker()
        self.worker()

    def submit(self, restore=None):
        args = [str(self.demo.bin / "clink"), "run", "--job=%s" % self.demo.job_so,
                "--coordinator-host=127.0.0.1", "--coordinator-port=%d" % self.control,
                "--name=%s" % self.name, "--checkpoint-dir=%s" % (self.dir / "checkpoints"),
                "--checkpoint-interval-ms=%d" % self.interval_ms, "--wait-timeout-s=0"]
        if restore is not None:
            args += ["--restore-from-dir=%s" % restore[0], "--restore-from-checkpoint-id=%d" % restore[1]]
        r = subprocess.run(logged(args, self.name), capture_output=True, text=True, timeout=120,
                           env=self.env())
        m = re.search(r'"job_id":(\d+)', r.stdout)
        if r.returncode != 0 or '"ok":true' not in r.stdout or not m:
            raise Failed("the submission was refused: %s" % (r.stdout + r.stderr)[-1500:])
        return int(m.group(1))

    def kill(self, name):
        self.procs[name].send_signal(signal.SIGKILL)
        self.procs[name].wait()

    def stop_worker(self, name):
        self.procs[name].send_signal(signal.SIGTERM)
        try:
            self.procs[name].wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.kill(name)

    def ctl(self, *args):
        r = subprocess.run(logged([str(self.demo.bin / "clink"), *args,
                                   "--coordinator-port=%d" % self.control], self.name),
                           capture_output=True, text=True, timeout=120)
        return r.stdout + r.stderr

    def markers(self, job):
        d = self.dir / "checkpoints" / "_jobs" / str(job)
        out = {}
        if d.is_dir():
            for p in d.iterdir():
                if p.name.startswith("COMPLETED-") and p.name[10:].isdigit():
                    try:
                        body = dict(line.split("=", 1) for line in p.read_text().split("\n") if "=" in line)
                    except OSError:
                        continue
                    out[int(p.name[10:])] = {"generation": int(body.get("generation", 1)),
                                             "subtasks": body.get("subtasks", "").split(",")}
        return out

    def await_marker(self, job, pred, what, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for cid, m in sorted(self.markers(job).items(), reverse=True):
                if pred(cid, m):
                    return cid
            time.sleep(0.2)
        raise Failed("no completed checkpoint %s" % what)

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs.values():
            try:
                p.wait(timeout=20)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()


class Feed(threading.Thread):
    """Produces batches in the background until stopped or out of records."""

    def __init__(self, kafka, topic, records, batch, pause):
        super().__init__(daemon=True)
        self.kafka, self.topic, self.records = kafka, topic, records
        self.batch, self.pause = batch, pause
        self.halt = threading.Event()
        self.sent = 0
        self.error = None

    def run(self):
        try:
            while self.sent < len(self.records) and not self.halt.is_set():
                chunk = self.records[self.sent:self.sent + self.batch]
                self.kafka.send(self.topic, chunk)
                self.sent += len(chunk)
                time.sleep(self.pause)
        except Exception as e:  # reported by the scenario
            self.error = e

    @property
    def running(self):
        return self.is_alive()


# ---- the demo -----------------------------------------------------------------------

def measure(argv, env=None):
    """Runs argv, returning (code, stdout, stderr, seconds, peak RSS bytes or None)."""
    timer = ["/usr/bin/time", "-l"] if platform.system() == "Darwin" else ["/usr/bin/time", "-v"]
    t0 = time.time()
    r = subprocess.run(timer + list(argv), capture_output=True, text=True, env=env, timeout=1800)
    seconds = time.time() - t0
    rss = None
    m = re.search(r"(\d+)\s+maximum resident set size", r.stderr)
    if m:
        rss = int(m.group(1))
    m = re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", r.stderr)
    if m:
        rss = int(m.group(1)) * 1024
    return r.returncode, r.stdout, r.stderr, seconds, rss


class Demo:
    def __init__(self, args):
        self.bin = Path(args.bin_dir).resolve()
        self.job_so = self.bin / "examples" / "state_inspection_job.so"
        self.work = Path(args.work).resolve()
        self.results = self.work / "results"
        for d in ("independent", "diffs", "queries"):
            (self.results / d).mkdir(parents=True, exist_ok=True)
        self.kafka = Kafka(args.project, args.kafka_port, free_port(), self.results)
        self.only = set(args.only.split(",")) if args.only else None
        self.outcomes = []
        self.costs = {}
        self.summary = open(self.results / "summary.txt", "a")
        self.iso = Isolation()
        self.loopback = LoopbackIsolation()

    # -- reporting -----------------------------------------------------------------
    def say(self, text):
        print("\n==> %s" % text, flush=True)
        self.summary.write("\n==> %s\n" % text)
        self.summary.flush()

    def note(self, text):
        print("    %s" % text, flush=True)
        self.summary.write("    %s\n" % text)
        self.summary.flush()

    def outcome(self, scenario, check, required, observed, ok, detail=""):
        self.outcomes.append({"scenario": scenario, "check": check, "required": required,
                              "observed": observed, "passed": bool(ok), "detail": detail})
        self.note("%s %s: required %s, observed %s%s" % ("PASS" if ok else "FAIL", check, required,
                                                          observed, (" (" + detail + ")") if detail else ""))
        return ok

    # -- the commands ----------------------------------------------------------------
    def clink(self, *args):
        return [str(self.bin / "clink"), *[str(a) for a in args]]

    def offline(self, argv, where, loopback=False):
        """A command under network isolation, proven at the start of the run;
        with loopback, under the mechanism that leaves loopback alone."""
        iso = self.loopback if loopback else self.iso
        if not iso.enforced:
            raise Failed("network isolation is %s: %s" % (iso.state, iso.detail))
        return subprocess.run(iso.wrap(logged(argv, where + (" (offline, loopback only)" if loopback
                                                             else " (offline)"))),
                              capture_output=True, text=True, timeout=1800)

    def export(self, c, job, out, checkpoint=None, extra=()):
        """state-export; returns (exit code, its JSON)."""
        args = self.clink("state-export", "--checkpoint-dir=%s" % (c.dir / "checkpoints"),
                          "--job=%d" % job, "--ha-dir=%s" % (c.dir / "ha"), "--out=%s" % out,
                          "--json", *extra)
        args.append("--checkpoint=%d" % checkpoint if checkpoint else "--restore-point")
        r = subprocess.run(logged(args, c.name), capture_output=True, text=True, timeout=1800)
        try:
            j = json.loads(r.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            j = {"unparsed": r.stdout[-500:] + r.stderr[-500:]}
        return r.returncode, j

    def caught_up(self, c, job, label, timeout=120):
        """Exports the restore point until it has read everything the topic
        holds; returns that dataset."""
        end = self.kafka.end_offsets(c.topic)
        deadline = time.time() + timeout
        n = 0
        while time.time() < deadline:
            probe = c.dir / "probes" / ("%s-%d" % (label, n))
            n += 1
            code, _ = self.export(c, job, probe)
            if code == 0:
                _, _, offsets = read_dataset(probe)
                if all(offsets.get(p, 0) == e for p, e in end.items()):
                    final = c.dir / "datasets" / label
                    probe.rename(final)
                    return final
            shutil.rmtree(probe, ignore_errors=True)
            time.sleep(0.5)
        raise Failed("the job did not catch up with %s (%s)" % (c.topic, end))

    def check_state(self, scenario, label, path, topic, scale=1):
        """The dataset's state is what the job's records up to its own
        positions fold to, reading the records back from Kafka."""
        doc, state, offsets = read_dataset(path)
        back = {}
        short = []
        for p in range(PARTITIONS):
            want = offsets.get(p, 0)
            got = [v for _, v in self.kafka.read_range(topic, p, 0, want)]
            if len(got) != want:
                short.append("partition %d: %d of %d records read back" % (p, len(got), want))
            back[p] = got
        expected = fold(back, scale)
        diffs = sorted(set(expected) ^ set(state)) + sorted(a for a in set(expected) & set(state)
                                                             if expected[a] != state[a])
        result = {"dataset": str(path), "checkpoint": doc["checkpoint"]["id"],
                  "complete": doc["complete"], "offsets": offsets,
                  "records_folded": sum(len(v) for v in back.values()), "accounts": len(state),
                  "accounts_expected": len(expected), "differing_accounts": diffs[:20],
                  "short_read": short}
        (self.results / "independent" / ("%s-%s.json" % (scenario, label))).write_text(
            json.dumps(result, indent=2))
        return self.outcome(scenario, "%s holds exactly the state the records up to its positions fold to"
                            % label, "complete, %d accounts, no difference" % len(expected),
                            "complete %s, %d accounts, %d differing%s" % (
                                doc["complete"], len(state), len(diffs), (", " + "; ".join(short)) if short else ""),
                            doc["complete"] and not diffs and not short and bool(offsets))

    def diff(self, scenario, label, a, b, extra=()):
        r = self.offline(self.clink("state-diff", "--a=%s" % a, "--b=%s" % b, "--json", *extra), scenario)
        (self.results / "diffs" / ("%s-%s.json" % (scenario, label))).write_text(r.stdout or r.stderr)
        try:
            return r.returncode, json.loads(r.stdout)
        except ValueError:
            return r.returncode, None

    @staticmethod
    def slot(report, name):
        return next((s for s in report["slots"] if s["slot"] == name), None)

    def query(self, scenario, label, dataset, sql, extra=()):
        r = self.offline(self.clink("state-query", "--dataset=%s" % dataset, "--sql=%s" % sql, *extra),
                         scenario, loopback=True)
        (self.results / "queries" / ("%s-%s.ndjson" % (scenario, label))).write_text(r.stdout)
        return r

    # -- running ---------------------------------------------------------------------
    def want(self, name):
        return self.only is None or name in self.only

    def run(self):
        self.say("Proving network isolation for the offline commands")
        self.iso.establish()
        self.note("isolation: %s (%s; %s)" % (self.iso.state, self.iso.mechanism, self.iso.detail))
        self.outcome("setup", "network isolation is enforced", "enforced", self.iso.state, self.iso.enforced)
        self.loopback.establish()
        self.note("loopback-only isolation for state-query: %s (%s; %s)" % (
            self.loopback.state, self.loopback.mechanism, self.loopback.detail))
        self.outcome("setup", "loopback-only isolation is enforced", "enforced", self.loopback.state,
                     self.loopback.enforced)
        self.say("Starting Kafka (compose project %s)" % self.kafka.project)
        self.kafka.up()
        versions = {"kafka_image": "apache/kafka:3.9.1", "pyarrow": pyarrow.__version__,
                    "duckdb": duckdb.__version__, "python": platform.python_version(),
                    "isolation": {"state": self.iso.state, "mechanism": self.iso.mechanism,
                                  "detail": self.iso.detail},
                    "loopback_isolation": {"state": self.loopback.state,
                                           "mechanism": self.loopback.mechanism,
                                           "detail": self.loopback.detail}}
        (self.results / "versions.json").write_text(json.dumps(versions, indent=2))
        import inspect_scenarios as scenarios
        for name, fn in scenarios.ALL:
            if not self.want(name):
                continue
            self.say("Scenario: %s" % name)
            try:
                fn(self)
            except Exception as e:
                self.outcome(name, "the scenario ran", "to the end", "stopped", False,
                             "%s: %s" % (type(e).__name__, e))
        failed = [o for o in self.outcomes if not o["passed"]]
        (self.results / "scenarios.json").write_text(json.dumps(self.outcomes, indent=2))
        (self.results / "commands.json").write_text(json.dumps(COMMANDS, indent=2))
        (self.results / "costs.json").write_text(json.dumps(self.costs, indent=2))
        self.say("%d check(s), %d failed" % (len(self.outcomes), len(failed)))
        return 0 if not failed and self.outcomes else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--project", default="clink-state-inspection")
    ap.add_argument("--kafka-port", type=int, default=29292)
    ap.add_argument("--only", default="")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    demo = Demo(args)
    try:
        return demo.run()
    finally:
        if not args.keep:
            demo.kafka.down()


if __name__ == "__main__":
    sys.exit(main())
