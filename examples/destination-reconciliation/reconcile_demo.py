#!/usr/bin/env python3
"""The destination-reconciliation example's driver (see README.md; run.sh runs it).

Each scenario runs the example job on a fresh local cluster from the build,
against the compose stack's Kafka and ClickHouse, collects a bundle, and
reconciles a completed checkpoint with `clink incident-reconcile`. An
independent check then confirms the topic is still the log the source recorded,
reads the checkpoint's source range back from Kafka, derives the rows the job
owes the destination with workload.reference_row(),
and compares them with what `incident-reconcile` expected and with what
ClickHouse holds, read by this script on its own path. A scenario passes when
the reconciliation's verdict is the one the scenario requires and the
independent check agrees with it.

Python 3 standard library only; Docker for the services.
"""
import argparse
import decimal
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import proxy as fault_proxy  # noqa: E402
import workload  # noqa: E402

CH_PASSWORD = "clink-demo"
RECONCILE_PASSWORD = "reconcile-demo"


class Failed(Exception):
    pass


COMMANDS = []  # every clink command run, for the qualification bundle


def logged(args, where=""):
    COMMANDS.append({"at": time.strftime("%Y-%m-%dT%H:%M:%S"), "where": where,
                     "argv": [str(a) for a in args]})
    return args


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Services:
    """The compose stack: Kafka and ClickHouse."""

    def __init__(self, project, kafka_port, ch_port, log):
        self.project = project
        self.kafka_port = kafka_port
        self.ch_port = ch_port
        self.log = log
        self.env = dict(os.environ, COMPOSE_PROJECT_NAME=project, KAFKA_PORT=str(kafka_port),
                        CLICKHOUSE_PORT=str(ch_port))

    def compose(self, *args, input_text=None, check=True, timeout=600):
        cmd = ["docker", "compose", "--project-directory", str(HERE), "-f",
               str(HERE / "docker-compose.yml"), *args]
        r = subprocess.run(cmd, env=self.env, input=input_text, capture_output=True, text=True,
                           timeout=timeout)
        if check and r.returncode != 0:
            raise Failed("docker compose %s failed: %s" % (" ".join(args[:3]), r.stderr[-2000:]))
        return r

    def up(self):
        self.compose("up", "-d", "--wait", timeout=900)

    def down(self):
        self.compose("down", "-v", "--remove-orphans", check=False, timeout=300)

    def ch(self, sql, user="admin", password="admin", fmt=None):
        args = ["exec", "-T", "clickhouse", "clickhouse-client", "--user", user, "--password",
                password, "--query", sql]
        if fmt:
            args += ["--format", fmt]
        r = self.compose(*args, check=False)
        if r.returncode != 0:
            raise Failed("clickhouse-client: %s\n%s" % (sql[:200], r.stderr[-1500:]))
        return r.stdout

    def kafka(self, *tool_args, input_text=None, check=True):
        return self.compose("exec", "-T", "kafka", *tool_args, input_text=input_text, check=check)

    def create_topic(self, topic, partitions=None):
        self.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server=kafka:9092", "--create",
                   "--topic=%s" % topic,
                   "--partitions=%d" % (partitions or workload.TOPIC_PARTITIONS),
                   "--replication-factor=1")

    def topic_log(self, topic):
        """The log the topic is now, as Kafka's own tools print it: the
        cluster's id and the topic's id, or None when Kafka has no such
        topic."""
        r = self.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server=kafka:9092",
                       "--describe", "--topic=%s" % topic, check=False)
        topic_id = re.search(r"TopicId:\s*(\S+)", r.stdout)
        if r.returncode != 0 or topic_id is None:
            return None
        c = self.kafka("/opt/kafka/bin/kafka-cluster.sh", "cluster-id",
                       "--bootstrap-server=kafka:9092", check=False)
        cluster_id = re.search(r"Cluster ID:\s*(\S+)", c.stdout)
        return {"topic": topic, "cluster_id": cluster_id.group(1) if cluster_id else None,
                "topic_id": topic_id.group(1)}

    def recreate_topic(self, topic):
        """Deletes the topic and creates it again under its name: another
        topic, with another id, whose offsets start again at 0. Kafka deletes
        in the background, so the create is retried until the name is free."""
        self.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server=kafka:9092", "--delete",
                   "--topic=%s" % topic)
        deadline = time.time() + 60
        while True:
            r = self.kafka("/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server=kafka:9092",
                           "--create", "--topic=%s" % topic,
                           "--partitions=%d" % workload.TOPIC_PARTITIONS,
                           "--replication-factor=1", check=False)
            if r.returncode == 0:
                return
            if time.time() > deadline:
                raise Failed("topic %s was not created again: %s" % (topic, r.stderr[-500:]))
            time.sleep(0.5)

    def produce(self, topic, ids):
        lines = "".join("%s\t%s\n" % workload.record(i) for i in ids)
        self.kafka("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server=kafka:9092",
                   "--topic=%s" % topic, "--property", "parse.key=true", "--property",
                   "key.separator=\t", input_text=lines)

    def read_range(self, topic, partition, start, end):
        """The records at offsets [start, end) of one partition, as
        (offset, value). Fewer than asked for means the topic no longer holds
        them: the caller sees which offsets came back."""
        if end <= start:
            return []
        r = self.kafka("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server=kafka:9092",
                       "--topic=%s" % topic, "--partition=%d" % partition, "--offset=%d" % start,
                       "--max-messages=%d" % (end - start), "--timeout-ms=15000",
                       "--property", "print.offset=true", "--property", "print.key=false",
                       check=False)
        out = []
        for line in r.stdout.splitlines():
            if not line.startswith("Offset:"):
                continue
            head, _, value = line.partition("\t")
            out.append((int(head.split(":", 1)[1]), value))
        return out

    def delete_before(self, topic, partition, offset):
        spec = json.dumps({"partitions": [{"topic": topic, "partition": partition, "offset": offset}],
                           "version": 1})
        self.compose("exec", "-T", "kafka", "sh", "-c",
                     "cat > /tmp/delete.json && /opt/kafka/bin/kafka-delete-records.sh "
                     "--bootstrap-server kafka:9092 --offset-json-file /tmp/delete.json",
                     input_text=spec)


TABLE_DDL = """CREATE TABLE demo.{table} (
    event_id     Int64,
    account      String,
    kind         LowCardinality(String),
    amount       Decimal(18, 2),
    amount_cents Int64,
    fee          Float64,
    flagged      Bool,
    note         Nullable(String),
    occurred_at  DateTime64(3)
) ENGINE = MergeTree ORDER BY event_id
SETTINGS async_insert = 0, non_replicated_deduplication_window = {window}"""


# Defects put into the job on purpose, each as (what the job says, what it
# says instead): the job writes what it computes faithfully, and only a
# reference computed outside it can tell.
JOB_VARIANTS = {
    # Values: the amount in cents is computed at the wrong scale.
    "wrong-projection": ("CAST(amount * 100 AS BIGINT) AS amount_cents",
                         "CAST(amount * 10 AS BIGINT) AS amount_cents"),
    # A filter that drops valid payments: a threshold of 10.00, not 1.00.
    "faulty-filter": ("amount >= 1.00;", "amount >= 10.00;"),
    # A filter that lets through rows it should not: chargebacks.
    "extra-output": ("kind IN ('purchase', 'refund')", "kind IN ('purchase', 'refund', 'chargeback')"),
    # Every row and every amount is there, so counts and totals agree: the
    # account is written in capitals.
    "equal-totals": ("SELECT event_id,\n       account,", "SELECT event_id,\n       UPPER(account) AS account,"),
}


class Cluster:
    """A Coordinator and two Workers from the build, for one scenario. Another
    example runs its own job through it by overriding the job's template, its
    parallelism and its deliberate defects."""

    job_template = HERE / "job.sql.in"
    parallelism = 2
    variants = JOB_VARIANTS
    # The completed checkpoints each Worker keeps; another example lowers it
    # to have the Workers purge state before it is collected.
    retained = 100000

    def __init__(self, demo, name, worker1_faults="", worker2_late=False, capture=True,
                 capture_records=100000, interval_ms=1000, defer_worker2=False, variant=None):
        self.demo = demo
        self.name = name
        self.dir = demo.work / name
        for d in ("ha", "checkpoints", "capture", "logs"):
            (self.dir / d).mkdir(parents=True, exist_ok=True)
        self.coord_port = free_port()
        self.coord_http = free_port()
        self.procs = {}
        self.worker1_faults = worker1_faults
        self.worker2_late = worker2_late
        # worker-2 is left to the scenario to start (worker(2)), when it wants
        # the job to restore there.
        self.defer_worker2 = defer_worker2
        # A defect put into the job's SQL on purpose (JOB_VARIANTS), or None.
        self.variant = variant
        self.capture = capture
        self.capture_records = capture_records
        self.interval_ms = interval_ms

    def _spawn(self, name, args, extra_env=None):
        env = dict(os.environ, CLINK_DEMO_CH_PASSWORD=CH_PASSWORD)
        env.update(extra_env or {})
        log = open(self.dir / "logs" / ("%s.log" % name), "w")
        p = subprocess.Popen(logged([str(self.demo.bin / "clink_node"), *args],
                                    "%s %s%s" % (self.name, name,
                                                 " CLINK_FAULT_INJECT=" + env["CLINK_FAULT_INJECT"]
                                                 if "CLINK_FAULT_INJECT" in env else "")),
                             stdout=log, stderr=subprocess.STDOUT, env=env)
        self.procs[name] = p
        return p

    def _await_http(self, port, what):
        import urllib.request
        for _ in range(200):
            try:
                urllib.request.urlopen("http://127.0.0.1:%d/api/v1/health" % port, timeout=1)
                return
            except Exception:
                time.sleep(0.1)
        raise Failed("%s did not come up" % what)

    def worker(self, n, faults=""):
        port = free_port()
        http = free_port()
        env = {"CLINK_FAULT_INJECT": faults} if faults else {}
        self._spawn("worker-%d" % n,
                    ["--role=worker", "--id=worker-%d" % n, "--coordinator-host=127.0.0.1",
                     "--coordinator-port=%d" % self.coord_port, "--port=%d" % port,
                     "--data-host=127.0.0.1", "--slots=16", "--http-port=%d" % http,
                     "--checkpoint-num-retained=%d" % self.retained], env)
        self._await_http(http, "worker-%d" % n)

    def start(self, topic, table, ch_port):
        self._spawn("coordinator", ["--role=coordinator", "--bind-host=127.0.0.1",
                                    "--port=%d" % self.coord_port, "--http-port=%d" % self.coord_http,
                                    "--ha-dir=%s" % (self.dir / "ha")])
        self._await_http(self.coord_http, "the Coordinator")
        self.worker(1, self.worker1_faults)
        if not self.worker2_late and not self.defer_worker2:
            self.worker(2)
        sql = Path(self.job_template).read_text()
        for k, v in {"@BROKERS@": "127.0.0.1:%d" % self.demo.services.kafka_port, "@TOPIC@": topic,
                     "@CH_PORT@": str(ch_port), "@TABLE@": table}.items():
            sql = sql.replace(k, v)
        if self.variant is not None:
            right, wrong = self.variants[self.variant]
            if right not in sql:
                raise Failed("the job's SQL has no %r for variant %s" % (right, self.variant))
            sql = sql.replace(right, wrong)
        (self.dir / "job.sql").write_text(sql)
        args = [str(self.demo.bin / "clink"), "run", str(self.dir / "job.sql"),
                "--coordinator-host=127.0.0.1", "--coordinator-port=%d" % self.coord_http,
                "--name=%s" % self.name, "--parallelism=%d" % self.parallelism,
                "--checkpoint-dir=%s" % (self.dir / "checkpoints"),
                "--checkpoint-interval-ms=%d" % self.interval_ms]
        if self.capture:
            args += ["--capture-dir=%s" % (self.dir / "capture"),
                     "--capture-records=%d" % self.capture_records]
        r = subprocess.run(logged(args, self.name), capture_output=True, text=True, timeout=120)
        (self.dir / "logs" / "submit.log").write_text(r.stdout + r.stderr)
        if r.returncode != 0 or '"ok":true' not in r.stdout:
            raise Failed("the submission was refused: %s" % (r.stdout + r.stderr)[-1500:])
        if self.worker2_late and not self.defer_worker2:
            self.worker(2)

    @property
    def markers(self):
        return self.dir / "checkpoints" / "_jobs" / "1"

    def completed(self):
        if not self.markers.is_dir():
            return []
        return sorted(int(p.name.split("-", 1)[1]) for p in self.markers.iterdir()
                      if p.name.startswith("COMPLETED-") and p.name.split("-", 1)[1].isdigit())

    def await_completed(self, at_least, timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            done = self.completed()
            if len(done) >= at_least:
                return done
            time.sleep(0.2)
        raise Failed("only %d checkpoint(s) completed" % len(self.completed()))

    def collect(self, out, captures=True, extra=()):
        args = [str(self.demo.bin / "clink"), "incident-collect",
                "--checkpoint-dir=%s" % (self.dir / "checkpoints"), "--job=1",
                "--ha-dir=%s" % (self.dir / "ha"), "--capture-dir=%s" % (self.dir / "capture"),
                "--include-state", "--epochs=1-1000000", "--out=%s" % out, "--json", *extra]
        if captures:
            args.append("--include-captures")
        logged(args, self.name)
        for attempt in range(1, 21):
            r = subprocess.run(args, capture_output=True, text=True, timeout=600)
            if r.returncode == 0:
                return json.loads(r.stdout)
            if "refused" not in r.stderr and "changed" not in r.stderr:
                raise Failed("collection failed: %s" % r.stderr[-1500:])
            time.sleep(0.2)
        raise Failed("collection never completed: %s" % r.stderr[-1500:])

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


class Demo:
    # The module of scenarios beside the driver; another example names its own.
    scenario_module = "scenarios"

    def __init__(self, args):
        self.bin = Path(args.bin_dir).resolve()
        self.work = Path(args.work).resolve()
        self.results = self.work / "results"
        for d in ("reports", "expected", "independent", "logs"):
            (self.results / d).mkdir(parents=True, exist_ok=True)
        self.services = Services(args.project, args.kafka_port, args.clickhouse_port, self.results)
        self.outcomes = []
        self.summary = open(self.results / "summary.txt", "a")
        self.only = set(args.only.split(",")) if args.only else None
        self.feed_rate = args.rate
        # The interpreter with clink_reference and its Kafka client installed
        # (run.sh makes a virtual environment for it).
        self.reference_python = args.reference_python
        (self.results / "references").mkdir(parents=True, exist_ok=True)

    # -- reporting --------------------------------------------------------------
    def say(self, text):
        line = "\n==> %s" % text
        print(line, flush=True)
        self.summary.write(line + "\n")
        self.summary.flush()

    def note(self, text):
        line = "    %s" % text
        print(line, flush=True)
        self.summary.write(line + "\n")
        self.summary.flush()

    def outcome(self, scenario, check, required, observed, ok, detail=""):
        self.outcomes.append({"scenario": scenario, "check": check, "required": required,
                              "observed": observed, "passed": bool(ok), "detail": detail})
        self.note("%s %s: required %s, observed %s%s" % ("PASS" if ok else "FAIL", check, required,
                                                          observed, (" (" + detail + ")") if detail else ""))

    # -- ClickHouse setup -------------------------------------------------------
    def setup_clickhouse(self):
        s = self.services
        s.ch("CREATE DATABASE IF NOT EXISTS demo")
        s.ch("CREATE USER IF NOT EXISTS clink IDENTIFIED WITH plaintext_password BY '%s'" % CH_PASSWORD)
        s.ch("GRANT SELECT, INSERT ON demo.* TO clink")
        # The system tables the native sink's open checks read
        # (docs/connectors/clickhouse.md, clickhouse.access_denied).
        for t in ("settings", "tables", "columns", "merge_tree_settings",
                  "replicated_merge_tree_settings"):
            s.ch("GRANT SELECT ON system.%s TO clink" % t)
        s.ch("CREATE USER IF NOT EXISTS reconciler IDENTIFIED WITH plaintext_password BY '%s'"
             % RECONCILE_PASSWORD)
        # Read only: SELECT on the example's tables and the three system
        # tables a reconciliation reads, and nothing else.
        s.ch("GRANT SELECT ON demo.* TO reconciler")
        for t in ("tables", "columns", "mutations", "parts"):
            s.ch("GRANT SELECT ON system.%s TO reconciler" % t)

    def create_table(self, table, window=1000):
        self.services.ch(TABLE_DDL.format(table=table, window=window))

    # -- the commands -----------------------------------------------------------
    def reconcile(self, label, bundle, checkpoint, port=None, extra=()):
        """Runs incident-reconcile; returns (exit code, JSON report or None, text)."""
        report = self.results / "reports" / ("%s.json" % label)
        expected = self.results / "expected" / ("%s.ndjson" % label)
        args = [str(self.bin / "clink"), "incident-reconcile", str(bundle), "--host=127.0.0.1",
                "--port=%d" % (port or self.services.ch_port), "--user=reconciler",
                "--password-env=CLINK_RECONCILE_PASSWORD", "--key=event_id",
                "--checkpoint=%d" % checkpoint, "--report=%s" % report,
                "--expected-out=%s" % expected, *extra]
        env = dict(os.environ, CLINK_RECONCILE_PASSWORD=RECONCILE_PASSWORD)
        t0 = time.time()
        r = subprocess.run(logged(args, label), capture_output=True, text=True, env=env, timeout=600)
        elapsed = time.time() - t0
        (self.results / "reports" / ("%s.txt" % label)).write_text(r.stdout + r.stderr)
        data = json.loads(report.read_text()) if report.exists() and report.stat().st_size else None
        if data is not None:
            data["_elapsed_s"] = elapsed
        return r.returncode, data, r.stdout + r.stderr

    def scope(self, label, bundle, checkpoint, whole_run=False):
        """clink incident-scope: the scope document a reference computes from.
        Returns (exit code, path)."""
        out = self.results / "references" / ("%s.scope.json" % label)
        args = [str(self.bin / "clink"), "incident-scope", str(bundle),
                "--checkpoint=%d" % checkpoint, "--out=%s" % out]
        if whole_run:
            args.append("--from-run-start")
        r = subprocess.run(logged(args, label), capture_output=True, text=True, timeout=120)
        (self.results / "references" / ("%s.scope.txt" % label)).write_text(r.stdout + r.stderr)
        return r.returncode, out

    def reference(self, label, scope_path, run_start=None, extra=()):
        """The example's reference implementation (reference_payments.py) over
        the scope, reading Kafka on the connection given here. Returns (exit
        code, artifact path, seconds, its output)."""
        out = self.results / "references" / ("%s.reference.ndjson" % label)
        args = [self.reference_python, str(HERE / "reference_payments.py"),
                "--scope=%s" % scope_path, "--bootstrap=127.0.0.1:%d" % self.services.kafka_port,
                "--out=%s" % out, *extra]
        if run_start is not None:
            args.append("--run-start-offset=ALL=%d" % run_start)
        t0 = time.time()
        r = subprocess.run(logged(args, label), capture_output=True, text=True, timeout=900,
                           env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"))
        elapsed = time.time() - t0
        (self.results / "references" / ("%s.reference.txt" % label)).write_text(r.stdout + r.stderr)
        return r.returncode, out, elapsed, (r.stdout + r.stderr).strip()

    # -- the independent check -------------------------------------------------------
    def destination_rows(self, table, ids):
        """The destination's rows for these keys, read by this script with the
        administrator's own client, as canonical cells."""
        rows = []
        ids = sorted(ids)
        for i in range(0, len(ids), 2000):
            chunk = ",".join(str(x) for x in ids[i:i + 2000])
            out = self.services.ch(
                # No alias names a column: an alias is visible across the whole
                # query, so `assumeNotNull(note) AS note` would turn the
                # isNull(note) beside it into isNull of the alias.
                "SELECT toString(event_id) AS r_id, account AS r_account, toString(kind) AS r_kind, "
                "toString(amount) AS r_amount, toString(amount_cents) AS r_cents, "
                "toString(reinterpretAsUInt64(fee)) AS r_fee_bits, toUInt8(flagged) AS r_flagged, "
                "isNull(note) AS r_note_null, assumeNotNull(note) AS r_note, "
                "toString(reinterpretAsInt64(occurred_at)) AS r_ts FROM demo.%s "
                "WHERE event_id IN (%s)" % (table, chunk), fmt="JSONEachRow")
            for line in out.splitlines():
                v = json.loads(line)
                rows.append({
                    "event_id": "I%d" % int(v["r_id"]),
                    "account": "S" + v["r_account"],
                    "kind": "S" + v["r_kind"],
                    "amount": "D%d@2" % int(decimal.Decimal(v["r_amount"]) * 100),
                    "amount_cents": "I%d" % int(v["r_cents"]),
                    "fee": "F%016x" % int(v["r_fee_bits"]),
                    "flagged": "B%d" % int(v["r_flagged"]),
                    "note": "N" if int(v["r_note_null"]) else "S" + v["r_note"],
                    "occurred_at": "T%d@3" % int(v["r_ts"]),
                })
        return rows

    def independent(self, label, topic, table, report):
        """Confirms the topic is still the log the source recorded, reads the
        scope's source range from it and derives the rows the job owes the
        destination; compares them with incident-reconcile's expectation and
        with the destination. Returns its verdict: exact, differs (with counts)
        or unavailable (with why)."""
        ranges = report["scope"]["source"]["ranges"]
        if not ranges or not report["scope"]["source"]["from_complete"] or \
                not report["scope"]["source"]["to_complete"]:
            return {"verdict": "unavailable", "why": "the report names no complete source range"}
        # An offset names a record only in the log it was read from: a topic
        # deleted and created again under its name holds other records at the
        # same offsets.
        recorded = [i["end"] for i in report["scope"]["source"].get("identities", []) if i.get("end")]
        if not recorded:
            return {"verdict": "unavailable", "why": "the report names no log for the source's positions"}
        live = self.services.topic_log(topic)
        for log in recorded:
            if live is None or log != live:
                return {"verdict": "unavailable",
                        "why": "the source read topic %s (cluster %s, topic id %s), and Kafka now has %s"
                               % (log["topic"], log["cluster_id"], log["topic_id"],
                                  "no such topic" if live is None else
                                  "topic id %s on cluster %s" % (live["topic_id"], live["cluster_id"]))}
        reference = {}
        for r in ranges:
            start = r["from"] if r["from"] is not None else 0
            end = r["to"]
            got = self.services.read_range(topic, r["partition"], start, end)
            offsets = [o for o, _ in got]
            if offsets != list(range(start, end)):
                return {"verdict": "unavailable",
                        "why": "partition %d offsets [%d, %d) are not all in the topic any more "
                               "(read %d, from %s)" % (r["partition"], start, end, len(offsets),
                                                       offsets[0] if offsets else "none")}
            for _, value in got:
                row = workload.reference_from_value(value)
                if row is not None:
                    reference[row["event_id"]] = row
        expected_path = self.results / "expected" / ("%s.ndjson" % label)
        exported = [json.loads(line) for line in expected_path.read_text().splitlines() if line]
        exported_by_id = {e["event_id"]: e for e in exported}
        same_expectation = (len(exported) == len(exported_by_id) and
                            exported_by_id == reference)
        ids = [int(k[1:]) for k in reference]
        observed = self.destination_rows(table, ids)
        by_id = {}
        for row in observed:
            by_id.setdefault(row["event_id"], []).append(row)
        missing = duplicated = changed = unexpected = 0
        for key, want in reference.items():
            got = by_id.get(key, [])
            exact = sum(1 for g in got if g == want)
            if not got:
                missing += 1
            elif exact == 0:
                changed += 1
                unexpected += len(got) - 1
            else:
                duplicated += exact - 1
                unexpected += len(got) - exact
        rows = len(reference)
        count_obs = len(observed)
        sum_ref = sum(int(r["amount"][1:].split("@")[0]) for r in reference.values())
        sum_obs = sum(int(r["amount"][1:].split("@")[0]) for r in observed)
        result = {"verdict": "exact" if missing + duplicated + changed + unexpected == 0 else "differs",
                  "rows": rows, "missing": missing, "duplicated": duplicated, "changed": changed,
                  "unexpected": unexpected, "same_expectation": same_expectation,
                  "same_log": True, "log": live,
                  "count_reference": rows, "count_observed": count_obs,
                  "sum_cents_reference": sum_ref, "sum_cents_observed": sum_obs,
                  "duplicated_ids": sorted(int(k[1:]) for k, v in by_id.items()
                                           if k in reference and sum(1 for g in v if g == reference[k]) > 1)}
        (self.results / "independent" / ("%s.json" % label)).write_text(json.dumps(result, indent=2))
        return result

    def agree(self, scenario, label, code, report, ind):
        """The reconciliation and the independent check tell the same story."""
        if ind["verdict"] == "unavailable":
            self.outcome(scenario, label + " agrees with the independent check", "a verdict to agree with",
                         "unavailable", False, ind.get("why", ""))
            return False
        if code == 0:
            ok = ind["verdict"] == "exact" and ind["same_expectation"] and ind["same_log"]
            self.outcome(scenario, label + " agrees with the independent check",
                         "exact, same expectation, same log",
                         "%s, same expectation %s, same log %s"
                         % (ind["verdict"], ind["same_expectation"], ind["same_log"]), ok)
            return ok
        if code == 1:
            cmp = next(c for c in report["checks"] if c["id"] == "reconcile.rows")["counts"]
            mine = (cmp["missing"], cmp["changed"], cmp["unexpected"], cmp["duplicated"])
            theirs = (ind["missing"], ind["changed"], ind["unexpected"], ind["duplicated"])
            ok = ind["verdict"] == "differs" and mine == theirs and ind["same_expectation"] and ind["same_log"]
            self.outcome(scenario, label + " agrees with the independent check",
                         "missing/changed/unexpected/duplicated %s, same log" % (mine,),
                         "%s %s, same expectation %s, same log %s"
                         % (ind["verdict"], theirs, ind["same_expectation"], ind["same_log"]), ok)
            return ok
        return True

    # -- running ----------------------------------------------------------------------
    def want(self, name):
        return self.only is None or name in self.only

    def run(self):
        self.say("Starting Kafka and ClickHouse (compose project %s)" % self.services.project)
        self.services.up()
        self.setup_clickhouse()
        versions = {
            "clickhouse": self.services.ch("SELECT version()").strip(),
            "kafka_image": "apache/kafka:3.9.1",
            "clickhouse_image": "clickhouse/clickhouse-server:26.8@sha256:3043f691ec1a847f38b446ff43708893fcbf9815bd9bd88c0f84bfe064ce852f",
            "clink": next((line.split(None, 1)[1] for line in subprocess.run(
                [str(self.bin / "clink"), "--capabilities"], capture_output=True,
                text=True).stdout.splitlines() if line.startswith("version ")), ""),
        }
        (self.results / "versions.json").write_text(json.dumps(versions, indent=2))
        self.note("ClickHouse %s, clink %s" % (versions["clickhouse"], versions["clink"]))
        import importlib
        scenarios = importlib.import_module(self.scenario_module)
        for name, fn in scenarios.ALL:
            if not self.want(name):
                continue
            self.say("Scenario: %s" % name)
            try:
                fn(self)
            except Exception as e:  # a scenario's own Failed, or anything it did not foresee
                self.outcome(name, "the scenario ran", "to the end", "stopped", False,
                             "%s: %s" % (type(e).__name__, e))
        failed = [o for o in self.outcomes if not o["passed"]]
        (self.results / "scenarios.json").write_text(json.dumps(self.outcomes, indent=2))
        (self.results / "commands.json").write_text(json.dumps(COMMANDS, indent=2))
        self.say("%d check(s), %d failed" % (len(self.outcomes), len(failed)))
        return 0 if not failed and self.outcomes else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin-dir", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--project", default="clink-reconcile-demo")
    ap.add_argument("--kafka-port", type=int, default=29192)
    ap.add_argument("--clickhouse-port", type=int, default=29100)
    ap.add_argument("--rate", type=int, default=200)
    ap.add_argument("--only", default="")
    ap.add_argument("--reference-python", default=sys.executable)
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()
    demo = Demo(args)
    try:
        return demo.run()
    finally:
        if not args.keep:
            demo.services.down()


if __name__ == "__main__":
    sys.exit(main())
