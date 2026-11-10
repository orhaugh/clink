"""Read exactly a scope's Kafka offset ranges, or say why not.

For each source in the scope the reader first confirms, against the cluster,
that the topic is the log the job read: the cluster's id and the topic's id
must be the ones the job's checkpoints recorded, so a topic deleted and created
again under its name, or another cluster, is never read in its place. A topic
whose cleanup policy compacts may have removed records from a range, which
offsets cannot show, so its range is not complete.

A range [start, end) is read at the scope's isolation level. Under
read_committed it can hold offsets that are never delivered: transaction
markers, and records of aborted transactions. So the number of records is not
end - start, and a poll that returns nothing proves nothing. A range is
complete only when the consumer has positively passed its end: a record at or
past `end` was delivered, or the partition's end of log was reached at or past
`end`. A range whose start has expired from the log, a log that now ends before
`end`, an error, or a deadline or bound reached first leaves the read
incomplete, with why, and records already delivered are not made into a
complete result.

The client is confluent-kafka, over librdkafka: a different binding from the
job's but the same underlying client library, which the artifact records.
Credentials are the caller's, passed in `config`; nothing is read from the
scope beyond topics, partitions and offsets.
"""

import base64
import struct
import time
import uuid
from dataclasses import dataclass, field


@dataclass
class Record:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None
    headers: list = field(default_factory=list)


@dataclass
class ReadResult:
    complete: bool = True
    incomplete: list = field(default_factory=list)
    verified: list = field(default_factory=list)
    recorded: list = field(default_factory=list)
    read: list = field(default_factory=list)
    records: int = 0
    bytes: int = 0
    seconds: float = 0.0

    def fail(self, why):
        self.complete = False
        self.incomplete.append(why)


def client_versions():
    """The client's version and its librdkafka's, as text. confluent-kafka's
    version() is a (text, number) pair in older releases and text in newer."""
    import confluent_kafka
    v = confluent_kafka.version()
    lib = confluent_kafka.libversion()
    return (v[0] if isinstance(v, tuple) else v, lib[0] if isinstance(lib, tuple) else lib)


def kafka_uuid_text(most, least):
    """A Kafka UUID as Kafka's tools print it: its 16 bytes, most significant
    first, URL-safe base64 without padding."""
    raw = struct.pack(">qq", most, least)
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def read_scope(scope, bootstrap, on_record, config=None, run_start_offsets=None,
               deadline_s=300.0, max_records=50_000_000, max_bytes=8 << 30,
               poll_timeout_s=1.0, request_timeout_s=15.0):
    """Reads every range of `scope`, calling on_record(Record) for each record
    in it, in offset order per partition. Returns a ReadResult saying whether
    the read was complete. `run_start_offsets` gives {(topic, partition):
    offset} for ranges that start at the run's own start, which the job's
    checkpoints do not record; without it such a range is incomplete."""
    from confluent_kafka import Consumer, KafkaError, TopicCollection, TopicPartition
    from confluent_kafka.admin import AdminClient, ConfigResource

    result = ReadResult()
    started = time.monotonic()
    base = {"bootstrap.servers": bootstrap}
    base.update(config or {})
    if not scope.sources_complete:
        result.fail("the scope does not record every source range and log (sources_complete is "
                    "false), so no read of it can be complete")
        return result
    admin = AdminClient(dict(base))
    try:
        cluster_id = admin.describe_cluster(request_timeout=request_timeout_s).result().cluster_id
    except Exception as e:  # noqa: BLE001 - any failure leaves the identity unconfirmed
        result.fail("the cluster's id could not be read: %s" % e)
        return result
    result.recorded.append("read with confluent-kafka %s over librdkafka %s" % client_versions())
    for src in scope.sources:
        if src.topic is None:
            result.fail("source %s has no recorded topic" % src.op)
            continue
        if src.cluster_id is not None and src.cluster_id != cluster_id:
            result.fail("the brokers at %s are cluster %s, and the job read topic %s on cluster %s"
                        % (bootstrap, cluster_id, src.topic, src.cluster_id))
            continue
        if src.cluster_id is None:
            result.recorded.append("the job's checkpoints record no cluster id for %s; not compared"
                                   % src.topic)
        try:
            desc = admin.describe_topics(TopicCollection([src.topic]),
                                         request_timeout=request_timeout_s)[src.topic].result()
            tid = desc.topic_id
            topic_id = kafka_uuid_text(tid.get_most_significant_bits(),
                                       tid.get_least_significant_bits()) if tid is not None else None
        except Exception as e:  # noqa: BLE001
            result.fail("topic %s could not be described: %s" % (src.topic, e))
            continue
        if topic_id == "AAAAAAAAAAAAAAAAAAAAAA":
            topic_id = None
        if src.topic_id is not None:
            if topic_id is None:
                result.fail("the cluster reports no id for topic %s, so it cannot be confirmed as "
                            "the log the job read (id %s)" % (src.topic, src.topic_id))
                continue
            if topic_id != src.topic_id:
                result.fail("topic %s now has id %s, and the job read the topic with id %s: it was "
                            "deleted and created again, and its offsets name other records"
                            % (src.topic, topic_id, src.topic_id))
                continue
            result.verified.append("topic %s is the log the job read: cluster %s, topic id %s"
                                   % (src.topic, cluster_id, topic_id))
        else:
            result.recorded.append("the job's checkpoints record no topic id for %s (a broker "
                                   "that reports none); a topic created again is not told apart"
                                   % src.topic)
        try:
            res = ConfigResource(ConfigResource.Type.TOPIC, src.topic)
            entries = admin.describe_configs([res], request_timeout=request_timeout_s)[res].result()
            policy = entries["cleanup.policy"].value
        except Exception as e:  # noqa: BLE001
            result.fail("topic %s's cleanup.policy could not be read (%s), so whether compaction "
                        "removed records from the range is not known" % (src.topic, e))
            continue
        if "compact" in policy.split(","):
            result.fail("topic %s compacts (cleanup.policy=%s), which can remove records from a "
                        "range without leaving a sign in its offsets" % (src.topic, policy))
            continue
        result.verified.append("topic %s deletes by retention only (cleanup.policy=%s)"
                               % (src.topic, policy))
        for r in src.ranges:
            _read_range(Consumer, KafkaError, TopicPartition, base, src, r, run_start_offsets,
                        on_record, result, started, deadline_s, max_records, max_bytes,
                        poll_timeout_s, request_timeout_s)
    result.seconds = time.monotonic() - started
    return result


def _read_range(Consumer, KafkaError, TopicPartition, base, src, r, run_start_offsets,
                on_record, result, started, deadline_s, max_records, max_bytes,
                poll_timeout_s, request_timeout_s):
    topic, p = src.topic, r.partition
    where = "%s[%d]" % (topic, p)
    start = r.start
    if start is None:
        start = (run_start_offsets or {}).get((topic, p))
        if start is None:
            result.fail("%s starts at the run's own start, which the job's checkpoints do not "
                        "record; give the run's first offset (run_start_offsets)" % where)
            return
        result.recorded.append("%s: the run's first offset, %d, is the operator's, not recorded "
                               "by the job" % (where, start))
    end = r.end
    if end is None:
        result.fail("%s has no recorded end" % where)
        return
    entry = {"topic": topic, "partition": p, "from": start, "to": end, "records": 0,
             "completed_by": None}
    result.read.append(entry)
    if end < start:
        result.fail("%s ends (%d) before it starts (%d)" % (where, end, start))
        return
    conf = dict(base)
    conf.update({"group.id": "clink-reference-" + uuid.uuid4().hex, "enable.auto.commit": False,
                 "enable.partition.eof": True, "auto.offset.reset": "error",
                 "isolation.level": src.isolation_level})
    consumer = Consumer(conf)
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p),
                                                   timeout=request_timeout_s, cached=False)
        if low > start:
            result.fail("%s: offsets [%d, %d) are no longer in the topic (its log now starts at "
                        "%d): expired or deleted" % (where, start, min(low, end), low))
            return
        if high < end:
            result.fail("%s: the log now ends at %d (%s), before the range's end %d"
                        % (where, high, "last stable offset" if src.isolation_level ==
                           "read_committed" else "high watermark", end))
            return
        if start == end:
            entry["completed_by"] = "empty range"
            return
        consumer.assign([TopicPartition(topic, p, start)])
        while True:
            if time.monotonic() - started > deadline_s:
                result.fail("%s: the deadline (%.0f s) passed at offset %s, before the range's end "
                            "%d" % (where, deadline_s, entry.get("last"), end))
                return
            msg = consumer.poll(poll_timeout_s)
            if msg is None:
                continue  # a quiet poll proves nothing
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    if msg.offset() >= end:
                        entry["completed_by"] = "end of log at %d" % msg.offset()
                        return
                    continue
                result.fail("%s: %s" % (where, err.str()))
                return
            off = msg.offset()
            if off >= end:
                entry["completed_by"] = "record at %d" % off
                return
            if off < start:
                continue
            value = msg.value()
            size = len(value or b"") + len(msg.key() or b"")
            result.records += 1
            result.bytes += size
            entry["records"] += 1
            entry["last"] = off
            if result.records > max_records or result.bytes > max_bytes:
                result.fail("%s: the read passed its bound (%d records, %d bytes) before the "
                            "range's end" % (where, max_records, max_bytes))
                return
            ts_type, ts = msg.timestamp()
            on_record(Record(topic=topic, partition=p, offset=off, key=msg.key(), value=value,
                             timestamp_ms=ts if ts_type else None,
                             headers=list(msg.headers() or [])))
    finally:
        consumer.close()
