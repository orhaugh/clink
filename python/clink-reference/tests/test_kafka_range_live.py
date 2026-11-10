"""read_scope against a real Kafka broker (Apache Kafka, which reports topic
ids): transaction markers and aborted records leave offsets that are never
delivered, so a range's record count is not its span, and a range is complete
only once its end is passed. Expired history, a topic created again, a
compacting topic, a bound and another cluster each leave the read incomplete.

Runs when CLINK_REFERENCE_KAFKA names the broker (host:port); skipped
otherwise. scripts/reference-live.sh starts one and runs it.
"""

import os
import time
import unittest
import uuid

BROKER = os.environ.get("CLINK_REFERENCE_KAFKA")


@unittest.skipUnless(BROKER, "CLINK_REFERENCE_KAFKA names no broker")
class KafkaRangeLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from confluent_kafka.admin import AdminClient
        cls.admin = AdminClient({"bootstrap.servers": BROKER})
        cls.cluster_id = cls.admin.describe_cluster(request_timeout=15).result().cluster_id

    def topic(self, config=None, partitions=1):
        from confluent_kafka.admin import NewTopic
        name = "ref-" + uuid.uuid4().hex[:10]
        f = self.admin.create_topics([NewTopic(name, partitions, 1, config=config or {})])
        f[name].result()
        time.sleep(0.5)
        return name

    def topic_id(self, name):
        from confluent_kafka import TopicCollection
        from clink_reference import kafka_uuid_text
        d = self.admin.describe_topics(TopicCollection([name]))[name].result()
        return kafka_uuid_text(d.topic_id.get_most_significant_bits(),
                               d.topic_id.get_least_significant_bits())

    def produce_transactions(self, topic, txns):
        """Each of `txns` is (values, commit): its values in one transaction,
        committed or aborted. Returns the offsets the committed values landed at."""
        from confluent_kafka import Producer
        p = Producer({"bootstrap.servers": BROKER, "transactional.id": "ref-" + uuid.uuid4().hex,
                      "enable.idempotence": True})
        p.init_transactions(30)
        landed = {}

        def note(err, msg):
            if err is None:
                landed[msg.value().decode()] = msg.offset()

        for values, commit in txns:
            p.begin_transaction()
            for v in values:
                p.produce(topic, value=v.encode(), partition=0, on_delivery=note)
            p.flush(30)
            if commit:
                p.commit_transaction(30)
            else:
                p.abort_transaction(30)
        return landed

    def produce_plain(self, topic, values):
        from confluent_kafka import Producer
        p = Producer({"bootstrap.servers": BROKER})
        for v in values:
            p.produce(topic, value=v.encode(), partition=0)
        p.flush(30)

    def scope(self, topic, start, end, isolation="read_committed", topic_id=None, cluster_id=None):
        from clink_reference import Scope
        return Scope.parse({
            "format": "clink-audit-scope", "version": 1, "job": "1", "sink_op": "9",
            "attempts": ["a"], "graph_sha256": None, "checkpoint": 2, "starts_after": 1,
            "from_run_start": False, "starts_at_run_start": False, "epochs": [2],
            "sources": [{"op": "7", "topic": topic,
                         "cluster_id": cluster_id or self.cluster_id,
                         "topic_id": topic_id or self.topic_id(topic),
                         "isolation_level": isolation, "isolation_set_by_job": False,
                         "same_log": True,
                         "partitions": [{"partition": 0, "from": start, "to": end}]}],
            "sources_complete": True,
            "output": {"columns": [{"name": "v", "type": "String"}],
                       "semantics": "clink-canonical-cells-v1"},
            "scope_sha256": "0" * 64})

    def read(self, scope, **kw):
        from clink_reference import read_scope
        got = []
        result = read_scope(scope, BROKER, lambda rec: got.append((rec.offset, rec.value.decode())),
                            deadline_s=kw.pop("deadline_s", 60), **kw)
        return result, got

    def test_read_committed_skips_markers_and_aborted_records_and_passes_the_end(self):
        t = self.topic()
        landed = self.produce_transactions(t, [(["a", "b"], True), (["x", "y"], False), (["c"], True)])
        # a, b at 0, 1; marker 2; x, y at 3, 4; abort marker 5; c at 6; marker 7.
        self.assertEqual(landed["c"], 6)
        result, got = self.read(self.scope(t, 0, 8))
        self.assertTrue(result.complete, result.incomplete)
        self.assertEqual([v for _, v in got], ["a", "b", "c"])
        self.assertEqual(result.read[0]["records"], 3, "the span is 8 offsets, the records 3")
        self.assertIn("end of log", result.read[0]["completed_by"])
        # A range that ends at c is passed when c is delivered, and c is not in it.
        result, got = self.read(self.scope(t, 0, 6))
        self.assertTrue(result.complete, result.incomplete)
        self.assertEqual([v for _, v in got], ["a", "b"])
        self.assertIn("record at 6", result.read[0]["completed_by"])
        # A range of nothing but an aborted transaction and its marker is complete and empty.
        result, got = self.read(self.scope(t, 3, 6))
        self.assertTrue(result.complete, result.incomplete)
        self.assertEqual(got, [])

    def test_read_uncommitted_delivers_aborted_records(self):
        t = self.topic()
        self.produce_transactions(t, [(["a"], True), (["x"], False), (["b"], True)])
        result, got = self.read(self.scope(t, 0, 6, isolation="read_uncommitted"))
        self.assertTrue(result.complete, result.incomplete)
        self.assertEqual([v for _, v in got], ["a", "x", "b"])

    def test_expired_history_and_a_log_that_ends_early_are_incomplete(self):
        from confluent_kafka import TopicPartition
        t = self.topic()
        self.produce_plain(t, ["r%d" % i for i in range(10)])
        result, _ = self.read(self.scope(t, 0, 12))
        self.assertFalse(result.complete)
        self.assertIn("ends at 10", result.incomplete[0])
        self.admin.delete_records([TopicPartition(t, 0, 4)])[TopicPartition(t, 0, 4)].result()
        result, got = self.read(self.scope(t, 0, 10))
        self.assertFalse(result.complete)
        self.assertIn("no longer in the topic", result.incomplete[0])
        self.assertEqual(got, [], "nothing is read from what remains")

    def test_a_topic_created_again_another_cluster_and_compaction_are_not_read(self):
        t = self.topic()
        self.produce_plain(t, ["a", "b"])
        old = self.topic_id(t)
        self.admin.delete_topics([t])[t].result()
        time.sleep(2)
        for _ in range(30):
            try:
                self.admin.create_topics([__import__("confluent_kafka.admin", fromlist=["NewTopic"])
                                          .NewTopic(t, 1, 1)])[t].result()
                break
            except Exception:  # noqa: BLE001 - deletion still in progress
                time.sleep(1)
        self.produce_plain(t, ["c", "d"])
        result, got = self.read(self.scope(t, 0, 2, topic_id=old))
        self.assertFalse(result.complete)
        self.assertIn("deleted and created again", result.incomplete[0])
        self.assertEqual(got, [])
        result, _ = self.read(self.scope(t, 0, 2, cluster_id="another-cluster"))
        self.assertFalse(result.complete)
        self.assertIn("are cluster", result.incomplete[0])
        c = self.topic(config={"cleanup.policy": "compact"})
        self.produce_plain(c, ["a"])
        result, _ = self.read(self.scope(c, 0, 1))
        self.assertFalse(result.complete)
        self.assertIn("compacts", result.incomplete[0])

    def test_a_bound_reached_first_is_incomplete(self):
        t = self.topic()
        self.produce_plain(t, ["r%d" % i for i in range(5)])
        result, _ = self.read(self.scope(t, 0, 5), max_records=2)
        self.assertFalse(result.complete)
        self.assertIn("bound", result.incomplete[0])


if __name__ == "__main__":
    unittest.main()
