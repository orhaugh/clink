-- Kafka -> clink -> Neo4j: synthetic transfers into a graph, and a fan-in
-- indicator computed in one-minute event-time windows, enriched with device
-- relationships the graph already held.
--
-- run.sh submits this file to the Coordinator. Each INSERT becomes its own
-- job, with its own Kafka table and its own consumer group: one writes the
-- transfer graph, the other the signals. They are separate sink transactions
-- and separate jobs, so a signal can be in the graph before the transfers that
-- support it, or after; nothing is visible across the two atomically (see the
-- README, "What is consistent with what").
--
-- Both statements are replay-safe. Delivery to Neo4j is at least once: after
-- the Worker kill, the restored jobs write again some batches the graph
-- already holds, and the uniqueness constraints in schema.cypher, with the
-- statements below, make a second write change nothing.

-- One JSON transfer per Kafka record, e.g.
--   {"event_id": 64, "sender": "acct-08", "recipient": "acct-07",
--    "amount": 900, "at": 1772355724000}
-- `at` is the event time in epoch milliseconds.
CREATE TABLE transfers_for_graph (
    event_id  BIGINT,
    sender    VARCHAR,
    recipient VARCHAR,
    amount    BIGINT,
    at        BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'transfers',
    group_id          = 'clink-neo4j-graph',
    auto_offset_reset = 'earliest'
);

CREATE TABLE transfers_for_signals (
    event_id  BIGINT,
    sender    VARCHAR,
    recipient VARCHAR,
    amount    BIGINT,
    at        BIGINT
) WITH (
    connector         = 'kafka',
    format            = 'json',
    brokers           = 'kafka:9092',
    topic             = 'transfers',
    group_id          = 'clink-neo4j-signals',
    auto_offset_reset = 'earliest',
    event_time_column = 'at',
    watermark_lag_ms  = '5000'
);

-- The transfer graph: (:Account)-[:SENT]->(:Transfer)-[:TO]->(:Account).
-- Accounts and transfers are found by their ids under uniqueness constraints,
-- and a transfer's two relationships are created only by the transaction that
-- creates the transfer, so a replayed or concurrent duplicate finds the node
-- and creates nothing.
CREATE TABLE transfer_graph (
    event_id  BIGINT,
    sender    VARCHAR,
    recipient VARCHAR,
    amount    BIGINT,
    at        BIGINT
) WITH (
    connector = 'neo4j',
    endpoint  = 'http://neo4j:7474',
    username  = 'neo4j',
    password  = 'env://NEO4J_PASSWORD',
    batch_size = '100',
    flush_interval_ms = '500',
    statement = 'UNWIND $rows AS row
                 MERGE (s:Account {id: row.sender})
                 MERGE (r:Account {id: row.recipient})
                 MERGE (t:Transfer {event_id: row.event_id})
                   ON CREATE SET t.amount = row.amount, t.at = row.at,
                                 t.at_time = datetime({epochMillis: row.at}), t._created = true
                 WITH s, r, t
                 CALL (s, r, t) {
                   WITH s, r, t WHERE t._created
                   CREATE (s)-[:SENT]->(t)
                   CREATE (t)-[:TO]->(r)
                 }
                 REMOVE t._created'
);

-- Graph context for a signal's account, read from Neo4j as each signal is
-- raised: the devices the account has used, how many other accounts used any
-- of them, and the first ten of those accounts. A read-only lookup: one
-- parameterised read per signal, the account as $key, through an index seek
-- (the account_id constraint) and two fixed hops, never an open-ended path.
-- No row for an account with no device on record: matched is then false and
-- the counts NULL. A read that fails (the server down past the retry budget,
-- a timeout, a value of the wrong type) fails the job instead; it is never
-- written as a no-match.
--
-- It reads the Device and USES_DEVICE reference data seed_reference.py loads
-- before the jobs start, never what they write, so the answer does not depend
-- on how far the transfer job has got. It reads the graph as it is when the
-- signal is raised: observed_at is clink's clock at the reply, and bookmark
-- the database's position token for the read, not a time.
--
-- Community Edition has no roles, so the example reads as neo4j; on an
-- edition with roles, give the lookup a user granted only reads.
CREATE TABLE device_context (
    account           VARCHAR,
    devices           BIGINT,
    shared_accounts   BIGINT,
    evidence_accounts VARCHAR,
    matched           BOOLEAN,
    observed_at       TIMESTAMP(3),
    bookmark          VARCHAR
) WITH (
    connector = 'lookup',
    function  = 'neo4j',
    endpoint  = 'http://neo4j:7474',
    username  = 'neo4j',
    password  = 'env://NEO4J_PASSWORD',
    statement = 'MATCH (a:Account {id: $key})
                 WHERE EXISTS { (a)-[:USES_DEVICE]->(:Device) }
                 WITH a, COLLECT {
                   MATCH (a)-[:USES_DEVICE]->(:Device)<-[:USES_DEVICE]-(o:Account)
                   WHERE o <> a
                   RETURN DISTINCT o.id AS id ORDER BY id LIMIT 1000
                 } AS others
                 RETURN COUNT { (a)-[:USES_DEVICE]->(:Device) } AS devices,
                        size(others) AS shared_accounts,
                        reduce(s = "", x IN others[0..10] |
                               s + CASE WHEN s = "" THEN "" ELSE "," END + x) AS evidence_accounts',
    matched_column     = 'matched',
    observed_at_column = 'observed_at',
    bookmark_column    = 'bookmark',
    request_timeout_ms = '5000',
    server_timeout_s   = '2',
    max_retries        = '3',
    retry_window_ms    = '30000'
);

-- The indicator: an account that, inside one minute of event time, receives
-- from at least five distinct senders. Each signal keeps what explains it:
-- its window, its counts, the event ids of the transfers in it, the version
-- of the rule that raised it, and the graph context it was enriched with,
-- with when that was read. Its id is a function of the window, so a replay
-- writes the same signal again rather than a second one; the enrichment
-- properties then hold the latest read.
CREATE TABLE fan_in_signals (
    signal_id             VARCHAR,
    account               VARCHAR,
    window_start          BIGINT,
    window_end            BIGINT,
    distinct_senders      BIGINT,
    transfers             BIGINT,
    event_ids             BIGINT ARRAY,
    rule_version          VARCHAR,
    enrichment_version    VARCHAR,
    device_evidence_found BOOLEAN,
    devices               BIGINT,
    shared_accounts       BIGINT,
    evidence_accounts     VARCHAR,
    context_observed_at   TIMESTAMP(3),
    context_bookmark      VARCHAR
) WITH (
    connector = 'neo4j',
    endpoint  = 'http://neo4j:7474',
    username  = 'neo4j',
    password  = 'env://NEO4J_PASSWORD',
    statement = 'UNWIND $rows AS row
                 MERGE (a:Account {id: row.account})
                 MERGE (sig:Signal {id: row.signal_id})
                   ON CREATE SET sig._created = true
                 SET sig.kind = ''indicator for review'',
                     sig.account = row.account,
                     sig.rule_version = row.rule_version,
                     sig.window_start = row.window_start,
                     sig.window_end = row.window_end,
                     sig.window_start_time = datetime({epochMillis: row.window_start}),
                     sig.distinct_senders = row.distinct_senders,
                     sig.transfers = row.transfers,
                     sig.event_ids = COLLECT { UNWIND row.event_ids AS e RETURN e ORDER BY e },
                     sig.enrichment_version = row.enrichment_version,
                     sig.device_evidence_found = row.device_evidence_found,
                     sig.devices = row.devices,
                     sig.shared_device_accounts = row.shared_accounts,
                     sig.device_evidence = row.evidence_accounts,
                     sig.context_observed_at = row.context_observed_at,
                     sig.context_bookmark = row.context_bookmark
                 WITH a, sig
                 CALL (a, sig) {
                   WITH a, sig WHERE sig._created
                   CREATE (sig)-[:FLAGS]->(a)
                 }
                 REMOVE sig._created'
);

INSERT INTO transfer_graph
SELECT event_id, sender, recipient, amount, at FROM transfers_for_graph;

INSERT INTO fan_in_signals
WITH windows AS (
    SELECT recipient              AS account,
           window_start,
           window_end,
           COUNT(DISTINCT sender) AS distinct_senders,
           COUNT(*)               AS transfers,
           ARRAY_AGG(event_id)    AS event_ids
    FROM transfers_for_signals
    GROUP BY recipient, TUMBLE(at, INTERVAL '1' MINUTE)
    HAVING COUNT(DISTINCT sender) >= 5
)
SELECT w.account || ':' || CAST(w.window_start AS VARCHAR) || ':fan-in/v1' AS signal_id,
       w.account,
       w.window_start,
       w.window_end,
       w.distinct_senders,
       w.transfers,
       w.event_ids,
       'fan-in/v1'         AS rule_version,
       'device-context/v1' AS enrichment_version,
       d.matched           AS device_evidence_found,
       d.devices,
       d.shared_accounts,
       d.evidence_accounts,
       d.observed_at       AS context_observed_at,
       d.bookmark          AS context_bookmark
FROM windows w
LEFT JOIN device_context d ON w.account = d.account;
