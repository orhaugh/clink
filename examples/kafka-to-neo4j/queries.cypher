// Investigation queries for the example's graph. Run one with
//   ./scripts/cypher.py "<query>"
// or paste it into Neo4j Browser at http://localhost:7474.
//
// A Signal is an indicator for a person to review, raised by one stated rule
// (rule_version) from synthetic data. It is not a finding about anyone.

// 1. The signals, newest window first, with what raised them.
MATCH (s:Signal)-[:FLAGS]->(a:Account)
RETURN s.id, a.id AS account, s.window_start_time, s.distinct_senders, s.transfers, s.rule_version
ORDER BY s.window_start DESC;

// 2. Explain one signal from its supporting events: the transfers in its
//    window, by event id, with who sent them. The transfers are written by the
//    other job, so until it has caught up some may not be there yet.
MATCH (s:Signal {id: 'acct-07:1772355720000:fan-in/v1'})
MATCH (from:Account)-[:SENT]->(t:Transfer)-[:TO]->(:Account {id: s.account})
WHERE t.event_id IN s.event_ids
RETURN t.event_id, from.id AS sender, t.amount, t.at_time
ORDER BY t.at;

// 3. What the flagged account did next: transfers it sent within five minutes
//    after the window closed (the "and then moved it on" half of the pattern,
//    which the graph answers rather than the signal).
MATCH (s:Signal)-[:FLAGS]->(a:Account)-[:SENT]->(t:Transfer)-[:TO]->(to:Account)
WHERE t.at >= s.window_end AND t.at < s.window_end + 300000
RETURN s.id, t.event_id, to.id AS recipient, t.amount, t.at_time
ORDER BY s.id, t.at;

// 4. Two hops on: where the onward money went after that.
MATCH (s:Signal)-[:FLAGS]->(a:Account)-[:SENT]->(t1:Transfer)-[:TO]->(mid:Account)
      -[:SENT]->(t2:Transfer)-[:TO]->(next:Account)
WHERE t1.at >= s.window_end AND t2.at >= t1.at
RETURN s.id, mid.id, t2.event_id, next.id, t2.amount
ORDER BY s.id, t2.at
LIMIT 50;

// 5. Accounts that paid into more than one flagged account.
MATCH (payer:Account)-[:SENT]->(t:Transfer)-[:TO]->(a:Account)<-[:FLAGS]-(s:Signal)
WHERE t.event_id IN s.event_ids
WITH payer, collect(DISTINCT a.id) AS flagged
WHERE size(flagged) > 1
RETURN payer.id, flagged;

// 6. The graph's own health: every transfer has exactly one sender and one
//    recipient. Expect no rows.
MATCH (t:Transfer)
WHERE COUNT { (t)<-[:SENT]-() } <> 1 OR COUNT { (t)-[:TO]->() } <> 1
RETURN t.event_id;
