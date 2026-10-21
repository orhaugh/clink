// The constraints the pipeline's statements rely on. Applied once, before the
// pipeline starts (run.sh does it with scripts/cypher.py --file schema.cypher).
// They are what make a replayed batch, or two transactions writing the same
// id at once, find the existing node instead of creating a second.

CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT transfer_event_id IF NOT EXISTS FOR (t:Transfer) REQUIRE t.event_id IS UNIQUE;
CREATE CONSTRAINT signal_id IF NOT EXISTS FOR (s:Signal) REQUIRE s.id IS UNIQUE;
