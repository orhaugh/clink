#!/usr/bin/env python3
"""Generate the nexmark query templates for BOTH engines from one definition.

Why generated rather than hand-written: a cross-engine ratio only means
something if both engines ran the same query, and two hand-maintained
directories of SQL drift silently - a filter tightened on one side, a window
width changed on the other, and the ratio becomes noise nobody can attribute.
Here each query is defined once, as the projection / filter / grouping it is,
and the two dialects are rendered from that single definition. A difference
between the engines' SQL is then always a deliberate dialect difference,
visible in this file.

Three variants are generated per query and engine. `q*_bh.tmpl.sql` discards the
output, which is what throughput_sampled.sh measures against so the figure is the
engine's read-and-process rate rather than the output connector's ceiling.
`q*.tmpl.sql` writes to Kafka, which is what gate.sh compares row counts from.
`q*_up.tmpl.sql` writes to an upsert sink and is generated only for the queries
that declare a `pk`, for upsert_gate.sh.

q0 and q12 are deliberately NOT generated. Every recorded before/after number
in this repository is against those two exact files, so regenerating them -
even into something equivalent - would put the historical comparisons on a
different premise than the files they were measured from.

Run from this directory:  python3 gen_queries.py
"""

import os
import textwrap

HERE = os.path.dirname(os.path.abspath(__file__))
CLINK_DIR = os.path.join(HERE, "clink")
FLINK_DIR = os.path.join(HERE, "..", "flink-job", "queries")

# ---------------------------------------------------------------------------
# Stream declarations. Both engines read the same three Kafka topics, loaded by
# the harness from one nexmark_dump run, with a 4s watermark lag on each.
# ---------------------------------------------------------------------------

STREAMS = {
    "bid": {
        "topic": "nx-bid",
        "cols": [
            ("auction", "BIGINT"),
            ("bidder", "BIGINT"),
            ("price", "BIGINT"),
            ("channel", "VARCHAR"),
            ("url", "VARCHAR"),
            ("datetime", "BIGINT"),
        ],
    },
    "auction": {
        "topic": "nx-auction",
        "cols": [
            ("id", "BIGINT"),
            ("itemname", "VARCHAR"),
            ("initialbid", "BIGINT"),
            ("reserve", "BIGINT"),
            ("expires", "BIGINT"),
            ("seller", "BIGINT"),
            ("category", "BIGINT"),
            ("datetime", "BIGINT"),
        ],
    },
    "person": {
        "topic": "nx-person",
        "cols": [
            ("id", "BIGINT"),
            ("name", "VARCHAR"),
            ("emailaddress", "VARCHAR"),
            ("city", "VARCHAR"),
            ("state", "VARCHAR"),
            ("datetime", "BIGINT"),
        ],
    },
}

WATERMARK_LAG_MS = 4000

# The two dialects' bounded-out-of-orderness watermarks differ by exactly
# one millisecond: the reference emits maxTimestamp - lag - 1ms (its
# BoundedOutOfOrderness convention), clink emits maxTimestamp - lag. On
# aligned windows (TUMBLE/HOP/CUMULATE ends are step-multiples) the 1ms
# never crosses a pane boundary, but a SESSION's fire point is an
# arbitrary millisecond (last event + gap), and QUAL-07 measured the
# difference as exactly one session: a singleton whose fire point landed
# ON clink's final watermark and one ms past the reference's. clink's lag
# is declared 1ms deeper so the two effective watermarks are identical
# and the fired sets compare equal - the same premise-reconciliation
# discipline as q4/q17's AVG cast and q18's total order.
CLINK_WATERMARK_LAG_MS = WATERMARK_LAG_MS + 1


def clink_source(name, tag):
    s = STREAMS[name]
    cols = ", ".join(f"{c} {t}" for c, t in s["cols"])
    return (
        f"CREATE TABLE {name} ({cols})\n"
        f"  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='{s['topic']}',\n"
        f"        group_id='clink-{tag}-{name}', auto_offset_reset='earliest',\n"
        f"        event_time_column='datetime', watermark_lag_ms='{CLINK_WATERMARK_LAG_MS}');"
    )


def flink_source(name, tag):
    s = STREAMS[name]
    # STRING for VARCHAR; `datetime` is reserved so it is always quoted. The
    # event-time attribute is a computed TIMESTAMP_LTZ, since Flink cannot put a
    # watermark directly on a BIGINT.
    cols = ", ".join(
        f"`{c}` STRING" if t == "VARCHAR" else f"`{c}` {t}" for c, t in s["cols"]
    )
    return (
        f"CREATE TABLE {name} (\n"
        f"  {cols},\n"
        f"  ts AS TO_TIMESTAMP_LTZ(`datetime`, 3),\n"
        f"  WATERMARK FOR ts AS ts - INTERVAL '{WATERMARK_LAG_MS // 1000}' SECOND\n"
        f") WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{s['topic']}',\n"
        f"  'properties.bootstrap.servers' = 'kafka:29092',\n"
        f"  'properties.group.id' = 'flink-{tag}-{name}',\n"
        f"  'scan.startup.mode' = 'earliest-offset',\n"
        f"  'format' = 'json',\n"
        f"  'json.ignore-parse-errors' = 'false'\n"
        f");"
    )


def clink_upsert_sink(name, cols, pk):
    decl = ", ".join(f"{c} {t}" for c, t in cols)
    return (
        f"CREATE TABLE {name} ({decl})\n"
        f"  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='__OUT__',\n"
        f"        mode='upsert', primary_key='{','.join(pk)}');"
    )


def flink_upsert_sink(name, cols, pk):
    decl = ", ".join(
        f"`{c}` STRING" if t == "VARCHAR" else f"`{c}` {t}" for c, t in cols
    )
    keys = ", ".join(f"`{c}`" for c in pk)
    return (
        f"CREATE TABLE {name} ({decl}, PRIMARY KEY ({keys}) NOT ENFORCED) WITH (\n"
        f"  'connector' = 'upsert-kafka',\n"
        f"  'topic' = '__OUT__',\n"
        f"  'properties.bootstrap.servers' = 'kafka:29092',\n"
        f"  'key.format' = 'json',\n"
        f"  'value.format' = 'json'\n"
        f");"
    )


def clink_sink(name, cols, kafka=False):
    decl = ", ".join(f"{c} {t}" for c, t in cols)
    if not kafka:
        return f"CREATE TABLE {name} ({decl}) WITH (connector='blackhole');"
    return (
        f"CREATE TABLE {name} ({decl})\n"
        f"  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='__OUT__');"
    )


def flink_sink(name, cols, kafka=False):
    decl = ", ".join(
        f"`{c}` STRING" if t == "VARCHAR" else f"`{c}` {t}" for c, t in cols
    )
    if not kafka:
        return f"CREATE TABLE {name} ({decl}) WITH ('connector' = 'blackhole');"
    return (
        f"CREATE TABLE {name} ({decl}) WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '__OUT__',\n"
        f"  'properties.bootstrap.servers' = 'kafka:29092',\n"
        f"  'format' = 'json',\n"
        f"  'sink.delivery-guarantee' = 'at-least-once'\n"
        f");"
    )


# ---------------------------------------------------------------------------
# The queries.
#
# `note`     - what the query measures, and any documented reduction from the
#              official nexmark definition. Reductions are stated, never silent.
# `streams`  - which sources to declare.
# `sink`     - output column declaration, identical on both engines.
# `clink`    - the clink SELECT body.
# `flink`    - the Flink SELECT body. Same semantics; dialect differences only.
# ---------------------------------------------------------------------------

QUERIES = {
    "q1": dict(
        note="Currency conversion. Per-record projection with a float multiply.",
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "DOUBLE"),
              ("datetime", "BIGINT")],
        clink="SELECT auction, bidder, price * 0.908, datetime FROM bid",
        flink="SELECT auction, bidder, price * 0.908, `datetime` FROM bid",
    ),
    "q2": dict(
        note="Selection. A filter on an EXPRESSION of a column, not the column.",
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("price", "BIGINT")],
        clink="SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0",
        flink="SELECT auction, price FROM bid WHERE MOD(auction, 123) = 0",
    ),
    "q5": dict(
        note=("Hot items: the single most-bid-on auction per 10s sliding window, "
              "advancing 2s. Sliding window aggregate feeding a top-1 rank. NOTE the "
              "argument orders differ and are NOT interchangeable: clink is "
              "HOP(time, SIZE, SLIDE) while Flink's table function is "
              "HOP(TABLE, DESCRIPTOR, SLIDE, SIZE). Getting it backwards makes slide "
              "exceed size, which the operator rejects."),
        streams=["bid"],
        # wstart is projected for the UPSERT variant's benefit: the changelog
        # revises one row per window, so the window start IS the primary key, and
        # without it in the output there is nothing to key an upsert on.
        sink=[("wstart", "BIGINT"), ("auction", "BIGINT"), ("num", "BIGINT")],
        pk=["wstart"],
        # Two dialect differences here, both found by running it.
        #
        # `window_start` is BIGINT epoch millis in clink and TIMESTAMP(3) in
        # Flink, so projecting it straight into a BIGINT sink column made Flink
        # refuse the job outright: "Incompatible types for sink column 'wstart'".
        # The conversion below is the one q8 already uses and gates on.
        #
        # ORDER BY num DESC alone is not TOTAL: two auctions tied on count within
        # a window have no defined winner, and top-1 then picks arbitrarily per
        # engine. auction breaks it - it is unique within a window group. This is
        # the same defect q19 had, pre-empted here rather than waited for.
        clink=("SELECT wstart, auction, num FROM ("
               "SELECT *, ROW_NUMBER() OVER (PARTITION BY wstart ORDER BY num DESC, "
               "auction ASC) AS rn FROM ("
               "SELECT auction, COUNT(*) AS num, window_start AS wstart FROM bid "
               "GROUP BY HOP(datetime, INTERVAL '10' SECOND, INTERVAL '2' SECOND), auction"
               ") AS W) AS R WHERE rn <= 1"),
        flink=("SELECT wstart, auction, num FROM ("
               "SELECT *, ROW_NUMBER() OVER (PARTITION BY wstart ORDER BY num DESC, "
               "auction ASC) AS rn FROM ("
               "SELECT auction, COUNT(*) AS num, "
               "CAST(UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 AS BIGINT) AS wstart "
               "FROM TABLE(HOP(TABLE bid, DESCRIPTOR(ts), INTERVAL '2' SECOND, "
               "INTERVAL '10' SECOND)) GROUP BY window_start, window_end, auction"
               ") AS W) AS R WHERE rn <= 1"),
    ),
    "q7": dict(
        note=("Highest bid per 10s tumbling window. REDUCED from the official q7, "
              "which joins the window maximum back to the bid stream to recover the "
              "bidder; this keeps the windowed MAX/MIN aggregate only, so both "
              "engines run the same shape."),
        streams=["bid"],
        sink=[("price", "BIGINT"), ("bidder", "BIGINT")],
        clink=("SELECT MAX(price) AS price, MIN(bidder) AS bidder FROM bid "
               "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND)"),
        flink=("SELECT MAX(price) AS price, MIN(bidder) AS bidder FROM "
               "TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
               "GROUP BY window_start, window_end"),
    ),
    "q11": dict(
        note="User sessions: bids per bidder per 10s-gap session window.",
        streams=["bid"],
        sink=[("bidder", "BIGINT"), ("bid_count", "BIGINT")],
        clink=("SELECT bidder, COUNT(*) AS bid_count FROM bid "
               "GROUP BY SESSION(datetime, INTERVAL '10' SECOND), bidder"),
        flink=("SELECT bidder, COUNT(*) AS bid_count FROM "
               "TABLE(SESSION(TABLE bid PARTITION BY bidder, DESCRIPTOR(ts), "
               "INTERVAL '10' SECOND)) GROUP BY window_start, window_end, bidder"),
    ),
    # --- window-kind coverage. NOT nexmark queries.
    #
    # The gate is meant to cover every window kind, and on the nexmark set alone it
    # cannot. TUMBLE has q12 and SESSION has q11, both bare windowed aggregates the
    # row-count gate can carry. HOP's only nexmark query is q5, which wraps it in a
    # top-1 rank and therefore emits a CHANGELOG, so it can only be gated by
    # upsert_gate.sh - and the last run there produced zero rows on both engines, so
    # HOP has never actually been compared. CUMULATE appears nowhere in nexmark at
    # all.
    #
    # These two put both kinds on the same footing as q11 and q12: a bare aggregate
    # over the window, append-only, row-count comparable. Same shape as q11
    # deliberately, so a count difference is attributable to the window and nothing
    # else.
    #
    # The window BOUNDS are not projected. The gate compares row counts, so a bound
    # column would add nothing it checks, while requiring each dialect to render a
    # window timestamp into a BIGINT sink column - a conversion difference that would
    # show up as a submit failure rather than as anything about windowing. Bounds are
    # pinned in-suite instead, against an independent oracle, by
    # NexmarkQueries.CumulateWindowPanes.
    "qhop": dict(
        note=("NOT a nexmark query. Bids per auction per 10s window sliding every "
              "2s, so each bid lands in five overlapping panes. HOP's only nexmark "
              "query is q5, whose top-1 rank makes it a changelog the row-count gate "
              "cannot carry. NOTE the argument orders differ and are NOT "
              "interchangeable: clink is HOP(time, SIZE, SLIDE), Flink's table "
              "function is HOP(TABLE, DESCRIPTOR, SLIDE, SIZE)."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("num", "BIGINT")],
        clink=("SELECT auction, COUNT(*) AS num FROM bid "
               "GROUP BY HOP(datetime, INTERVAL '10' SECOND, INTERVAL '2' SECOND), auction"),
        flink=("SELECT auction, COUNT(*) AS num FROM "
               "TABLE(HOP(TABLE bid, DESCRIPTOR(ts), INTERVAL '2' SECOND, "
               "INTERVAL '10' SECOND)) GROUP BY window_start, window_end, auction"),
    ),
    "qhopv": dict(
        note=("NOT a nexmark query. qhop's windowed counts with the window start "
              "projected and a primary key, so upsert_gate.sh compares the "
              "VALUES rather than the row count. This exists because a window "
              "that fires early emits the same NUMBER of panes with lower counts "
              "in them, so no row-count gate can see an undercount - which is the "
              "failure mode a multi-partition watermark produces, and the one "
              "34819d4 fixed once already. This is the check that would notice it "
              "coming back."),
        streams=["bid"],
        sink=[("wstart", "BIGINT"), ("auction", "BIGINT"), ("num", "BIGINT")],
        pk=["wstart", "auction"],
        clink=("SELECT window_start AS wstart, auction, COUNT(*) AS num FROM bid "
               "GROUP BY HOP(datetime, INTERVAL '10' SECOND, INTERVAL '2' SECOND), auction"),
        flink=("SELECT CAST(UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 AS BIGINT) "
               "AS wstart, auction, COUNT(*) AS num FROM "
               "TABLE(HOP(TABLE bid, DESCRIPTOR(ts), INTERVAL '2' SECOND, "
               "INTERVAL '10' SECOND)) GROUP BY window_start, window_end, auction"),
    ),
    "qcum": dict(
        note=("NOT a nexmark query. Cumulative bids per bidder over a 10s window "
              "stepping 2s: the panes share a start and end at successive steps, so "
              "a bid contributes to every pane of its window that ends after it and "
              "the pane count depends on where in the window it falls. Here the two "
              "dialects agree on argument order - clink is CUMULATE(time, STEP, "
              "SIZE) and Flink is CUMULATE(TABLE, DESCRIPTOR, STEP, SIZE) - which is "
              "the opposite situation to HOP above, and the reason each is written "
              "out per dialect rather than assumed to follow the other."),
        streams=["bid"],
        sink=[("bidder", "BIGINT"), ("num", "BIGINT")],
        clink=("SELECT bidder, COUNT(*) AS num FROM bid "
               "GROUP BY CUMULATE(datetime, INTERVAL '2' SECOND, INTERVAL '10' SECOND), bidder"),
        flink=("SELECT bidder, COUNT(*) AS num FROM "
               "TABLE(CUMULATE(TABLE bid, DESCRIPTOR(ts), INTERVAL '2' SECOND, "
               "INTERVAL '10' SECOND)) GROUP BY window_start, window_end, bidder"),
    ),
    "q14": dict(
        note=("Calculation. Expression filter plus a CASE projection - the query "
              "that is almost all scalar expression evaluation."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "DOUBLE"),
              ("bidtimetype", "VARCHAR")],
        # The official query filters on price > 1000000, which excludes EVERY row
        # of the generated stream at the sizes this harness runs - the output gate
        # then compared 0 rows against 0 rows and proved nothing. The threshold is
        # lowered so a substantial fraction survives and the filter is actually
        # exercised on both engines.
        clink=("SELECT auction, bidder, 0.908 * price AS price, "
               "CASE WHEN MOD(datetime, 2) = 0 THEN 'even' ELSE 'odd' END AS bidtimetype "
               "FROM bid WHERE 0.908 * price > 250"),
        flink=("SELECT auction, bidder, 0.908 * price AS price, "
               "CASE WHEN MOD(`datetime`, 2) = 0 THEN 'even' ELSE 'odd' END AS bidtimetype "
               "FROM bid WHERE 0.908 * price > 250"),
    ),
    "q15": dict(
        note=("Bidding statistics: COUNT plus two COUNT(DISTINCT) per 10s window. "
              "The official query groups by calendar day; a 10s window is the same "
              "distinct-set-per-group shape at a size the harness can drain."),
        streams=["bid"],
        sink=[("total", "BIGINT"), ("distinct_bidder", "BIGINT"),
              ("distinct_auction", "BIGINT")],
        clink=("SELECT COUNT(*) AS total, COUNT(DISTINCT bidder) AS distinct_bidder, "
               "COUNT(DISTINCT auction) AS distinct_auction FROM bid "
               "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND)"),
        flink=("SELECT COUNT(*) AS total, COUNT(DISTINCT bidder) AS distinct_bidder, "
               "COUNT(DISTINCT auction) AS distinct_auction FROM "
               "TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
               "GROUP BY window_start, window_end"),
    ),
    "q16": dict(
        note="Channel statistics: the same distinct counting, keyed by channel.",
        streams=["bid"],
        sink=[("channel", "VARCHAR"), ("total", "BIGINT"),
              ("distinct_bidder", "BIGINT")],
        clink=("SELECT channel, COUNT(*) AS total, COUNT(DISTINCT bidder) AS distinct_bidder "
               "FROM bid GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), channel"),
        flink=("SELECT channel, COUNT(*) AS total, COUNT(DISTINCT bidder) AS distinct_bidder "
               "FROM TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
               "GROUP BY window_start, window_end, channel"),
    ),
    "q17": dict(
        note=("Auction statistics: five aggregates per auction per 10s window. "
              "Flink casts the price to DOUBLE inside AVG for the same reason q4 "
              "does: AVG of an exact numeric type truncates there, while clink's "
              "AVG already returns a real number. The row-count gate never saw "
              "the difference; a value-level comparison does."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("total", "BIGINT"), ("minp", "BIGINT"),
              ("maxp", "BIGINT"), ("avgp", "DOUBLE")],
        clink=("SELECT auction, COUNT(*) AS total, MIN(price) AS minp, MAX(price) AS maxp, "
               "AVG(price) AS avgp FROM bid "
               "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), auction"),
        flink=("SELECT auction, COUNT(*) AS total, MIN(price) AS minp, MAX(price) AS maxp, "
               "AVG(CAST(price AS DOUBLE)) AS avgp FROM "
               "TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
               "GROUP BY window_start, window_end, auction"),
    ),
    "q18": dict(
        note=("Latest bid per bidder/auction pair: deduplication by ROW_NUMBER = 1 "
              "over a descending time order. Unbounded keyspace, one retained row "
              "per key. The ORDER BY is made TOTAL for the same reason as q5 and "
              "q19: two bids by one bidder on one auction in the same millisecond "
              "have no defined 'latest', and each engine would retain an arbitrary "
              "one. price then url break the tie - together with the partition key "
              "and datetime they identify a bid in this dataset."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "BIGINT"),
              ("channel", "VARCHAR"), ("url", "VARCHAR"), ("datetime", "BIGINT")],
        pk=["bidder", "auction"],
        clink=("SELECT * FROM (SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY bidder, auction "
               "ORDER BY datetime DESC, price DESC, url DESC) AS rn FROM bid) AS R "
               "WHERE rn = 1"),
        flink=("SELECT auction, bidder, price, channel, url, `datetime` FROM "
               "(SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY bidder, auction "
               "ORDER BY `datetime` DESC, price DESC, url DESC) AS rn FROM bid) AS R "
               "WHERE rn = 1"),
    ),
    "q19": dict(
        note=("Local bid ranking: the top 10 bids per auction by price. Ranked "
              "state per key, retracting as higher bids arrive."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "BIGINT"),
              ("channel", "VARCHAR"), ("url", "VARCHAR"), ("datetime", "BIGINT")],
        # The bid's own identity, not (auction, rn): clink's top-N drops the
        # synthetic rank column from its output, so rn is not available to key on.
        # A bid is uniquely identified by these four in this dataset.
        pk=["auction", "bidder", "price", "datetime"],
        # The ORDER BY must be TOTAL or this query has no single right answer.
        # With `price DESC` alone, two bids on one auction at the same price are
        # tied and which one makes the top 10 is undefined - measured 2026-07-28
        # as 3 differing rows out of 215,613, with the (auction, price) multiset
        # IDENTICAL on both engines, so the engines agreed on every pair and
        # disagreed only on which tied bid carried it. datetime and bidder break
        # the tie; together with auction and price they are this dataset's bid
        # identity, which is the same tuple the `pk` below uses.
        clink=("SELECT * FROM (SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY auction ORDER BY price DESC, datetime DESC, bidder DESC) "
               "AS rn FROM bid) AS R WHERE rn <= 10"),
        flink=("SELECT auction, bidder, price, channel, url, `datetime` FROM "
               "(SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY auction ORDER BY price DESC, `datetime` DESC, bidder DESC) "
               "AS rn FROM bid) AS R WHERE rn <= 10"),
    ),
    "q21": dict(
        note="Add channel id: a CASE over a string column, per record.",
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "BIGINT"),
              ("channel", "VARCHAR")],
        clink=("SELECT auction, bidder, price, CASE WHEN channel = 'apple' THEN '0' "
               "WHEN channel = 'google' THEN '1' WHEN channel = 'facebook' THEN '2' "
               "WHEN channel = 'baidu' THEN '3' ELSE channel END AS channel FROM bid"),
        flink=("SELECT auction, bidder, price, CASE WHEN channel = 'apple' THEN '0' "
               "WHEN channel = 'google' THEN '1' WHEN channel = 'facebook' THEN '2' "
               "WHEN channel = 'baidu' THEN '3' ELSE channel END AS channel FROM bid"),
    ),
    "q22": dict(
        note="URL directory extraction: string splitting, per record.",
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "BIGINT"),
              ("dir", "VARCHAR")],
        clink=("SELECT auction, bidder, price, SPLIT_INDEX(url, '/', 3) AS dir FROM bid"),
        flink=("SELECT auction, bidder, price, SPLIT_INDEX(url, '/', 3) AS dir FROM bid"),
    ),
    # --- multi-stream. Small inputs (auction and person are a few percent of the
    # --- bid stream), so these belong to the correctness gate rather than the
    # --- sustained-throughput sweep, but the shape coverage is the point.
    "q3": dict(
        note=("Local item suggestion: auction joined to person on seller, filtered "
              "to one category. Stream-stream equi-join."),
        streams=["auction", "person"],
        sink=[("name", "VARCHAR"), ("city", "VARCHAR"), ("state", "VARCHAR"),
              ("id", "BIGINT")],
        clink=("SELECT P.name, P.city, P.state, A.id FROM auction AS A "
               "JOIN person AS P ON A.seller = P.id WHERE A.category = 10"),
        flink=("SELECT P.name, P.city, P.state, A.id FROM auction AS A "
               "JOIN person AS P ON A.seller = P.id WHERE A.category = 10"),
    ),
    "q4": dict(
        note=("Average price per category: auction joined to bid, then aggregated. "
              "REDUCED from the official q4, which first takes the maximum bid per "
              "auction; this averages every bid in the category, so both engines "
              "run one join plus one aggregate. Flink casts the price to DOUBLE "
              "explicitly and clink does not, which is a DIALECT difference for the "
              "same intent: AVG of an exact numeric type returns an exact numeric "
              "type in Flink, so AVG(BIGINT) truncates - measured at 49975.0 where "
              "clink gives 49975.9 - while clink's AVG already returns a real "
              "number. clink cannot take the cast inside AVG (its AVG accepts only "
              "a bare column reference), so the cast goes where it is needed."),
        streams=["auction", "bid"],
        sink=[("category", "BIGINT"), ("avgp", "DOUBLE")],
        pk=["category"],
        clink=("SELECT A.category AS category, AVG(B.price) AS avgp FROM auction AS A "
               "JOIN bid AS B ON A.id = B.auction GROUP BY A.category"),
        flink=("SELECT A.category AS category, AVG(CAST(B.price AS DOUBLE)) AS avgp "
               "FROM auction AS A JOIN bid AS B ON A.id = B.auction GROUP BY A.category"),
    ),
    "q20": dict(
        note="Expand bid with auction: bid joined to auction on the auction id.",
        streams=["bid", "auction"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "BIGINT"),
              ("itemname", "VARCHAR")],
        clink=("SELECT B.auction, B.bidder, B.price, A.itemname FROM bid AS B "
               "JOIN auction AS A ON B.auction = A.id"),
        flink=("SELECT B.auction, B.bidder, B.price, A.itemname FROM bid AS B "
               "JOIN auction AS A ON B.auction = A.id"),
    ),
}


def render(q, engine, kafka=False, upsert=False):
    d = QUERIES[q]
    # Distinct consumer groups per variant: a blackhole run and a Kafka-sink run
    # of the same query must not share an offset commit position.
    tag = (q + "up") if upsert else (q if kafka else f"{q}bh")
    sink_name = f"sink_{q}"
    src = clink_source if engine == "clink" else flink_source
    snk = clink_sink if engine == "clink" else flink_sink
    which = "UPSERT-SINK" if upsert else ("KAFKA-SINK" if kafka else "BLACKHOLE")
    header = textwrap.fill(
        f"Nexmark {q} on {engine}, {which} sink variant. {d['note']}",
        width=78,
        initial_indent="-- ",
        subsequent_indent="-- ",
    )
    parts = [
        header,
        "-- GENERATED by queries/gen_queries.py from the single cross-engine",
        "-- definition. Edit that file, not this one.",
    ]
    parts += [src(s, tag) for s in d["streams"]]
    if upsert:
        up = clink_upsert_sink if engine == "clink" else flink_upsert_sink
        parts.append(up(sink_name, d["sink"], d["pk"]))
    else:
        parts.append(snk(sink_name, d["sink"], kafka))
    parts.append(f"INSERT INTO {sink_name}\n{d[engine]};")
    return "\n".join(parts) + "\n"


def main():
    os.makedirs(FLINK_DIR, exist_ok=True)
    n = 0
    for q in QUERIES:
        for engine, out_dir in (("clink", CLINK_DIR), ("flink", FLINK_DIR)):
            for kafka, suffix in ((False, "_bh"), (True, "")):
                path = os.path.join(out_dir, f"{q}{suffix}.tmpl.sql")
                with open(path, "w") as f:
                    f.write(render(q, engine, kafka))
                n += 1
            if QUERIES[q].get("pk"):
                path = os.path.join(out_dir, f"{q}_up.tmpl.sql")
                with open(path, "w") as f:
                    f.write(render(q, engine, kafka=True, upsert=True))
                n += 1
    print(f"wrote {n} templates for {len(QUERIES)} queries "
          f"(blackhole + kafka-sink, both engines)")
    print("bid-only (sustained-throughput candidates):",
          " ".join(q for q, d in QUERIES.items() if d["streams"] == ["bid"]))
    print("multi-stream (gate candidates):",
          " ".join(q for q, d in QUERIES.items() if d["streams"] != ["bid"]))


if __name__ == "__main__":
    main()
