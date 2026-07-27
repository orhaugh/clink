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

Only the BLACKHOLE variants are generated (`q*_bh.tmpl.sql`), which is what
throughput_sampled.sh runs: the sink is discarded so the measurement is the
engine's read-and-process rate rather than the output connector's ceiling.
The Kafka-sink variants used by the correctness gate stay hand-written, since
q0 / q12 are the only ones the gate covers.

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


def clink_source(name, tag):
    s = STREAMS[name]
    cols = ", ".join(f"{c} {t}" for c, t in s["cols"])
    return (
        f"CREATE TABLE {name} ({cols})\n"
        f"  WITH (connector='kafka', format='json', brokers='__BROKERS__', topic='{s['topic']}',\n"
        f"        group_id='clink-{tag}-{name}', auto_offset_reset='earliest',\n"
        f"        event_time_column='datetime', watermark_lag_ms='{WATERMARK_LAG_MS}');"
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


def clink_sink(name, cols):
    decl = ", ".join(f"{c} {t}" for c, t in cols)
    return f"CREATE TABLE {name} ({decl}) WITH (connector='blackhole');"


def flink_sink(name, cols):
    decl = ", ".join(
        f"`{c}` STRING" if t == "VARCHAR" else f"`{c}` {t}" for c, t in cols
    )
    return f"CREATE TABLE {name} ({decl}) WITH ('connector' = 'blackhole');"


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
              "advancing 2s. Sliding window aggregate feeding a top-1 rank."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("num", "BIGINT")],
        clink=("SELECT auction, num FROM ("
               "SELECT *, ROW_NUMBER() OVER (PARTITION BY wstart ORDER BY num DESC) AS rn FROM ("
               "SELECT auction, COUNT(*) AS num, window_start AS wstart FROM bid "
               "GROUP BY HOP(datetime, INTERVAL '2' SECOND, INTERVAL '10' SECOND), auction"
               ") AS W) AS R WHERE rn <= 1"),
        flink=("SELECT auction, num FROM ("
               "SELECT *, ROW_NUMBER() OVER (PARTITION BY wstart ORDER BY num DESC) AS rn FROM ("
               "SELECT auction, COUNT(*) AS num, window_start AS wstart FROM "
               "TABLE(HOP(TABLE bid, DESCRIPTOR(ts), INTERVAL '2' SECOND, INTERVAL '10' SECOND)) "
               "GROUP BY window_start, window_end, auction"
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
    "q14": dict(
        note=("Calculation. Expression filter plus a CASE projection - the query "
              "that is almost all scalar expression evaluation."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "DOUBLE"),
              ("bidtimetype", "VARCHAR")],
        clink=("SELECT auction, bidder, 0.908 * price AS price, "
               "CASE WHEN MOD(datetime, 2) = 0 THEN 'even' ELSE 'odd' END AS bidtimetype "
               "FROM bid WHERE 0.908 * price > 1000000"),
        flink=("SELECT auction, bidder, 0.908 * price AS price, "
               "CASE WHEN MOD(`datetime`, 2) = 0 THEN 'even' ELSE 'odd' END AS bidtimetype "
               "FROM bid WHERE 0.908 * price > 1000000"),
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
        note="Auction statistics: five aggregates per auction per 10s window.",
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("total", "BIGINT"), ("minp", "BIGINT"),
              ("maxp", "BIGINT"), ("avgp", "DOUBLE")],
        clink=("SELECT auction, COUNT(*) AS total, MIN(price) AS minp, MAX(price) AS maxp, "
               "AVG(price) AS avgp FROM bid "
               "GROUP BY TUMBLE(datetime, INTERVAL '10' SECOND), auction"),
        flink=("SELECT auction, COUNT(*) AS total, MIN(price) AS minp, MAX(price) AS maxp, "
               "AVG(price) AS avgp FROM "
               "TABLE(TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
               "GROUP BY window_start, window_end, auction"),
    ),
    "q18": dict(
        note=("Latest bid per bidder/auction pair: deduplication by ROW_NUMBER = 1 "
              "over a descending time order. Unbounded keyspace, one retained row "
              "per key."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "BIGINT"),
              ("channel", "VARCHAR"), ("url", "VARCHAR"), ("datetime", "BIGINT")],
        clink=("SELECT * FROM (SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY bidder, auction ORDER BY datetime DESC) AS rn FROM bid) AS R "
               "WHERE rn = 1"),
        flink=("SELECT auction, bidder, price, channel, url, `datetime` FROM "
               "(SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY bidder, auction ORDER BY `datetime` DESC) AS rn FROM bid) AS R "
               "WHERE rn = 1"),
    ),
    "q19": dict(
        note=("Local bid ranking: the top 10 bids per auction by price. Ranked "
              "state per key, retracting as higher bids arrive."),
        streams=["bid"],
        sink=[("auction", "BIGINT"), ("bidder", "BIGINT"), ("price", "BIGINT"),
              ("channel", "VARCHAR"), ("url", "VARCHAR"), ("datetime", "BIGINT")],
        clink=("SELECT * FROM (SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) AS R "
               "WHERE rn <= 10"),
        flink=("SELECT auction, bidder, price, channel, url, `datetime` FROM "
               "(SELECT *, ROW_NUMBER() OVER "
               "(PARTITION BY auction ORDER BY price DESC) AS rn FROM bid) AS R "
               "WHERE rn <= 10"),
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
              "run one join plus one aggregate."),
        streams=["auction", "bid"],
        sink=[("category", "BIGINT"), ("avgp", "DOUBLE")],
        clink=("SELECT A.category, AVG(B.price) AS avgp FROM auction AS A "
               "JOIN bid AS B ON A.id = B.auction GROUP BY A.category"),
        flink=("SELECT A.category, AVG(B.price) AS avgp FROM auction AS A "
               "JOIN bid AS B ON A.id = B.auction GROUP BY A.category"),
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


def render(q, engine):
    d = QUERIES[q]
    tag = f"{q}bh"
    sink_name = f"sink_{q}"
    src = clink_source if engine == "clink" else flink_source
    snk = clink_sink if engine == "clink" else flink_sink
    header = textwrap.fill(
        f"Nexmark {q} on {engine}, BLACKHOLE sink variant. {d['note']}",
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
    parts.append(snk(sink_name, d["sink"]))
    parts.append(f"INSERT INTO {sink_name}\n{d[engine]};")
    return "\n".join(parts) + "\n"


def main():
    os.makedirs(FLINK_DIR, exist_ok=True)
    n = 0
    for q in QUERIES:
        for engine, out_dir in (("clink", CLINK_DIR), ("flink", FLINK_DIR)):
            path = os.path.join(out_dir, f"{q}_bh.tmpl.sql")
            with open(path, "w") as f:
                f.write(render(q, engine))
            n += 1
    print(f"wrote {n} templates for {len(QUERIES)} queries")
    print("bid-only (sustained-throughput candidates):",
          " ".join(q for q, d in QUERIES.items() if d["streams"] == ["bid"]))
    print("multi-stream (gate candidates):",
          " ".join(q for q, d in QUERIES.items() if d["streams"] != ["bid"]))


if __name__ == "__main__":
    main()
