"""Live Kafka round trip through an installed pyclink wheel.

Run against a broker by the wheels workflow's verify job, from outside the
source tree and with CLINK_LIB unset, so the library under test is the one the
wheel bundles. The broker is observed from outside by rpk on both sides:

  consume  topic `wheel-in` holds the five JSON records rpk produced before this
           ran; pyclink reads them through connector='kafka' into a collect
           table and must see exactly (1, 'r1') .. (5, 'r5').
  produce  pyclink writes five rows from a file source into topic `wheel-out`
           through connector='kafka'; the job then counts them with rpk.

    BROKERS=127.0.0.1:19092 python wheel_kafka_live.py
"""

import json
import os
import sys
import tempfile
import time

import pyclink

BROKERS = os.environ.get("BROKERS", "127.0.0.1:19092")


def consume() -> None:
    engine = pyclink.Engine()
    engine.execute(f"""
        CREATE TABLE src (id BIGINT, v STRING) WITH (
          connector='kafka', format='json', brokers='{BROKERS}', topic='wheel-in',
          group_id='wheel-verify', auto_offset_reset='earliest');
        CREATE TABLE sink (id BIGINT, v STRING) WITH (connector='collect');
        INSERT INTO sink SELECT id, v FROM src
    """)
    reader = engine.collect("sink")
    got = []
    deadline = time.monotonic() + 60
    while len(got) < 5 and time.monotonic() < deadline:
        batch = reader.read_next_batch()
        got += list(zip(batch.column("id").to_pylist(), batch.column("v").to_pylist()))
    # The source is unbounded; cancelling it may report the job as failed, which
    # is not what this test is about.
    engine.cancel_all()
    try:
        engine.await_all(10000)
    except Exception:
        pass
    engine.close()
    got.sort()
    want = [(i, f"r{i}") for i in range(1, 6)]
    print("consumed:", got)
    if got != want:
        sys.exit(f"wheel_kafka_live: consumed {got}, expected {want}")


def produce() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "rows.ndjson")
        with open(path, "w") as fh:
            for i in range(1, 6):
                fh.write(json.dumps({"id": 100 + i, "v": f"p{i}"}) + "\n")
        with pyclink.Engine() as engine:
            engine.execute(f"""
                CREATE TABLE f (id BIGINT, v STRING) WITH (
                  connector='file', format='json', path='{path}');
                CREATE TABLE k (id BIGINT, v STRING) WITH (
                  connector='kafka', format='json', brokers='{BROKERS}', topic='wheel-out');
                INSERT INTO k SELECT id, v FROM f
            """)
            engine.await_all(60000)
    print("produced 5 rows to wheel-out")


if __name__ == "__main__":
    if os.environ.get("CLINK_LIB"):
        sys.exit("wheel_kafka_live: unset CLINK_LIB so the wheel's own library is tested")
    consume()
    produce()
    print("wheel_kafka_live OK")
