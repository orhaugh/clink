#!/usr/bin/env python3
"""Generate the fat-fingered-order incident used by the agent-diagnosis guide.

    python3 examples/agent-diagnosis/make_incident.py /tmp/incident
    clink run /tmp/incident/job.sql --checkpoint-dir=/tmp/incident/ckpt \
        --checkpoint-interval-ms=100 --capture-dir=/tmp/incident/capture \
        --capture-records=100000

Writes two files into the directory given: `orders.ndjson`, ten minutes of
small orders from eight users with ONE order whose amount was typed as
4990000 instead of 4990, and `job.sql`, a running total of spend per user
over that file, with absolute paths so it runs from anywhere. The data is
deterministic (seeded), so the numbers in the guide reproduce exactly.

Standard library only; no clink Python package needed.
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

FAT_FINGER = 4_990_000  # meant to be 4,990


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: make_incident.py <directory>", file=sys.stderr)
        return 2
    root = Path(argv[1]).resolve()
    root.mkdir(parents=True, exist_ok=True)

    rng = random.Random(7)
    users = ["ana", "bo", "chen", "dana", "eli", "fay", "gus", "hana"]
    rows = []
    for sec in range(600):
        for u in users:
            rows.append(
                {"usr": u, "ts": sec * 1000 + rng.randint(0, 999), "amount": rng.randint(5, 60)}
            )
    rows.append({"usr": "dana", "ts": 421_420, "amount": FAT_FINGER})
    rows.sort(key=lambda r: r["ts"])

    orders = root / "orders.ndjson"
    orders.write_text("".join(json.dumps(r) + "\n" for r in rows))
    (root / "job.sql").write_text(
        "-- A running total of spend per user. The sink receives every update,\n"
        "-- so the last line for a user is that user's current total.\n"
        "CREATE TABLE orders (usr TEXT, ts BIGINT, amount BIGINT)\n"
        f"  WITH (connector='file', path='{orders}', format='json');\n"
        "CREATE TABLE spend (usr TEXT, total BIGINT)\n"
        f"  WITH (connector='file', path='{root / 'spend.ndjson'}', format='json');\n"
        "INSERT INTO spend SELECT usr, SUM(amount) AS total FROM orders GROUP BY usr;\n"
    )
    print(f"{len(rows)} orders -> {orders}")
    print(f"job -> {root / 'job.sql'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
