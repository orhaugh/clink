#!/usr/bin/env python3
"""Load the example's reference graph: synthetic devices and which synthetic
accounts have used them, as scripts/workload.py defines them.

    ./scripts/seed_reference.py

run.sh calls it once, before the pipeline starts, so every enrichment the
pipeline makes reads relationships that were already in the graph. MERGE on
the ids under the uniqueness constraints in schema.cypher, so running it again
changes nothing.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cypher  # noqa: E402
import workload  # noqa: E402


def main() -> int:
    links = [{"account": a, "device": d} for a, d in workload.DEVICE_LINKS]
    cypher.run("UNWIND $links AS l "
               "MERGE (a:Account {id: l.account}) "
               "MERGE (d:Device {id: l.device}) "
               "MERGE (a)-[:USES_DEVICE]->(d)",
               {"links": links})
    devices = cypher.run("MATCH (d:Device) RETURN count(d)")[0][0]
    uses = cypher.run("MATCH (:Account)-[u:USES_DEVICE]->(:Device) RETURN count(u)")[0][0]
    print(f"seed_reference: {devices} devices, {uses} USES_DEVICE relationships")
    return 0


if __name__ == "__main__":
    sys.exit(main())
