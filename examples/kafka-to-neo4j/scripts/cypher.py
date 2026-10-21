#!/usr/bin/env python3
"""A minimal Neo4j Query API client, standard library only.

    ./scripts/cypher.py 'MATCH (s:Signal) RETURN s.id, s.distinct_senders'
    ./scripts/cypher.py --file schema.cypher    # each statement ending in ';'

Reads NEO4J_HTTP (default http://localhost:7474), NEO4J_USER (neo4j) and
NEO4J_PASSWORD (the example's, clink-example-pw). verify.py uses run() too.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import urllib.error
import urllib.request

ENDPOINT = os.environ.get("NEO4J_HTTP", "http://localhost:7474")
USER = os.environ.get("NEO4J_USER", "neo4j")
PASSWORD = os.environ.get("NEO4J_PASSWORD", "clink-example-pw")


def run(statement: str, parameters: dict | None = None) -> list:
    """The rows of `statement`, values as plain JSON. Raises on any error."""
    body = json.dumps({"statement": statement, "parameters": parameters or {}}).encode()
    req = urllib.request.Request(f"{ENDPOINT}/db/neo4j/query/v2", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    token = base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    req.add_header("Authorization", f"Basic {token}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            reply = json.load(resp)
    except urllib.error.HTTPError as e:
        reply = json.loads(e.read() or b"{}")
    if reply.get("errors"):
        raise RuntimeError(f"{reply['errors'][0].get('code')}: {reply['errors'][0].get('message')}")
    return reply.get("data", {}).get("values", [])


def statements(text: str) -> list:
    """The ';'-terminated statements of a file, comment lines dropped."""
    lines = [l for l in text.splitlines() if not l.strip().startswith("//")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--file":
        with open(sys.argv[2]) as f:
            for s in statements(f.read()):
                run(s)
        return 0
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    for row in run(sys.argv[1]):
        print("\t".join(json.dumps(v) for v in row))
    return 0


if __name__ == "__main__":
    sys.exit(main())
