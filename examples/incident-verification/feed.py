#!/usr/bin/env python3
"""A paced event feed for the incident-verification example.

Serves GET /events?after=<seq> with the records strictly after `after`,
ascending by `seq`, at most --page per response: the contract the HTTP polling
source's cursor mode reads (docs/connectors/http.md). Records are released at
--rate per second from the moment the server starts, so the job sees a steady
stream and takes checkpoints along the way. A request that finds nothing
released yet waits up to five seconds for the next record before it answers
an empty page.

Every record is a pure function of its sequence number (see record()), so a
restored job that asks again from its checkpointed cursor gets the same bytes,
and oracle.py can compute the expected result without reading anything the
job wrote.

Usage: feed.py --port=N --records=N --rate=N [--page=N]
"""

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


def record(seq, records):
    """Record `seq` of a feed of `records` records, then one more: a record far
    ahead in event time that the job's filter drops, so the watermark it
    carries closes every window while the job keeps running."""
    if seq == records + 1:
        return {"seq": seq, "usr": "end", "ts": (records + 1) * 37 + 3_600_000, "amount": 0}
    return {"seq": seq, "usr": "u%d" % (seq % 7), "ts": seq * 37, "amount": (seq * 31) % 97}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--records", type=int, required=True)
    ap.add_argument("--rate", type=float, required=True)
    ap.add_argument("--page", type=int, default=500)
    args = ap.parse_args()
    started = time.monotonic()

    total = args.records + 1

    def released():
        return min(total, int((time.monotonic() - started) * args.rate))

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            url = urlparse(self.path)
            if url.path != "/events":
                self.send_error(404)
                return
            try:
                after = int(parse_qs(url.query).get("after", ["0"])[0])
            except ValueError:
                self.send_error(400)
                return
            deadline = time.monotonic() + 5.0
            while after < total and released() <= after and time.monotonic() < deadline:
                time.sleep(0.005)
            last = min(released(), after + args.page)
            body = json.dumps([record(s, args.records) for s in range(after + 1, last + 1)]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
