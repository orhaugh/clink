"""The example's synthetic payments and the independent reference for them.

Every event is a pure function of its id, so the feed and the reference agree
without sharing anything but this file. reference_row() is the job's
transformation written again, from the event's own fields, in the canonical
cells clink incident-reconcile compares (see canonical() below): it never
reads clink's output, its captures or the destination.
"""
import decimal
import json
import random
import struct

TOPIC_PARTITIONS = 2
BASE_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
KINDS = ["purchase"] * 10 + ["refund"] * 3 + ["view"] * 5 + ["chargeback"] * 2
NOTES = [None, None, None, "", "plain", "quote ' and \\ backslash", "tab\tand\nnewline",
         "unicode é€\U0001F600", "x" * 300]


def event(i):
    """Event `i` (from 1): a dict, and its exact amount in cents."""
    r = random.Random(i * 2654435761 % (1 << 32))
    kind = r.choice(KINDS)
    cents = r.randrange(0, 250_000)
    return {
        "event_id": i,
        "account": "acct-%02d" % r.randrange(40),
        "kind": kind,
        "cents": cents,
        "ts": BASE_MS + i * 37 + r.randrange(0, 1000),
        "note": r.choice(NOTES),
    }


def record(i):
    """The Kafka record for event `i`: key, and the JSON value with the amount
    written as an exact two-decimal number."""
    e = event(i)
    amount = "%d.%02d" % divmod(e["cents"], 100)
    note = "null" if e["note"] is None else json.dumps(e["note"])
    value = ('{"event_id":%d,"account":"%s","kind":"%s","amount":%s,"ts":%d,"note":%s}'
             % (e["event_id"], e["account"], e["kind"], amount, e["ts"], note))
    return e["account"], value


def f64(x):
    if x != x:
        return "Fnan"
    return "F%016x" % struct.unpack(">Q", struct.pack(">d", x))[0]


def reference_row(e):
    """The row the job owes the destination for event `e`, or None when its
    filter drops it, as canonical cells keyed by column."""
    if e["kind"] not in ("purchase", "refund") or e["cents"] < 100:
        return None
    return {
        "event_id": "I%d" % e["event_id"],
        "account": "S" + e["account"],
        "kind": "S" + e["kind"],
        "amount": "D%d@2" % e["cents"],
        "amount_cents": "I%d" % e["cents"],
        "fee": f64(float(e["cents"]) * 0.029),
        "flagged": "B1" if e["cents"] >= 100_000 else "B0",
        "note": "N" if e["note"] is None else "S" + e["note"],
        "occurred_at": "T%d@3" % e["ts"],
    }


def reference_from_value(value_json):
    """reference_row() of an event read back from Kafka, by its own fields."""
    v = json.loads(value_json, parse_float=decimal.Decimal)
    cents = int(decimal.Decimal(v["amount"]) * 100)
    return reference_row({"event_id": v["event_id"], "account": v["account"], "kind": v["kind"],
                          "cents": cents, "ts": v["ts"], "note": v["note"]})
