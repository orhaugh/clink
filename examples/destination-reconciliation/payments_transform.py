"""The payments job's transformation, written again as a reference.

One Kafka record's JSON value in, the rows the job should write for it out,
as Python values the reference library encodes by column type. It is written
from the job's specification (README.md, "The job"), not from its SQL, and uses
neither clink's evaluator nor its sink's conversions: Python's json and
decimal do the work. It is the example's business logic, kept apart from the
reading and writing in clink_reference.
"""

import json
from decimal import Decimal

NAME = "payments-reference"
VERSION = "1"
RULES = {
    "filter": "kind is purchase or refund, and amount is at least 1.00",
    "amount_cents": "amount times 100, exactly",
    "fee": "amount_cents as a double, times 0.029",
    "flagged": "amount is at least 1000",
    "occurred_at": "ts, epoch milliseconds",
}


def transform(value):
    """The rows for one record's value (bytes or text)."""
    v = json.loads(value, parse_float=Decimal)
    amount = Decimal(v["amount"])
    if v["kind"] not in ("purchase", "refund") or amount < Decimal("1.00"):
        return []
    cents = amount * 100
    if cents != cents.to_integral_value():
        raise ValueError("event %s's amount %s is not whole cents" % (v["event_id"], amount))
    return [{
        "event_id": int(v["event_id"]),
        "account": v["account"],
        "kind": v["kind"],
        "amount": amount,
        "amount_cents": int(cents),
        "fee": float(int(cents)) * 0.029,
        "flagged": amount >= 1000,
        "note": v["note"],
        "occurred_at": int(v["ts"]),
    }]
