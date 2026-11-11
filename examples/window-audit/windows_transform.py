"""The windowed job's specification, written again as a reference.

What a window is, how it closes and what it holds, from the job's
specification (README.md, "The job"), not from its SQL: Python's json and
decimal do the arithmetic, and clink_reference.TumblingWindows applies the
window rules declared here. It uses neither clink's SQL evaluator, nor its
aggregates, nor its sink's conversions.
"""

import json
from decimal import Decimal

from clink_reference import WindowRules

NAME = "account-windows-reference"
VERSION = "1"
# Ten-second tumbling windows on the event's own time, the watermark two
# seconds behind the slowest partition, and a record for a closed window
# dropped.
RULES = WindowRules(size_ms=10_000, lag_ms=2_000, time_column="ts", allowed_lateness_ms=0)
AGGREGATES = [("events", "count"), ("net", "sum"), ("units", "sum")]
SPEC = {
    "window": "per account, ten seconds of the event's own time (ts, epoch milliseconds), "
              "[start, start + 10000)",
    "rows": "purchases and refunds; chargebacks are not payments",
    "events": "the window's purchases and refunds",
    "net": "purchases less refunds, exactly, in the amount's own decimals",
    "units": "the units of those payments",
    "watermark": "two seconds behind the least, over partitions, of the latest ts each has "
                 "delivered, every event counting, chargebacks included",
    "late": "an event for a window that has closed is dropped",
}


def event_time(value):
    """The event time every record advances the watermark by."""
    return int(json.loads(value)["ts"])


def select(value):
    """The record's part in its window: None when it is not a payment, else
    (key, {aggregate: value})."""
    v = json.loads(value, parse_float=Decimal)
    if v["kind"] not in ("purchase", "refund"):
        return None
    amount = Decimal(v["amount"])
    return ((v["account"],), {"net": -amount if v["kind"] == "refund" else amount,
                              "units": int(v["units"])})


def output(key, start, end, aggregates):
    return {"account": key[0], "window_start": start, "window_end": end,
            "events": aggregates["events"], "net": aggregates["net"],
            "units": aggregates["units"]}
