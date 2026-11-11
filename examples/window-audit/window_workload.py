"""The example's synthetic payments, laid out in event time on purpose.

Every event is generated with the fate the scenario designs for it: folded
into its window, dropped because its window had closed when it arrived, or
filtered out by the job (a chargeback). design_windows() computes the
windows those fates give, from the events alone, as a second derivation the
reference library's own classification is checked against: it shares no code
with clink or with clink_reference.

Events go to the partition the scenario chooses. Kafka's console producer
picks a keyed record's partition by the murmur2 hash of its key, so each
partition has a key that hashes to it (KEYS), and the scenarios read every
event back to confirm where it landed.
"""
import json
import random
from dataclasses import dataclass

PARTITIONS = 3
BASE_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
SIZE_MS = 10_000
LAG_MS = 2_000
ACCOUNTS = ["acct-%02d" % i for i in range(6)]


def murmur2(data):
    """Kafka's murmur2 (org.apache.kafka.common.utils.Utils.murmur2), as an
    unsigned 32-bit value."""
    m, r = 0x5BD1E995, 24
    h = (0x9747B28C ^ len(data)) & 0xFFFFFFFF
    n4 = len(data) // 4
    for i in range(n4):
        k = data[4 * i] | (data[4 * i + 1] << 8) | (data[4 * i + 2] << 16) | (data[4 * i + 3] << 24)
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    rest = len(data) % 4
    base = len(data) & ~3
    if rest == 3:
        h ^= data[base + 2] << 16
    if rest >= 2:
        h ^= data[base + 1] << 8
    if rest >= 1:
        h ^= data[base]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def partition_of(key, partitions=PARTITIONS):
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % partitions


def _keys():
    keys, i = {}, 0
    while len(keys) < PARTITIONS:
        k = "route-%d" % i
        keys.setdefault(partition_of(k), k)
        i += 1
    return keys


KEYS = _keys()


@dataclass
class Event:
    event_id: int
    partition: int
    account: str
    kind: str
    cents: int
    units: int
    ts: int        # event time, epoch milliseconds
    sent_ts: int   # when the device sent it, later than ts
    fate: str      # folded, dropped (its window closed before it arrived) or filtered

    def value(self):
        amount = "%s%d.%02d" % ("-" if self.cents < 0 else "", *divmod(abs(self.cents), 100))
        return ('{"event_id":%d,"account":"%s","kind":"%s","amount":%s,"units":%d,"ts":%d,'
                '"sent_ts":%d}' % (self.event_id, self.account, self.kind, amount, self.units,
                                   self.ts, self.sent_ts))

    def line(self):
        return "%s\t%s\n" % (KEYS[self.partition], self.value())


class Timeline:
    """Events in production order, each with its designed fate."""

    def __init__(self, seed):
        self.r = random.Random(seed)
        self.next_id = 1
        self.events = []
        self.running = {}  # partition -> its greatest event time so far

    def at(self, partition, rel_ms, fate="folded", account=None, kind=None):
        r = self.r
        kind = kind or r.choice(["purchase"] * 7 + ["refund"] * 2 + ["chargeback"])
        e = Event(event_id=self.next_id, partition=partition,
                  account=account or r.choice(ACCOUNTS), kind=kind,
                  cents=r.randrange(100, 50_000), units=r.randrange(1, 9),
                  ts=BASE_MS + rel_ms, sent_ts=BASE_MS + rel_ms + r.randrange(500, 4_000),
                  fate="filtered" if kind == "chargeback" else fate)
        self.next_id += 1
        self.events.append(e)
        prev = self.running.get(partition)
        self.running[partition] = e.ts if prev is None else max(prev, e.ts)
        return e

    def steady(self, partitions, start_rel, end_rel, per_second=10, out_of_order=0.15, kind=None):
        """Events on each partition from start_rel to end_rel (relative
        milliseconds), interleaved across partitions as a feed would be. Some
        arrive out of order, but never by more than the lag less one behind
        their own partition's greatest event time so far, so whatever order
        the job reads the partitions in, none of them is late. Window
        boundaries are hit exactly: a record at each window's start and one
        at its last millisecond."""
        step = 1000 // per_second
        out = []
        for t in range(start_rel, end_rel, step):
            for p in partitions:
                rel = t
                if t % SIZE_MS == 0 and p == partitions[0]:
                    rel = t  # a window's first millisecond
                elif (t + step) % SIZE_MS == 0 and p == partitions[-1]:
                    rel = t + step - 1  # its last
                elif self.r.random() < out_of_order:
                    floor = (self.running.get(p, BASE_MS + rel) - BASE_MS) - (LAG_MS - 1)
                    rel = max(floor, t - self.r.randrange(1, LAG_MS))
                out.append(self.at(p, rel, kind=kind))
        return out


def design_windows(events, final_by_ms, lateness_ms=0, size_ms=SIZE_MS):
    """The final windows the designed fates give: every window whose end plus
    the lateness is at or below `final_by_ms`, with its count, its net
    amount in cents and its units, from the events designed to be folded."""
    windows = {}
    for e in events:
        if e.fate != "folded":
            continue
        start = (e.ts // size_ms) * size_ms
        end = start + size_ms
        if final_by_ms is None or end + lateness_ms > final_by_ms:
            continue
        w = windows.setdefault((e.account, start, end), [0, 0, 0])
        w[0] += 1
        w[1] += -e.cents if e.kind == "refund" else e.cents
        w[2] += e.units
    return windows


def events_by_value(events):
    """event_id -> Event, to match records read back from Kafka."""
    return {e.event_id: e for e in events}


def event_id_of(value_json):
    return json.loads(value_json)["event_id"]
