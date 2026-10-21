"""The example's synthetic workload: transfers between made-up accounts.

Every transfer is a pure function of the constants below, so produce_events.py
can generate the stream and verify.py can recompute, from the same definition
and without trusting anything the pipeline wrote, exactly what the graph must
hold: the accounts, the transfers with their senders and recipients, and the
fan-in signals clink should have raised. Nothing here is random, and no
account or amount belongs to anyone.

Three accounts are given a planted pattern: inside one minute they receive
from many distinct senders, and shortly after they send money on. The signal
the pipeline computes is only the first half (many distinct senders into one
account within a one-minute window); queries.cypher follows the money from
there. A signal is an indicator for a person to look into, not a finding.
"""

import datetime

TOPIC = "transfers"
ACCOUNTS = [f"acct-{i:02d}" for i in range(1, 41)]
START_MS = 1_772_355_600_000  # 2026-03-01T09:00:00Z
WINDOW_MS = 60_000  # must match TUMBLE(at, INTERVAL '1' MINUTE) in pipeline.sql
WATERMARK_LAG_MS = 5_000  # must match watermark_lag_ms in pipeline.sql
THRESHOLD = 5  # must match HAVING COUNT(DISTINCT sender) >= 5 in pipeline.sql
RULE_VERSION = "fan-in/v1"  # must match the rule_version literal in pipeline.sql
MINUTES = 10
BACKGROUND_EVERY_MS = 2_000  # one ordinary transfer every two seconds of event time

# account, minute it is planted in, how many distinct senders pay in.
PLANTED = [("acct-07", 2, 7), ("acct-19", 5, 6), ("acct-33", 8, 8)]


def _mix(n: int) -> int:
    """A fixed integer hash, so every platform derives the same stream."""
    n = (n * 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    n ^= n >> 31
    n = (n * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    return n ^ (n >> 29)


def transfers():
    """Every transfer, in the order it reaches Kafka (event-time order)."""
    out = []
    planted_accounts = {a for a, _, _ in PLANTED}
    # Ordinary traffic between accounts that are never planted, so no
    # background transfer adds a sender to a planted account's window.
    ordinary = [a for a in ACCOUNTS if a not in planted_accounts]
    for i, at in enumerate(range(START_MS, START_MS + MINUTES * WINDOW_MS, BACKGROUND_EVERY_MS)):
        h = _mix(i + 1)
        sender = ordinary[h % len(ordinary)]
        recipient = ordinary[(h >> 8) % len(ordinary)]
        if recipient == sender:
            recipient = ordinary[(ordinary.index(sender) + 1) % len(ordinary)]
        out.append({"sender": sender, "recipient": recipient,
                    "amount": 20 + (h >> 16) % 480, "at": at})
    for account, minute, senders in PLANTED:
        window = START_MS + minute * WINDOW_MS
        payers = [a for a in ACCOUNTS if a not in planted_accounts][:senders]
        for k, payer in enumerate(payers):
            out.append({"sender": payer, "recipient": account,
                        "amount": 900 + 10 * k, "at": window + 4_000 + 6_000 * k})
        # Then the money moves on quickly, after the window closes.
        for k in range(3):
            out.append({"sender": account, "recipient": ACCOUNTS[(ACCOUNTS.index(account) + 11 + k) % 40],
                        "amount": 2_000 + 100 * k, "at": window + WINDOW_MS + 5_000 + 10_000 * k})
    # Past the last window, so the watermark passes it and every window fires.
    tail = START_MS + MINUTES * WINDOW_MS + WINDOW_MS + WATERMARK_LAG_MS
    out.append({"sender": "acct-01", "recipient": "acct-02", "amount": 1, "at": tail})
    out.sort(key=lambda t: t["at"])
    for n, t in enumerate(out, start=1):
        t["event_id"] = n
    return out


def expected_signals(all_transfers):
    """Recompute the fan-in signals from the transfers, as the pipeline's
    one-minute tumbling windows group them: every (account, window) whose
    inbound transfers come from at least THRESHOLD distinct senders, among the
    windows the watermark has closed."""
    last_at = max(t["at"] for t in all_transfers)
    watermark = last_at - WATERMARK_LAG_MS
    windows = {}
    for t in all_transfers:
        start = t["at"] - (t["at"] - START_MS) % WINDOW_MS
        if start + WINDOW_MS > watermark:
            continue  # a window the watermark has not closed never fires
        windows.setdefault((t["recipient"], start), []).append(t)
    out = {}
    for (account, start), legs in windows.items():
        senders = {t["sender"] for t in legs}
        if len(senders) >= THRESHOLD:
            out[f"{account}:{start}:{RULE_VERSION}"] = {
                "account": account,
                "window_start": start,
                "window_end": start + WINDOW_MS,
                "distinct_senders": len(senders),
                "transfers": len(legs),
                "event_ids": sorted(t["event_id"] for t in legs),
            }
    return out


def fmt_ts(ms: int) -> str:
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%H:%M:%S")
