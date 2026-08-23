"""The deterministic event specification shared by the QUAL-01 generator
and verifier. Every field of every input event is a pure function of
(seed, partition, seq), so the full expected output of the pipeline -
per-key, per-window counts and sums - is recomputable by anyone holding
the seed and the per-partition high-water sequences. The oracle is this
function, not any stored copy of history, and certainly not clink.

Event-time model: within a partition, event time advances monotonically
with seq at a fixed rate, minus a bounded deterministic jitter. The
pipeline's watermark lag must strictly exceed MAX_JITTER_MS, so no
in-window record is falsely late; the generator and campaign driver both
assert that relationship rather than assuming it.
"""

MASK = (1 << 64) - 1


def splitmix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & MASK
    z = x
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
    return (z ^ (z >> 31)) & MASK


class Spec:
    def __init__(self, seed: int, partitions: int, keys: int,
                 events_per_sec_per_partition: int, base_ms: int,
                 max_jitter_ms: int, window_ms: int,
                 key_epoch_ms: int = 0):
        assert events_per_sec_per_partition > 0
        assert window_ms > 0 and max_jitter_ms >= 0
        assert key_epoch_ms >= 0
        self.seed = seed
        self.partitions = partitions
        self.keys = keys
        self.eps = events_per_sec_per_partition
        self.base_ms = base_ms
        self.max_jitter_ms = max_jitter_ms
        self.window_ms = window_ms
        # 0 (the default) keeps the fixed key space every campaign before
        # QUAL-05 used: `keys` distinct keys, each touched for the whole run.
        #
        # Non-zero makes the key space TURN OVER. Event time is cut into
        # epochs of this length and each epoch draws from its own disjoint
        # block of `keys` keys, so a key is touched only during its epoch
        # and never again. That is what gives a retention campaign a
        # workload whose state can plateau: with a state_ttl of T the live
        # population settles at about keys * (1 + T / key_epoch_ms), which
        # is a level the campaign can predict and then measure against.
        self.key_epoch_ms = key_epoch_ms

    def event(self, partition: int, seq: int):
        """(key, amount, event_time_ms) for one event. Pure."""
        h = splitmix64(self.seed ^ (partition << 40) ^ seq)
        amount = (h >> 24) % 1000
        jitter = ((h >> 44) % (self.max_jitter_ms + 1)) if self.max_jitter_ms else 0
        # The epoch is taken from the UNJITTERED time, so a key's epoch is a
        # pure function of seq and no event can be pulled across an epoch
        # boundary by its jitter. A key's whole lifetime is therefore at
        # most key_epoch_ms + max_jitter_ms of event time, which is what a
        # campaign's TTL has to exceed for a key's aggregate never to be
        # truncated mid-life.
        base_ts = self.base_ms + (seq * 1000) // self.eps
        if self.key_epoch_ms:
            epoch = (base_ts - self.base_ms) // self.key_epoch_ms
            key = epoch * self.keys + (h % self.keys)
        else:
            key = h % self.keys
        return key, amount, base_ts - jitter

    def event_json(self, partition: int, seq: int) -> str:
        key, amount, ts = self.event(partition, seq)
        return ('{"event_id":"p%d-%d","k":%d,"amount":%d,"ts":%d}'
                % (partition, seq, key, amount, ts))

    def window_start(self, ts: int) -> int:
        return ts - (ts % self.window_ms)

    def seq_range_for_window(self, w_start: int):
        """Candidate seq range [lo, hi) per partition whose events COULD
        land in window [w_start, w_start + window_ms), given the bounded
        jitter. Callers filter by the exact per-event timestamp."""
        w_end = w_start + self.window_ms
        lo_ms = w_start - self.base_ms
        hi_ms = w_end - self.base_ms + self.max_jitter_ms
        lo = max(0, (lo_ms * self.eps) // 1000)
        hi = max(0, (hi_ms * self.eps) // 1000 + 1)
        return lo, hi

    def expected_for_window(self, w_start: int, produced_high: dict):
        """Expected {key: (count, sum)} for one closed window, given the
        per-partition high-water seq actually produced (exclusive).
        Recomputed from the pure function - no stored history."""
        out = {}
        w_end = w_start + self.window_ms
        for p in range(self.partitions):
            high = produced_high.get(p, 0)
            lo, hi = self.seq_range_for_window(w_start)
            for seq in range(lo, min(hi, high)):
                key, amount, ts = self.event(p, seq)
                if w_start <= ts < w_end:
                    c, s = out.get(key, (0, 0))
                    out[key] = (c + 1, s + amount)
        return out

    def window_fully_produced(self, w_start: int, produced_high: dict) -> bool:
        """True when every partition's production has passed the last seq
        that could contribute to this window - only then is the expected
        set final."""
        _, hi = self.seq_range_for_window(w_start)
        return all(produced_high.get(p, 0) >= hi for p in range(self.partitions))
