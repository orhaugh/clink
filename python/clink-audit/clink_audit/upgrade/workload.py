"""The deterministic input the controlled source reads.

Event time is laid out in blocks of `block_ms`. A block's events fall in the
block, in an order shuffled within it, and the block ends with a watermark at
its end, so no event is ever behind the watermark already emitted: an event
exactly at a block's start is at the watermark, never below it. Some events
fall exactly on a window boundary. Amounts are signed. Two groups of accounts
go quiet, one for good part way through the prefix and one for longer than the
idle gap across the savepoint, so timers are pending when it is taken and fire
after the restore. The last segment ends with a watermark far past every event
and END.

Segments are whole files, `segment-000001.txt` on; the savepoint is taken once
the source has read `savepoint_after_segment` of them and the operators have
seen its last watermark.
"""

import os
import random

FINAL_GAP_MS = 10 ** 9


def segments(inp):
    """Returns the segments as lists of lines."""
    r = random.Random(inp["seed"])
    block_ms = inp["block_ms"]
    blocks = inp["blocks"]
    n = inp["accounts"]
    start = inp["start_ms"]
    quiet_a = range(1, max(1, n // 3) + 1)               # quiet for a while, then back
    stop_b = range(len(quiet_a) + 1, len(quiet_a) + max(1, n // 6) + 1)  # stop for good
    cut_block = inp["savepoint_after_segment"] * inp["blocks_per_segment"]
    quiet_from, quiet_to = max(0, cut_block - 2), cut_block + max(2, blocks // 3)
    stop_from = max(1, cut_block * 3 // 4)
    out = []
    for b in range(blocks):
        bs = start + b * block_ms
        ev = []
        for a in range(1, n + 1):
            if a in quiet_a and quiet_from <= b < quiet_to:
                continue
            if a in stop_b and b >= stop_from:
                continue
            if r.random() >= 0.3:
                continue
            ts = bs + r.randrange(block_ms)
            if r.random() < 0.15:
                ts = bs  # at the watermark, and on a window boundary when bs is one
            ev.append("E,%d,%d,%d" % (ts, a, r.randrange(-500, 1000)))
        r.shuffle(ev)
        out.append(ev + ["W,%d" % (bs + block_ms)])
    per = inp["blocks_per_segment"]
    segs = [[line for blk in out[i:i + per] for line in blk] for i in range(0, blocks, per)]
    segs[-1] += ["W,%d" % (start + blocks * block_ms + FINAL_GAP_MS), "END"]
    return segs


def segment_name(i):
    return "segment-%06d.txt" % i


def write(inp, directory):
    """Writes every segment to `directory`; returns their names in order."""
    os.makedirs(directory, exist_ok=True)
    names = []
    for i, lines in enumerate(segments(inp), 1):
        name = segment_name(i)
        with open(os.path.join(directory, name), "w") as f:
            f.write("\n".join(lines) + "\n")
        names.append(name)
    return names


def publish(src, dst, names):
    """Makes segments visible to a running source: each is copied under a
    temporary name and renamed, so the source never reads one half written."""
    os.makedirs(dst, exist_ok=True)
    for name in names:
        tmp = os.path.join(dst, "." + name + ".tmp")
        with open(os.path.join(src, name), "rb") as f, open(tmp, "wb") as g:
            g.write(f.read())
        os.replace(tmp, os.path.join(dst, name))
