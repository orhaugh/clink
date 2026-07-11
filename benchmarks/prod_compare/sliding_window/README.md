# sliding-window bench

Exercises event-time sliding windows: every record fans out into
`size / slide` overlapping windows, each maintains its own aggregate,
each fires when its end passes the watermark. Realistic for moving-
average dashboards, anomaly detectors, threshold alerters.

## Workload

  - **Source**: same SyntheticEventSource as `inproc_compare/`, but
    emits 10M Events over 60 windows of 1 s with a 250 ms slide -> 4
    overlapping panes per record.
  - **Key**: `event.key` (1000 distinct keys).
  - **Window**: sliding event-time, `size=1000ms`, `slide=250ms`.
  - **Aggregate**: same EventStats (sum, count, latest payload) used
    in inproc_compare.
  - **Sink**: counts emitted EventStats and prints the total at end.

Expected window-output count:
`keys * (windows * 1000 / slide) = 1000 * 60 * 1000 / 250 = 240_000`

## Why it matters

Sliding windows multiply the per-record state work by `size / slide`.
At 4x overlap the per-record cost is dominated by 4 separate
load+combine+store cycles on the keyed state map, exactly the path
clink's iterator-based state access optimises. A pure sliding-window
benchmark validates that the optimisation holds at a different
workload shape than the single-pane tumbling bench.

## What it verifies

  - Per-pane state isolation (each (key, window_start) bucket
    distinct).
  - Watermark fires the right windows in the right order.
  - Sliding window state cleanup (each fired window purges its
    bucket after `size + allowed_lateness` ms).
  - Throughput-per-record is reasonable even at `size/slide = 4`.

## Status

  - Spec: this file.
  - clink job: `clink-job/` (built and run by the runner).
  - Flink job: `flink-job/` (Maven project, same workload).
  - Runner: `run.sh` (drives both sides; latest logs under `results/`).
