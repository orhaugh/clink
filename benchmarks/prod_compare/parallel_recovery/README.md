# parallel-recovery bench

Two production realities at once: scaling out beyond par=1 and
surviving a crash. Verifies clink's hash partitioning, side outputs
for late data, and end-to-end snapshot/restore at meaningful
parallelism.

## Workload

  - **Source**: 10M events, 1000 keys, **with 2% out-of-order**
    (some events arrive after their window's watermark would have
    closed it - exercises late-data routing).
  - **Parallelism**: 4 (configurable via `PARALLELISM`).
  - **Pipeline**:
      ```
      Source -> assign_timestamps_bounded_out_of_order(2s)
             -> keyBy(event.key)
             -> tumbling_window(1s)
             -> aggregate(EventStats)
             -> sink (count + per-window emit log)
      ```
    Late records (arriving after watermark > window.end + 2s) are
    routed to a `late_data_side_output<Event>` and counted
    separately by a side sink.
  - **Induced crash**: at the 6 s mark the runner kills the TM
    process. The runner then restarts the TM, the JM restores from
    the most recent successful checkpoint, the source replays from
    the restored offset, the job runs to completion.

## What it verifies

  - **Hash partitioning**: every (key, window_start) bucket is owned
    by exactly one subtask across the 4-way fanout. No double
    aggregation, no lost records.
  - **Watermark alignment**: per-subtask watermarks merge correctly
    so windows fire at consistent boundaries.
  - **Late data routing**: out-of-order records that miss their
    window land on the side output, NOT the main aggregate.
  - **Checkpoint barrier propagation**: aligned barriers cross all
    subtasks; snapshot captures consistent state.
  - **Restore correctness**: post-crash, the total emitted record
    count (main + late side output) matches the pre-crash projection
    exactly. No double-fired windows, no skipped records.

## Why it matters

If you're considering clink for production, the par > 1 + recovery
story is the highest-stakes question. This bench is the smallest
artifact that pushes both surfaces simultaneously.

## Status

  - clink job: shipped.
  - Flink job: shipped.
  - Runner: shipped. Default PARALLELISM=4.
  - Induced-crash + restore (v2): not yet implemented.

## Parallelism

This bench runs at par=4 by default and scales correctly to higher
par values. The par>4 startup race that previously caused intermittent
hangs at par=5/7/8 was fixed in commit 60918e6 (Dag::union_streams
barrier-alignment bug); 0/35 hangs verified at par=5/8/16. Set
`PARALLELISM=N` to test other values; expect linear-ish scaling.
