"""The tutorial's workload: eight temperature sensors, five minutes of readings.

Every reading is a pure function of the constants below, so produce_events.py
can generate the stream and verify.py can recompute, from the same definition
and without trusting anything the pipeline wrote, exactly what ClickHouse must
end up holding. Nothing here is random.
"""

import datetime
from collections import deque

TOPIC = "readings"
SENSORS = [f"sensor-{i:02d}" for i in range(1, 9)]
START_MS = 1_772_352_000_000  # 2026-03-01T08:00:00Z
TICK_MS = 1_000  # every sensor reports once per second of event time
DURATION_TICKS = 300  # five minutes of event time: 30 tumbling windows of 10 s
WINDOW_MS = 10_000  # must match TUMBLE(ts, INTERVAL '10' SECOND) in pipeline.sql
WATERMARK_LAG_MS = 3_000  # must match watermark_lag_ms in pipeline.sql
LATE_SENSOR = "sensor-03"  # its readings reach Kafka two seconds after everyone else's
LATE_TICKS = 2
# Readings past the last window's end, so the watermark (largest ts seen minus
# the lag) moves beyond it and the window fires. They fall into a 31st window
# that never closes: it stays open in clink's state, which the tutorial goes
# and looks at.
TAIL_TICKS = 5
TOTAL_TICKS = DURATION_TICKS + TAIL_TICKS
TOTAL_READINGS = TOTAL_TICKS * len(SENSORS)
COMPLETE_WINDOWS = DURATION_TICKS * TICK_MS // WINDOW_MS  # 30
OPEN_WINDOW_START_MS = START_MS + COMPLETE_WINDOWS * WINDOW_MS


def temperature_tenths(sensor_index: int, tick: int) -> int:
    """Temperature in tenths of a degree: integer arithmetic only, so the
    generator and the verifier agree to the last digit on every platform."""
    base = 180 + 5 * sensor_index  # 18.0 C for sensor-01, 18.5 C for sensor-02, ...
    drift = 3 * ((tick // 30) % 8)  # a slow step every 30 s, cycling over four minutes
    wobble = (tick * 7 + sensor_index * 13) % 5 - 2  # -0.2 C .. +0.2 C
    return base + drift + wobble


def reading(sensor_index: int, tick: int) -> dict:
    return {
        "sensor_id": SENSORS[sensor_index],
        "ts": START_MS + tick * TICK_MS,
        "temp_c": temperature_tenths(sensor_index, tick) / 10,
    }


def arrivals():
    """Every reading, in the order it reaches Kafka.

    Seven sensors report on time. sensor-03's readings are held back and
    released after the on-time readings two ticks later, so they arrive out of
    event-time order (by two seconds, inside the 3 s watermark lag). The
    aggregates must not notice."""
    held = deque()
    for tick in range(TOTAL_TICKS):
        for i, sensor in enumerate(SENSORS):
            r = reading(i, tick)
            if sensor == LATE_SENSOR:
                held.append((tick + LATE_TICKS, r))
            else:
                yield r
        while held and held[0][0] <= tick:
            yield held.popleft()[1]
    while held:
        yield held.popleft()[1]


def expected_windows() -> dict:
    """{(sensor_id, window_start_ms): (readings, avg_temp_c, min_temp_c, max_temp_c)}
    for every window the stream's watermark closes."""
    last_ts = START_MS + (TOTAL_TICKS - 1) * TICK_MS
    watermark = last_ts - WATERMARK_LAG_MS
    out = {}
    ticks_per_window = WINDOW_MS // TICK_MS
    for w in range(COMPLETE_WINDOWS):
        window_start = START_MS + w * WINDOW_MS
        if window_start + WINDOW_MS > watermark:
            break
        first_tick = w * ticks_per_window
        for i, sensor in enumerate(SENSORS):
            tenths = [temperature_tenths(i, t) for t in range(first_tick, first_tick + ticks_per_window)]
            out[(sensor, window_start)] = (
                len(tenths),
                sum(tenths) / (10 * len(tenths)),
                min(tenths) / 10,
                max(tenths) / 10,
            )
    return out


def fmt_ts(ms: int) -> str:
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%H:%M:%S")
