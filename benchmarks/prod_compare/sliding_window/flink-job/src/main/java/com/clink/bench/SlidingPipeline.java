package com.clink.bench;

import java.io.Serializable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

/**
 * Sliding-window clink-vs-Flink bench (Flink side).
 *
 * Pipeline mirrors the clink side in clink-job/sliding_pipeline.cpp:
 * a bounded synthetic source emits rich Event POJOs, the pipeline
 * keyBy + sliding_window(size=1s, slide=250ms) + aggregate into
 * EventStats, counting sink prints the final pane count. Each
 * record fans out into size/slide = 4 windows so the per-record
 * state work is 4x the tumbling shape.
 *
 * Run config via env vars:
 *   BENCH_RECORDS       (default 10_000_000)
 *   BENCH_KEYS          (default 1000)
 *   BENCH_WINDOWS       (default 100)
 *   BENCH_PAYLOAD_BYTES (default 1500)
 *   BENCH_CHECKPOINT_DIR (default /tmp/flink-sliding-ckpts)
 */
public final class SlidingPipeline {

    public static final class Event implements Serializable {
        private static final long serialVersionUID = 1L;
        public long tsMs;
        public long key;
        public long value;
        public String payload = "";
        public long[] tags = new long[0];
        public Map<String, String> attributes = new HashMap<>();
    }

    public static final class EventStats implements Serializable {
        private static final long serialVersionUID = 1L;
        public long sumValue;
        public long count;
        public String latestPayload = "";
    }

    private static final class SyntheticSplit implements SourceSplit, Serializable {
        private static final long serialVersionUID = 1L;
        private final long total;
        private final long keys;
        private final long windows;
        private final int payloadBytes;
        private long cursor;

        SyntheticSplit(long total, long keys, long windows, int payloadBytes, long cursor) {
            this.total = total;
            this.keys = keys;
            this.windows = windows;
            this.payloadBytes = payloadBytes;
            this.cursor = cursor;
        }

        @Override
        public String splitId() {
            return "synthetic-0";
        }
    }

    private static final class SplitSerializer implements SimpleVersionedSerializer<SyntheticSplit> {
        @Override public int getVersion() { return 1; }
        @Override public byte[] serialize(SyntheticSplit obj) {
            // 5 longs: total, keys, windows, payloadBytes, cursor.
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(5 * 8);
            buf.putLong(obj.total);
            buf.putLong(obj.keys);
            buf.putLong(obj.windows);
            buf.putLong(obj.payloadBytes);
            buf.putLong(obj.cursor);
            return buf.array();
        }
        @Override public SyntheticSplit deserialize(int v, byte[] b) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b);
            return new SyntheticSplit(buf.getLong(), buf.getLong(), buf.getLong(),
                                      (int) buf.getLong(), buf.getLong());
        }
    }

    private static final class EnumSerializer implements SimpleVersionedSerializer<Long> {
        @Override public int getVersion() { return 1; }
        @Override public byte[] serialize(Long obj) {
            return java.nio.ByteBuffer.allocate(8).putLong(obj == null ? 0L : obj).array();
        }
        @Override public Long deserialize(int v, byte[] b) {
            return java.nio.ByteBuffer.wrap(b).getLong();
        }
    }

    private static final class SyntheticReader implements SourceReader<Event, SyntheticSplit> {
        private final SourceReaderContext context;
        private SyntheticSplit split;
        private boolean noMoreSplits = false;
        private final String payload;
        private final long[] tagsTemplate;
        private final Map<String, String> attrsTemplate;
        private double stepMs;

        SyntheticReader(SourceReaderContext context, int payloadBytes) {
            this.context = context;
            char[] buf = new char[payloadBytes];
            for (int i = 0; i < payloadBytes; ++i) buf[i] = 'x';
            this.payload = new String(buf);
            this.tagsTemplate = new long[50];
            for (int i = 0; i < 50; ++i) this.tagsTemplate[i] = i * 7L;
            this.attrsTemplate = new HashMap<>();
            this.attrsTemplate.put("region", "us-west-2");
            this.attrsTemplate.put("env", "prod");
            this.attrsTemplate.put("service", "events");
            this.attrsTemplate.put("version", "v1.42.0");
        }

        @Override public void start() {
            // Source v2 requires the reader to pull a split from the
            // enumerator; without this call the enumerator never
            // assigns and the reader spins forever.
            context.sendSplitRequest();
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Event> out) {
            if (split == null) {
                return noMoreSplits ? InputStatus.END_OF_INPUT
                                    : InputStatus.NOTHING_AVAILABLE;
            }
            if (split.cursor >= split.total) {
                return InputStatus.END_OF_INPUT;
            }
            final long end = Math.min(split.cursor + 256, split.total);
            for (long i = split.cursor; i < end; ++i) {
                Event e = new Event();
                e.tsMs = (long) ((double) i * stepMs);
                e.key = i % split.keys;
                e.value = (i % 7) + 1;
                e.payload = payload;
                e.tags = tagsTemplate;
                e.attributes = attrsTemplate;
                out.collect(e);
            }
            split.cursor = end;
            return InputStatus.MORE_AVAILABLE;
        }

        @Override public java.util.List<SyntheticSplit> snapshotState(long checkpointId) {
            return split == null ? java.util.Collections.emptyList()
                                 : java.util.Collections.singletonList(split);
        }

        @Override public java.util.concurrent.CompletableFuture<Void> isAvailable() {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }

        @Override public void addSplits(java.util.List<SyntheticSplit> splits) {
            if (!splits.isEmpty()) {
                this.split = splits.get(0);
                this.stepMs = (double) (split.windows * 1000L) / (double) split.total;
            }
        }

        @Override public void notifyNoMoreSplits() { this.noMoreSplits = true; }
        @Override public void close() {}
    }

    private static final class SyntheticEnumerator
            implements SplitEnumerator<SyntheticSplit, Long> {
        private final SplitEnumeratorContext<SyntheticSplit> ctx;
        private final long total;
        private final long keys;
        private final long windows;
        private final int payloadBytes;
        // Pending split queue. Initialised with one split; readers
        // that fail and addSplitsBack feed back into this so the
        // restarted reader can pick up the work. Previously we kept a
        // sticky "assigned" flag which left restarted readers idle
        // when the first reader crashed - that bug made the bench
        // emit zero windows.
        private final java.util.ArrayDeque<SyntheticSplit> pending = new java.util.ArrayDeque<>();

        SyntheticEnumerator(SplitEnumeratorContext<SyntheticSplit> ctx,
                            long total, long keys, long windows, int payloadBytes) {
            this.ctx = ctx;
            this.total = total;
            this.keys = keys;
            this.windows = windows;
            this.payloadBytes = payloadBytes;
            pending.add(new SyntheticSplit(total, keys, windows, payloadBytes, 0));
        }

        @Override public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            SyntheticSplit next = pending.poll();
            if (next == null) {
                ctx.signalNoMoreSplits(subtaskId);
                return;
            }
            ctx.assignSplit(next, subtaskId);
            if (pending.isEmpty()) {
                ctx.signalNoMoreSplits(subtaskId);
            }
        }

        @Override public void addSplitsBack(java.util.List<SyntheticSplit> splits, int subtaskId) {
            // A reader that failed mid-flight gives its split back so
            // the restarted reader can pick up the work. Fixing this
            // path is what unblocked the bench - without re-queueing,
            // a transient first-reader failure left the bench with no
            // splits to assign and Flink finished in JVM-startup time
            // emitting zero windows.
            pending.addAll(splits);
        }
        @Override public void addReader(int subtaskId) {}
        @Override public Long snapshotState(long checkpointId) { return 0L; }
        @Override public void close() {}
    }

    private static final class SyntheticSource
            implements Source<Event, SyntheticSplit, Long> {
        private static final long serialVersionUID = 1L;
        private final long total;
        private final long keys;
        private final long windows;
        private final int payloadBytes;

        SyntheticSource(long total, long keys, long windows, int payloadBytes) {
            this.total = total;
            this.keys = keys;
            this.windows = windows;
            this.payloadBytes = payloadBytes;
        }

        @Override public Boundedness getBoundedness() { return Boundedness.BOUNDED; }

        @Override
        public SourceReader<Event, SyntheticSplit> createReader(SourceReaderContext readerContext) {
            return new SyntheticReader(readerContext, payloadBytes);
        }

        @Override
        public SplitEnumerator<SyntheticSplit, Long> createEnumerator(
                SplitEnumeratorContext<SyntheticSplit> enumContext) {
            return new SyntheticEnumerator(enumContext, total, keys, windows, payloadBytes);
        }

        @Override
        public SplitEnumerator<SyntheticSplit, Long> restoreEnumerator(
                SplitEnumeratorContext<SyntheticSplit> enumContext, Long checkpoint) {
            return createEnumerator(enumContext);
        }

        @Override
        public SimpleVersionedSerializer<SyntheticSplit> getSplitSerializer() {
            return new SplitSerializer();
        }

        @Override
        public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer() {
            return new EnumSerializer();
        }
    }

    private static final class SumAggregate
            implements AggregateFunction<Event, EventStats, EventStats> {
        @Override public EventStats createAccumulator() {
            EventStats s = new EventStats();
            s.sumValue = 0;
            s.count = 0;
            s.latestPayload = "";
            return s;
        }
        @Override public EventStats add(Event e, EventStats acc) {
            acc.sumValue += e.value;
            acc.count += 1;
            acc.latestPayload = e.payload;
            return acc;
        }
        @Override public EventStats getResult(EventStats acc) { return acc; }
        @Override public EventStats merge(EventStats a, EventStats b) {
            EventStats out = new EventStats();
            out.sumValue = a.sumValue + b.sumValue;
            out.count = a.count + b.count;
            out.latestPayload = b.latestPayload;
            return out;
        }
    }

    private static final class CountingSink implements SinkFunction<EventStats> {
        private static final long serialVersionUID = 1L;
        private long count = 0;
        private long lastSum = 0;
        private boolean printedFinal = false;

        @Override
        public void invoke(EventStats value, Context context) {
            count++;
            lastSum += value.count;
        }

        @Override
        public void finish() {
            // Sanity check at the end of the stream: print the pane
            // count and the total record count summed across panes,
            // so the bench harness can confirm the pipeline actually
            // did the windowing+aggregation work. Prior to this hook
            // the bench was silently emitting zero windows for hours
            // and the wall time being compared was JVM startup, not
            // record processing.
            if (!printedFinal) {
                System.out.println("FLINK_SINK_FINISH panes=" + count + " sum=" + lastSum);
                printedFinal = true;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final long records = Long.parseLong(System.getenv().getOrDefault("BENCH_RECORDS", "10000000"));
        final long keys = Long.parseLong(System.getenv().getOrDefault("BENCH_KEYS", "1000"));
        final long windows = Long.parseLong(System.getenv().getOrDefault("BENCH_WINDOWS", "100"));
        final int payloadBytes =
                Integer.parseInt(System.getenv().getOrDefault("BENCH_PAYLOAD_BYTES", "1500"));
        final String ckptDir =
                System.getenv().getOrDefault("BENCH_CHECKPOINT_DIR", "/tmp/flink-sliding-ckpts");

        // Flink 2.x: configure state backend + checkpoint storage via
        // Configuration (the older setStateBackend / setCheckpointStorage
        // overloads were removed).
        Configuration cfg = new Configuration();
        cfg.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        cfg.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        cfg.setString("state.checkpoints.dir", "file://" + ckptDir);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(cfg);
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        WatermarkStrategy<Event> watermarks = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((e, ts) -> e.tsMs);

        env.fromSource(
                new SyntheticSource(records, keys, windows, payloadBytes),
                watermarks,
                "synthetic-source")
            .returns(Types.POJO(Event.class))
            .keyBy((KeySelector<Event, Long>) e -> e.key)
            .window(SlidingEventTimeWindows.of(Duration.ofMillis(1000), Duration.ofMillis(250)))
            .aggregate(new SumAggregate())
            .returns(Types.POJO(EventStats.class))
            .addSink(new CountingSink());

        env.execute("flink-sliding-window-bench");
    }
}
