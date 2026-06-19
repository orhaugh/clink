package com.clink.bench;

import java.io.Serializable;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.util.Collector;

/**
 * Two-stream interval-join bench (v1): Flink side.
 *
 * Mirrors clink-job/join_pipeline.cpp. Orders + Payments key by
 * user_id, connect, and a KeyedCoProcessFunction keeps the latest
 * Order per key in ValueState; on Payment, the joined record is
 * emitted. No time-window enforcement in v1; the workload simply
 * guarantees each Payment sees a previously-arrived Order for the
 * same key.
 */
public final class JoinPipeline {

    public static final class Order implements Serializable {
        private static final long serialVersionUID = 1L;
        public long tsMs;
        public long userId;
        public long orderId;
        public long amountCents;
    }

    public static final class Payment implements Serializable {
        private static final long serialVersionUID = 1L;
        public long tsMs;
        public long userId;
        public long paymentId;
        public long paidCents;
    }

    public static final class Joined implements Serializable {
        private static final long serialVersionUID = 1L;
        public long userId;
        public long orderId;
        public long paymentId;
        public long amountCents;
        public long paidCents;
    }

    private static final class SyntheticSplit implements SourceSplit, Serializable {
        private static final long serialVersionUID = 1L;
        final long total;
        final long keys;
        final long windows;
        final long tsOffsetMs;
        long cursor;

        SyntheticSplit(long total, long keys, long windows, long tsOffsetMs, long cursor) {
            this.total = total;
            this.keys = keys;
            this.windows = windows;
            this.tsOffsetMs = tsOffsetMs;
            this.cursor = cursor;
        }

        @Override public String splitId() { return "synthetic-0"; }
    }

    private static final class SplitSerializer implements SimpleVersionedSerializer<SyntheticSplit> {
        @Override public int getVersion() { return 1; }
        @Override public byte[] serialize(SyntheticSplit obj) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(5 * 8);
            buf.putLong(obj.total);
            buf.putLong(obj.keys);
            buf.putLong(obj.windows);
            buf.putLong(obj.tsOffsetMs);
            buf.putLong(obj.cursor);
            return buf.array();
        }
        @Override public SyntheticSplit deserialize(int v, byte[] b) {
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(b);
            return new SyntheticSplit(buf.getLong(), buf.getLong(), buf.getLong(),
                                      buf.getLong(), buf.getLong());
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

    /** Source reader factored to emit either Orders or Payments depending on `isOrder`. */
    private static final class SyntheticReader<T> implements SourceReader<T, SyntheticSplit> {
        private final SourceReaderContext context;
        private final boolean isOrder;
        private SyntheticSplit split;
        private boolean noMoreSplits = false;
        private double stepMs;

        SyntheticReader(SourceReaderContext context, boolean isOrder) {
            this.context = context;
            this.isOrder = isOrder;
        }

        @Override public void start() { context.sendSplitRequest(); }

        @SuppressWarnings("unchecked")
        @Override
        public InputStatus pollNext(ReaderOutput<T> out) {
            if (split == null) {
                return noMoreSplits ? InputStatus.END_OF_INPUT
                                    : InputStatus.NOTHING_AVAILABLE;
            }
            if (split.cursor >= split.total) return InputStatus.END_OF_INPUT;
            final long end = Math.min(split.cursor + 256, split.total);
            for (long i = split.cursor; i < end; ++i) {
                long ts = (long) ((double) i * stepMs) + split.tsOffsetMs;
                if (isOrder) {
                    Order o = new Order();
                    o.tsMs = ts;
                    o.userId = i % split.keys;
                    o.orderId = i;
                    o.amountCents = ((i % 1000) + 1) * 100;
                    out.collect((T) o);
                } else {
                    Payment p = new Payment();
                    p.tsMs = ts;
                    p.userId = i % split.keys;
                    p.paymentId = i;
                    p.paidCents = ((i % 1000) + 1) * 100;
                    out.collect((T) p);
                }
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
        private final java.util.ArrayDeque<SyntheticSplit> pending = new java.util.ArrayDeque<>();

        SyntheticEnumerator(SplitEnumeratorContext<SyntheticSplit> ctx,
                            long total, long keys, long windows, long tsOffsetMs) {
            this.ctx = ctx;
            pending.add(new SyntheticSplit(total, keys, windows, tsOffsetMs, 0));
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
            pending.addAll(splits);
        }
        @Override public void addReader(int subtaskId) {}
        @Override public Long snapshotState(long checkpointId) { return 0L; }
        @Override public void close() {}
    }

    private static final class OrderSource implements Source<Order, SyntheticSplit, Long> {
        private static final long serialVersionUID = 1L;
        private final long total, keys, windows;
        OrderSource(long total, long keys, long windows) {
            this.total = total; this.keys = keys; this.windows = windows;
        }
        @Override public Boundedness getBoundedness() { return Boundedness.BOUNDED; }
        @Override public SourceReader<Order, SyntheticSplit> createReader(SourceReaderContext c) {
            return new SyntheticReader<>(c, true);
        }
        @Override public SplitEnumerator<SyntheticSplit, Long> createEnumerator(
                SplitEnumeratorContext<SyntheticSplit> enumCtx) {
            return new SyntheticEnumerator(enumCtx, total, keys, windows, 0L);
        }
        @Override public SplitEnumerator<SyntheticSplit, Long> restoreEnumerator(
                SplitEnumeratorContext<SyntheticSplit> enumCtx, Long cp) {
            return createEnumerator(enumCtx);
        }
        @Override public SimpleVersionedSerializer<SyntheticSplit> getSplitSerializer() {
            return new SplitSerializer();
        }
        @Override public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer() {
            return new EnumSerializer();
        }
    }

    private static final class PaymentSource implements Source<Payment, SyntheticSplit, Long> {
        private static final long serialVersionUID = 1L;
        private final long total, keys, windows;
        PaymentSource(long total, long keys, long windows) {
            this.total = total; this.keys = keys; this.windows = windows;
        }
        @Override public Boundedness getBoundedness() { return Boundedness.BOUNDED; }
        @Override public SourceReader<Payment, SyntheticSplit> createReader(SourceReaderContext c) {
            return new SyntheticReader<>(c, false);
        }
        @Override public SplitEnumerator<SyntheticSplit, Long> createEnumerator(
                SplitEnumeratorContext<SyntheticSplit> enumCtx) {
            // Offset payments by +50ms so each Payment.ts > matching Order.ts.
            return new SyntheticEnumerator(enumCtx, total, keys, windows, 50L);
        }
        @Override public SplitEnumerator<SyntheticSplit, Long> restoreEnumerator(
                SplitEnumeratorContext<SyntheticSplit> enumCtx, Long cp) {
            return createEnumerator(enumCtx);
        }
        @Override public SimpleVersionedSerializer<SyntheticSplit> getSplitSerializer() {
            return new SplitSerializer();
        }
        @Override public SimpleVersionedSerializer<Long> getEnumeratorCheckpointSerializer() {
            return new EnumSerializer();
        }
    }

    private static final class JoinFn
            extends KeyedCoProcessFunction<Long, Order, Payment, Joined> {
        private static final long serialVersionUID = 1L;
        private transient ValueState<Order> latestOrder;

        @Override
        public void open(org.apache.flink.api.common.functions.OpenContext oc) {
            latestOrder = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latest_order", Order.class));
        }

        @Override
        public void processElement1(Order o, Context ctx, Collector<Joined> out) throws Exception {
            latestOrder.update(o);
        }

        @Override
        public void processElement2(Payment p, Context ctx, Collector<Joined> out) throws Exception {
            Order o = latestOrder.value();
            if (o == null) return;
            Joined j = new Joined();
            j.userId = p.userId;
            j.orderId = o.orderId;
            j.paymentId = p.paymentId;
            j.amountCents = o.amountCents;
            j.paidCents = p.paidCents;
            out.collect(j);
        }
    }

    private static final class CountingSink implements SinkFunction<Joined> {
        private static final long serialVersionUID = 1L;
        private long count = 0;
        private boolean printedFinal = false;
        @Override public void invoke(Joined value, Context context) { count++; }
        @Override public void finish() {
            if (!printedFinal) {
                System.out.println("FLINK_SINK_FINISH panes=" + count);
                printedFinal = true;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final long orders = Long.parseLong(System.getenv().getOrDefault("BENCH_ORDERS", "5000000"));
        final long payments = Long.parseLong(System.getenv().getOrDefault("BENCH_PAYMENTS", "5000000"));
        final long keys = Long.parseLong(System.getenv().getOrDefault("BENCH_KEYS", "1000"));
        final long windows = Long.parseLong(System.getenv().getOrDefault("BENCH_WINDOWS", "100"));
        final String ckptDir =
                System.getenv().getOrDefault("BENCH_CHECKPOINT_DIR", "/tmp/flink-join-ckpts");

        Configuration cfg = new Configuration();
        cfg.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        cfg.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        cfg.setString("state.checkpoints.dir", "file://" + ckptDir);

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(cfg);
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        WatermarkStrategy<Order> orderWm = WatermarkStrategy
                .<Order>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((e, ts) -> e.tsMs);
        WatermarkStrategy<Payment> paymentWm = WatermarkStrategy
                .<Payment>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner((e, ts) -> e.tsMs);

        var orderStream = env.fromSource(new OrderSource(orders, keys, windows),
                                         orderWm, "orders-source")
                             .returns(Types.POJO(Order.class))
                             .keyBy((KeySelector<Order, Long>) o -> o.userId);
        var paymentStream = env.fromSource(new PaymentSource(payments, keys, windows),
                                           paymentWm, "payments-source")
                               .returns(Types.POJO(Payment.class))
                               .keyBy((KeySelector<Payment, Long>) p -> p.userId);

        orderStream.connect(paymentStream)
                   .process(new JoinFn())
                   .returns(Types.POJO(Joined.class))
                   .addSink(new CountingSink());

        env.execute("flink-interval-join-bench");
    }
}
