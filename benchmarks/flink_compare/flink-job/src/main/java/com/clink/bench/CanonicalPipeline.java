package com.clink.bench;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class CanonicalPipeline {

    public static final class Record {
        public long tsMs;
        public long key;
        public long value;
    }

    public static final class Out {
        public long windowEndMs;
        public long key;
        public long sumValue;
    }

    private static final class RecordDeserializer implements DeserializationSchema<Record> {
        @Override
        public Record deserialize(byte[] bytes) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            Record r = new Record();
            r.tsMs = buf.getLong();
            r.key = buf.getLong();
            r.value = buf.getLong();
            return r;
        }

        @Override
        public boolean isEndOfStream(Record record) { return false; }

        @Override
        public TypeInformation<Record> getProducedType() {
            return TypeInformation.of(Record.class);
        }
    }

    private static final class OutSerializer implements SerializationSchema<Out> {
        @Override
        public byte[] serialize(Out o) {
            ByteBuffer buf = ByteBuffer.allocate(24).order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(o.windowEndMs);
            buf.putLong(o.key);
            buf.putLong(o.sumValue);
            return buf.array();
        }
    }

    private static final class SumAggregate implements AggregateFunction<Record, Long, Long> {
        @Override public Long createAccumulator() { return 0L; }
        @Override public Long add(Record r, Long acc) { return acc + r.value; }
        @Override public Long getResult(Long acc) { return acc; }
        @Override public Long merge(Long a, Long b) { return a + b; }
    }

    private static final class EmitWindow extends ProcessWindowFunction<Long, Out, Long, TimeWindow> {
        @Override
        public void process(Long key, Context ctx, Iterable<Long> sums, Collector<Out> collector) {
            Out o = new Out();
            o.windowEndMs = ctx.window().getEnd();
            o.key = key;
            o.sumValue = sums.iterator().next();
            collector.collect(o);
        }
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "kafka:29092");
        String inTopic = System.getenv().getOrDefault("INPUT_TOPIC", "bench-in");
        String outTopic = System.getenv().getOrDefault("OUTPUT_TOPIC", "bench-out");
        int parallelism = Integer.parseInt(System.getenv().getOrDefault("PARALLELISM", "4"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.disableOperatorChaining();

        KafkaSource<Record> source = KafkaSource.<Record>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(inTopic)
            .setGroupId("flink-canonical")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new RecordDeserializer())
            .build();

        KafkaSink<Out> sink = KafkaSink.<Out>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.<Out>builder()
                .setTopic(outTopic)
                .setValueSerializationSchema(new OutSerializer())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        WatermarkStrategy<Record> watermarks = WatermarkStrategy
            .<Record>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((r, ts) -> r.tsMs);

        env
            .fromSource(source, watermarks, "kafka-source")
            .returns(Types.GENERIC(Record.class))
            .keyBy((KeySelector<Record, Long>) r -> r.key)
            .window(TumblingEventTimeWindows.of(Duration.ofMillis(1000)))
            .aggregate(new SumAggregate(), new EmitWindow())
            .returns(Types.GENERIC(Out.class))
            .sinkTo(sink);

        env.execute("flink-canonical-pipeline");
    }
}
