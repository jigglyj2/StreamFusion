/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Byte-level temporal-join state/output coverage for every accepted Flink logical type. */
class StreamFusionTemporalJoinTypeParityTest {
    private static final int MAX_PARALLELISM = 128;

    @ParameterizedTest(name = "temporal key/state type: {0}")
    @MethodSource("tech.streamfusion.flink.deduplicate.StreamFusionDeduplicateKeyTypeParityTest#keyCases")
    void roundTripsEverySupportedTypeThroughBothBackends(String description, LogicalType keyType, Object key)
            throws Exception {
        RowType inputType = RowType.of(
                new LogicalType[] {keyType, new TimestampType(false, 3), new BigIntType(false)},
                new String[] {"join_key", "event_time", "payload"});
        RowType outputType = RowType.of(new LogicalType[] {
            keyType,
            new TimestampType(false, 3),
            new BigIntType(false),
            keyType,
            new TimestampType(false, 3),
            new BigIntType(false)
        });
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(inputType));
        byte[] exchangePlan = NativeExchangePlanSerializer.hash(inputType, new int[] {0}, MAX_PARALLELISM);
        LogicalType physicalKeyType = keyType instanceof DistinctType
                ? ((DistinctType) keyType).getSourceType().copy(keyType.isNullable())
                : keyType;
        RowType physicalOutputType = RowType.of(new LogicalType[] {
            physicalKeyType,
            new TimestampType(false, 3),
            new BigIntType(false),
            physicalKeyType,
            new TimestampType(false, 3),
            new BigIntType(false)
        });
        RowDataSerializer serializer = new RowDataSerializer(physicalOutputType);
        GenericRowData expected = GenericRowData.of(
                key, TimestampData.fromEpochMillis(1_500), 1L, key, TimestampData.fromEpochMillis(1_000), 2L);

        for (boolean rocks : new boolean[] {false, true}) {
            List<String> captured = new ArrayList<>();
            StreamFusionArrowTemporalJoinOperator operator = operator(inputType, outputType, selector, exchangePlan);
            NativeExchangeFrameKeySelector frameSelector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
            try (RootAllocator allocator = new RootAllocator(64L << 20);
                    KeyedTwoInputStreamOperatorTestHarness<
                                    Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                            harness = new KeyedTwoInputStreamOperatorTestHarness<>(
                                    operator, frameSelector, frameSelector, Types.INT, MAX_PARALLELISM, 1, 0)) {
                harness.setStateBackend(new StreamFusionStateBackend(
                        rocks ? new EmbeddedRocksDBStateBackend(true) : new HashMapStateBackend()));
                harness.setOutputCreator(ignored -> new SerializingOutput(serializer, captured));
                harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
                harness.open();
                process(
                        harness,
                        allocator,
                        1,
                        GenericRowData.of(key, TimestampData.fromEpochMillis(1_000), 2L),
                        inputType,
                        exchangePlan,
                        selector,
                        keyType);
                process(
                        harness,
                        allocator,
                        0,
                        GenericRowData.of(key, TimestampData.fromEpochMillis(1_500), 1L),
                        inputType,
                        exchangePlan,
                        selector,
                        keyType);
                harness.processWatermark1(new Watermark(1_500));
                harness.processWatermark2(new Watermark(1_500));
                assertThat(captured)
                        .as(description + (rocks ? " RocksDB" : " memory"))
                        .containsExactly(serialize(serializer, expected));
            }
        }
    }

    private static StreamFusionArrowTemporalJoinOperator operator(
            RowType inputType, RowType outputType, RowDataKeySelector selector, byte[] exchangePlan) {
        return new StreamFusionArrowTemporalJoinOperator(
                inputType,
                inputType,
                outputType,
                new int[] {0},
                new int[] {0},
                StreamFusionTemporalJoinPlan.create(
                        inputType,
                        inputType,
                        new int[] {0},
                        new int[] {0},
                        new boolean[] {true},
                        FlinkJoinType.INNER,
                        false,
                        1,
                        1,
                        0,
                        0),
                selector,
                selector,
                exchangePlan,
                exchangePlan,
                false,
                FlinkJoinType.INNER,
                null);
    }

    private static void process(
            KeyedTwoInputStreamOperatorTestHarness<Integer, NativeExchangeFrame, NativeExchangeFrame, ArrowRowDataBatch>
                    harness,
            RootAllocator allocator,
            int side,
            GenericRowData row,
            RowType inputType,
            byte[] exchangePlan,
            RowDataKeySelector selector,
            LogicalType keyType)
            throws Exception {
        List<byte[]> routingKeys = null;
        if (requiresPreencodedExchangeKey(keyType)) {
            BinaryRowData binary = (BinaryRowData) selector.getKey(row);
            routingKeys = List.of(
                    BinarySegmentUtils.copyToBytes(binary.getSegments(), binary.getOffset(), binary.getSizeInBytes()));
        }
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(row), inputType, allocator)
                        .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0});
                ArrowExchangeBatch.EnvelopeBatch envelope =
                        ArrowExchangeBatch.withEnvelope(input, inputType, routingKeys)) {
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(exchangePlan, envelope.batch(), allocator);
            assertThat(frames).hasSize(1);
            StreamRecord<NativeExchangeFrame> record = new StreamRecord<>(frames.get(0));
            if (side == 0) {
                harness.processElement1(record);
            } else {
                harness.processElement2(record);
            }
        }
    }

    private static boolean requiresPreencodedExchangeKey(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return false;
            default:
                return true;
        }
    }

    private static String serialize(RowDataSerializer serializer, RowData row) {
        try {
            DataOutputSerializer output = new DataOutputSerializer(256);
            serializer.serialize(row, output);
            return Base64.getEncoder().encodeToString(output.getCopyOfBuffer());
        } catch (IOException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static final class SerializingOutput implements Output<StreamRecord<ArrowRowDataBatch>> {
        private final RowDataSerializer serializer;
        private final List<String> captured;

        private SerializingOutput(RowDataSerializer serializer, List<String> captured) {
            this.serializer = serializer;
            this.captured = captured;
        }

        @Override
        public void collect(StreamRecord<ArrowRowDataBatch> record) {
            ArrowRowDataBatch batch = record.getValue();
            for (int row = 0; row < batch.size(); row++) {
                RowData view = batch.rowView(row);
                view.setRowKind(batch.rowKind(row));
                captured.add(serialize(serializer, view));
            }
        }

        @Override
        public void close() {}

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void emitWatermark(WatermarkEvent watermarkEvent) {}
    }
}
