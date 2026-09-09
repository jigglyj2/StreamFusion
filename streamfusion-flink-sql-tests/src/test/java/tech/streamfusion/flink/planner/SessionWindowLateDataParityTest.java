/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowWindowAggregateCDataBridge;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeWindowAggregateBridge;
import tech.streamfusion.proto.plan.v1.*;

/** Controlled arrival-order oracle for the retained session kernel; this does not bypass planner admission. */
class SessionWindowLateDataParityTest {
    private static final RowType INPUT = RowType.of(new BigIntType(), new TimestampType(3));
    private static final RowType OUTPUT = RowType.of(
            new BigIntType(), new BigIntType(false), new TimestampType(false, 3), new TimestampType(false, 3));
    private static final String SQL = "SELECT k, COUNT(*) AS n, window_start, window_end FROM TABLE("
            + "SESSION(TABLE local_window_input PARTITION BY k, DESCRIPTOR(ts), INTERVAL '10' SECOND)) "
            + "GROUP BY k, window_start, window_end";

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void mergedNamespaceDeterminesLatenessInArrivalOrder(boolean rocks, @TempDir Path directory) throws Exception {
        for (int seed : List.of(3, 19, 71))
            for (int batchSize : List.of(1, 7, 31))
                try (var run = new Comparison(rocks, directory.resolve(seed + "-" + batchSize))) {
                    run.input(List.of(row(10000)));
                    run.watermark(15000);
                    // Its own window ended at 9999, but inclusive contact merges into live [10000,20000).
                    run.input(List.of(row(0)));
                    assertThat(NativeWindowAggregateBridge.lateRecordCount(run.handle))
                            .isZero();
                    run.watermark(19999);
                    var random = new Random(seed);
                    for (int phase = 0; phase < 8; phase++) {
                        long base = 40000 + phase * 40000L;
                        run.watermark(base + 15000);
                        var rows = new ArrayList<RowData>();
                        // First older event has no live session and must be dropped. A later input
                        // in the same Arrow batch must not retroactively resurrect it.
                        rows.add(row(base));
                        rows.add(row(base + 10000));
                        rows.add(row(base));
                        for (int i = 0; i < 61; i++) rows.add(row(base + (random.nextInt(51) - 10) * 1000L));
                        for (int offset = 0; offset < rows.size(); offset += batchSize)
                            run.input(rows.subList(offset, Math.min(rows.size(), offset + batchSize)));
                        run.watermark(base + 39999);
                    }
                    run.watermark(Long.MAX_VALUE);
                }
    }

    private static RowData row(long timestamp) {
        return GenericRowData.of(1L, TimestampData.fromEpochMillis(timestamp));
    }

    private static byte[] plan() {
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder()
                        .setWindowAggregate(WindowAggregate.newBuilder()
                                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                                .setInputSchema(SharedSlicingWindowFixture.schema(INPUT))
                                .setOutputSchema(SharedSlicingWindowFixture.schema(OUTPUT))
                                .addGroupingIndices(0)
                                .addAggregateCalls(AggregateCall.newBuilder()
                                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                                        .setOutputType(FlinkLogicalTypeProto.serialize(new BigIntType(false))))
                                .setTimeAttributeIndex(1)
                                .setKind(WindowKind.WINDOW_KIND_SESSION)
                                .setSizeMillis(10000)
                                .setShiftTimeZone("UTC")
                                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_START)
                                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_END)))
                .build()
                .toByteArray();
    }

    private static final class Comparison implements AutoCloseable {
        final SharedAggregateRegionParityTest.Memory memory = new SharedAggregateRegionParityTest.Memory();
        final RootAllocator allocator = new RootAllocator(64L << 20);
        final KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> flink;
        final RowDataSerializer input = new RowDataSerializer(INPUT);
        final RowDataSerializer output = new RowDataSerializer(OUTPUT);
        final long handle;

        Comparison(boolean rocks, Path directory) throws Exception {
            flink = GlobalWindowFlinkOracle.create(SlicingWindowFlinkPlan.stage("WindowAggregate", SQL), rocks, null);
            handle = rocks
                    ? NativeWindowAggregateBridge.createRocksDb(plan(), 128, 0, 127, directory, 8L << 20, memory)
                    : NativeWindowAggregateBridge.create(plan(), 128, 0, 127, memory);
        }

        void input(List<RowData> rows) throws Exception {
            for (var row : rows) flink.processElement(new StreamRecord<>(input.toBinaryRow(row), 123));
            try (var batch = ArrowRowDataBatch.transpose(rows, INPUT, allocator);
                    var result = ArrowWindowAggregateCDataBridge.process(
                            handle, batch, null, false, 0, OUTPUT, allocator, memory)) {
                compare(result);
            }
        }

        void watermark(long timestamp) throws Exception {
            flink.processWatermark(new Watermark(timestamp));
            try (var result =
                    ArrowWindowAggregateCDataBridge.advance(handle, false, timestamp, OUTPUT, allocator, memory)) {
                compare(result);
            }
        }

        void compare(ArrowRowDataBatch actual) throws Exception {
            var expectedBytes = new DataOutputSerializer(128);
            for (var event : flink.getOutput())
                if (event instanceof StreamRecord<?>) {
                    var record = (StreamRecord<?>) event;
                    assertThat(record.hasTimestamp()).isFalse();
                    output.serialize((RowData) record.getValue(), expectedBytes);
                }
            flink.getOutput().clear();
            var actualBytes = new DataOutputSerializer(128);
            for (int i = 0; i < actual.size(); i++) {
                var row = actual.rowView(i);
                row.setRowKind(actual.rowKind(i));
                output.serialize(row, actualBytes);
            }
            assertThat(actualBytes.getCopyOfBuffer()).containsExactly(expectedBytes.getCopyOfBuffer());
            var late = (Counter)
                    RegisteredMetricSurface.metrics(flink.getOperator().getMetricGroup())
                            .get("numLateRecordsDropped");
            assertThat(NativeWindowAggregateBridge.lateRecordCount(handle)).isEqualTo(late.getCount());
        }

        @Override
        public void close() throws Exception {
            try {
                NativeWindowAggregateBridge.destroy(handle);
                assertThat(memory.available()).isEqualTo(memory.limit());
            } finally {
                try {
                    allocator.close();
                } finally {
                    flink.close();
                }
            }
        }
    }
}
