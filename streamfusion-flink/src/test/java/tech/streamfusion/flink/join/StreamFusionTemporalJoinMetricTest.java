/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.join;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingOperatorMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowExchangeCDataBridge;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.exchange.ArrowExchangeBatch;
import tech.streamfusion.flink.exchange.NativeExchangeFrame;
import tech.streamfusion.flink.exchange.NativeExchangeFrameKeySelector;
import tech.streamfusion.flink.exchange.NativeExchangePlanSerializer;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Complete logical-I/O, changelog, state, timer, and checkpoint metric coverage. */
class StreamFusionTemporalJoinMetricTest {
    private static final int MAX_PARALLELISM = 128;
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new TimestampType(false, 3), new VarCharType(true, VarCharType.MAX_LENGTH)
            },
            new String[] {"id", "event_time", "payload"});
    private static final RowType OUTPUT_TYPE = RowType.of(new LogicalType[] {
        new BigIntType(false),
        new TimestampType(false, 3),
        new VarCharType(true, VarCharType.MAX_LENGTH),
        new BigIntType(true),
        new TimestampType(true, 3),
        new VarCharType(true, VarCharType.MAX_LENGTH)
    });
    private static final byte[] EXCHANGE_PLAN =
            NativeExchangePlanSerializer.hash(INPUT_TYPE, new int[] {0}, MAX_PARALLELISM);

    @Test
    void exposesCompleteMetricsWithLogicalRecordAndTimerSemantics() throws Exception {
        InterceptingOperatorMetricGroup metrics = new InterceptingOperatorMetricGroup() {
            @Override
            public MetricGroup addGroup(String name) {
                return this;
            }

            @Override
            public MetricGroup addGroup(String key, String value) {
                return this;
            }
        };
        InterceptingTaskMetricGroup taskMetrics = new InterceptingTaskMetricGroup() {
            @Override
            public InternalOperatorMetricGroup getOrAddOperator(
                    OperatorID id, String name, Map<String, String> additionalVariables) {
                return metrics;
            }
        };
        try (RootAllocator inputs = new RootAllocator(64L << 20);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Temporal join metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setMaxParallelism(MAX_PARALLELISM)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build()) {
            NativeExchangeFrameKeySelector frameSelector = new NativeExchangeFrameKeySelector(MAX_PARALLELISM);
            try (MetricHarness harness = new MetricHarness(operator(), frameSelector, environment)) {
                harness.setStateBackend(new StreamFusionStateBackend(new HashMapStateBackend()));
                harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
                harness.open();

                ((Counter) metrics.get("numRecordsIn")).inc();
                process(harness, inputs, 1, row(7, 1_000, "right"));
                ((Counter) metrics.get("numRecordsIn")).inc();
                process(harness, inputs, 0, row(7, 1_500, "left"));
                ((Counter) metrics.get("numRecordsOut")).inc();
                harness.processWatermark1(new Watermark(1_500));
                harness.processWatermark2(new Watermark(1_500));

                assertCounter(metrics, "numRecordsIn", 2);
                assertCounter(metrics, "numRecordsOut", 1);
                assertCounter(metrics, "processedBatches", 2);
                assertCounter(metrics, "processedRows", 2);
                assertCounter(metrics, "emittedRows", 1);
                assertCounter(metrics, "emittedInserts", 1);
                assertCounter(metrics, "emittedUpdateBefores", 0);
                assertCounter(metrics, "emittedUpdateAfters", 0);
                assertCounter(metrics, "emittedDeletes", 0);
                assertCounter(metrics, "stateReadBatches", 3);
                assertCounter(metrics, "stateWriteBatches", 3);
                assertCounter(metrics, "processingFailures", 0);
                assertCounter(metrics, "watermarksAdvanced", 1);
                assertCounter(metrics, "eventTimeTimersFired", 1);
                assertCounter(metrics, "processingTimeTimersFired", 0);
                assertThat(((Counter) metrics.get("timersRegistered")).getCount())
                        .isGreaterThanOrEqualTo(1);
                assertCounter(metrics, "timersDeleted", 0);
                assertThat(((Counter) metrics.get("timersFired")).getCount()).isGreaterThanOrEqualTo(1);
                for (String name : List.of(
                        "checkpoints",
                        "alignedCheckpoints",
                        "unalignedCheckpoints",
                        "canonicalSavepoints",
                        "incrementalCheckpoints",
                        "checkpointBytes",
                        "incrementalUploadedBytes",
                        "incrementalReusedBytes",
                        "checkpointDurationNanos",
                        "checkpointFailures",
                        "restores",
                        "restoreBytes",
                        "restoreDurationNanos",
                        "restoreFailures",
                        "joinConditionEvaluations")) {
                    assertCounter(metrics, name, 0);
                }
                assertThat(((Gauge<?>) metrics.get("pendingEventTimeTimers")).getValue())
                        .isEqualTo(0L);
                assertThat(((Gauge<?>) metrics.get("pendingProcessingTimeTimers")).getValue())
                        .isEqualTo(0L);
                assertThat(((Gauge<?>) metrics.get("rocksDbBackend")).getValue())
                        .isEqualTo(0);
            }
        }
    }

    private static void assertCounter(InterceptingOperatorMetricGroup metrics, String name, long expected) {
        assertThat(metrics.get(name)).as(name).isInstanceOf(Counter.class);
        assertThat(((Counter) metrics.get(name)).getCount()).as(name).isEqualTo(expected);
    }

    private static StreamFusionArrowTemporalJoinOperator operator() {
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                StreamFusionTemporalJoinMetricTest.class.getClassLoader(),
                new int[] {0},
                InternalTypeInfo.of(INPUT_TYPE));
        return new StreamFusionArrowTemporalJoinOperator(
                INPUT_TYPE,
                INPUT_TYPE,
                OUTPUT_TYPE,
                new int[] {0},
                new int[] {0},
                StreamFusionTemporalJoinPlan.create(
                        INPUT_TYPE,
                        INPUT_TYPE,
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
                EXCHANGE_PLAN,
                EXCHANGE_PLAN,
                false,
                FlinkJoinType.INNER,
                null);
    }

    private static GenericRowData row(long key, long timestamp, String payload) {
        GenericRowData row =
                GenericRowData.of(key, TimestampData.fromEpochMillis(timestamp), StringData.fromString(payload));
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static void process(MetricHarness harness, RootAllocator allocator, int side, GenericRowData row)
            throws Exception {
        try (ArrowRowDataBatch input = ArrowRowDataBatch.transpose(List.of(row), INPUT_TYPE, allocator)
                        .withEnvelope(new RowKind[] {row.getRowKind()}, new boolean[] {false}, new long[] {0});
                ArrowExchangeBatch.EnvelopeBatch envelope = ArrowExchangeBatch.withEnvelope(input, INPUT_TYPE)) {
            List<NativeExchangeFrame> frames =
                    ArrowExchangeCDataBridge.route(EXCHANGE_PLAN, envelope.batch(), allocator);
            assertThat(frames).hasSize(1);
            StreamRecord<NativeExchangeFrame> record = new StreamRecord<>(frames.get(0));
            if (side == 0) {
                harness.processElement1(record);
            } else {
                harness.processElement2(record);
            }
        }
    }

    private static final class MetricHarness extends AbstractStreamOperatorTestHarness<ArrowRowDataBatch> {
        private final StreamFusionArrowTemporalJoinOperator operator;

        private MetricHarness(
                StreamFusionArrowTemporalJoinOperator operator,
                NativeExchangeFrameKeySelector selector,
                MockEnvironment environment)
                throws Exception {
            super(operator, environment);
            this.operator = operator;
            config.setStatePartitioner(0, selector);
            config.setStatePartitioner(1, selector);
            config.setStateKeySerializer(Types.INT.createSerializer(executionConfig.getSerializerConfig()));
            config.serializeAllConfigs();
        }

        private void processElement1(StreamRecord<NativeExchangeFrame> element) throws Exception {
            operator.setKeyContextElement1(element);
            operator.processElement1(element);
        }

        private void processElement2(StreamRecord<NativeExchangeFrame> element) throws Exception {
            operator.setKeyContextElement2(element);
            operator.processElement2(element);
        }

        private void processWatermark1(Watermark watermark) throws Exception {
            operator.processWatermark1(watermark);
        }

        private void processWatermark2(Watermark watermark) throws Exception {
            operator.processWatermark2(watermark);
        }
    }
}
