/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.AggregateCall;
import tech.streamfusion.proto.plan.v1.AggregateFunction;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LocalGroupAggregate;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

class StreamFusionArrowLocalGroupAggregateOperatorTest {
    private static final RowType INPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new BigIntType(true)}, new String[] {"bidder", "price"});
    private static final RowType OUTPUT_TYPE = RowType.of(
            new LogicalType[] {new BigIntType(false), new VarBinaryType(false, VarBinaryType.MAX_LENGTH)},
            new String[] {"bidder", "accumulator"});

    @Test
    void exposesFlinkBundleAndLogicalIoMetrics() throws Exception {
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
        RowDataKeySelector selector = KeySelectorUtil.getRowDataSelector(
                getClass().getClassLoader(), new int[] {0}, InternalTypeInfo.of(INPUT_TYPE));
        StreamFusionArrowLocalGroupAggregateOperator operator = new StreamFusionArrowLocalGroupAggregateOperator(
                plan(), INPUT_TYPE, OUTPUT_TYPE, new int[] {0}, false, selector, false);

        try (RootAllocator allocator = new RootAllocator(64L << 20);
                ArrowRowDataBatch first = ArrowRowDataBatch.transpose(
                        List.of(GenericRowData.of(7L, 10L), GenericRowData.of(7L, 20L)), INPUT_TYPE, allocator);
                ArrowRowDataBatch second =
                        ArrowRowDataBatch.transpose(List.of(GenericRowData.of(8L, 30L)), INPUT_TYPE, allocator);
                MockEnvironment environment = new MockEnvironmentBuilder()
                        .setTaskName("Local aggregate metric parity")
                        .setManagedMemorySize(64L << 20)
                        .setInputSplitProvider(new MockInputSplitProvider())
                        .setBufferSize(32 * 1024)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .setMetricGroup(taskMetrics)
                        .build();
                OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch> harness =
                        new OneInputStreamOperatorTestHarness<>(operator, environment)) {
            harness.setup(ArrowRowDataBatchSerializer.INSTANCE);
            harness.open();

            ((Counter) metrics.get("numRecordsIn")).inc();
            harness.processElement(new StreamRecord<>(first));
            assertThat(((Gauge<?>) metrics.get("bundleSize")).getValue()).isEqualTo(2);
            assertThat(((Gauge<?>) metrics.get("bundleRatio")).getValue()).isEqualTo(2.0);
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(2L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isZero();

            ((Counter) metrics.get("numRecordsIn")).inc();
            ((Counter) metrics.get("numRecordsOut")).inc();
            harness.processElement(new StreamRecord<>(second));
            assertThat(((Gauge<?>) metrics.get("bundleSize")).getValue()).isEqualTo(0);
            assertThat(((Gauge<?>) metrics.get("bundleRatio")).getValue()).isEqualTo(0.0);
            assertThat(((Counter) metrics.get("numRecordsIn")).getCount()).isEqualTo(3L);
            assertThat(((Counter) metrics.get("numRecordsOut")).getCount()).isEqualTo(2L);
        }
    }

    private static byte[] plan() {
        tech.streamfusion.proto.plan.v1.LogicalType bigint = tech.streamfusion.proto.plan.v1.LogicalType.newBuilder()
                .setBigint(EmptyType.getDefaultInstance())
                .build();
        LocalGroupAggregate aggregate = LocalGroupAggregate.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .addGroupingIndices(0)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                        .setOutputType(bigint))
                .setMiniBatchSize(3)
                .setInputSchema(schema(INPUT_TYPE))
                .setOutputSchema(schema(OUTPUT_TYPE))
                .build();
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setLocalGroupAggregate(aggregate))
                .build()
                .toByteArray();
    }

    private static tech.streamfusion.proto.plan.v1.Schema schema(RowType type) {
        tech.streamfusion.proto.plan.v1.Schema.Builder schema = tech.streamfusion.proto.plan.v1.Schema.newBuilder();
        for (RowType.RowField field : type.getFields()) {
            schema.addFields(tech.streamfusion.proto.plan.v1.Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        return schema.build();
    }
}
