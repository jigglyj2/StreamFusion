/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.*;

/** Protocol fixtures for Calc -> stateful native HOP COUNT -> Calc. */
final class SharedSlicingWindowFixture {
    static final RowType INPUT = RowType.of(
            new BigIntType(),
            new VarBinaryType(false, VarBinaryType.MAX_LENGTH),
            new BigIntType(false),
            new BigIntType(false));
    static final RowType FLINK_INPUT = RowType.of(new BigIntType(), new BigIntType(false), new BigIntType(false));
    static final RowType OUTPUT = RowType.of(
            new BigIntType(), new BigIntType(false), new TimestampType(false, 3), new TimestampType(false, 3));

    private SharedSlicingWindowFixture() {}

    static byte[] count(long value) {
        return ByteBuffer.allocate(26)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(new byte[] {'S', 'F', 'G', 'A', 6})
                .putLong(value)
                .putInt(1)
                .put((byte) 1)
                .putLong(value)
                .array();
    }

    static byte[] resources(boolean rocks, Path directory, Long watermark) {
        var binding = rocks
                ? NativeStateResources.rocksDb(3, 128, 0, 127, directory, 8L << 20)
                : NativeStateResources.memory(3, 128, 0, 127);
        if (watermark != null)
            binding = binding.toBuilder().setRestoredWatermark(watermark).build();
        return NativeStateResources.serialize(List.of(binding));
    }

    static byte[] plan() throws Exception {
        return plan(false);
    }

    static byte[] plan(boolean tumble) throws Exception {
        var rowtime = new TimestampType(false, org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3);
        var strategy = new org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy(
                window(tumble), rowtime, 1);
        return compose(
                tech.streamfusion.flink.planner.window.StreamFusionGlobalWindowAggregateTranslator.createStagePlan(
                        RowType.of(new BigIntType(), rowtime),
                        INPUT,
                        OUTPUT,
                        1,
                        new org.apache.calcite.rel.core.AggregateCall[] {
                            call(org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT, List.of(), new BigIntType(false))
                        },
                        strategy,
                        properties(),
                        false,
                        config()),
                4,
                4);
    }

    static org.apache.flink.table.planner.plan.logical.WindowSpec window(boolean tumble) {
        return tumble
                ? new org.apache.flink.table.planner.plan.logical.TumblingWindowSpec(
                        java.time.Duration.ofSeconds(2), null)
                : hop();
    }

    static org.apache.flink.configuration.Configuration config() {
        var config = new org.apache.flink.configuration.Configuration();
        config.set(org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
        return config;
    }

    static org.apache.flink.table.planner.plan.logical.HoppingWindowSpec hop() {
        return new org.apache.flink.table.planner.plan.logical.HoppingWindowSpec(
                java.time.Duration.ofSeconds(6), java.time.Duration.ofSeconds(2), null);
    }

    static org.apache.flink.table.runtime.groupwindow.NamedWindowProperty[] properties() {
        var reference = new org.apache.flink.table.runtime.groupwindow.WindowReference(
                "window", new TimestampType(false, org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3));
        return new org.apache.flink.table.runtime.groupwindow.NamedWindowProperty[] {
            new org.apache.flink.table.runtime.groupwindow.NamedWindowProperty(
                    "window_start", new org.apache.flink.table.runtime.groupwindow.WindowStart(reference)),
            new org.apache.flink.table.runtime.groupwindow.NamedWindowProperty(
                    "window_end", new org.apache.flink.table.runtime.groupwindow.WindowEnd(reference))
        };
    }

    static org.apache.calcite.rel.core.AggregateCall call(
            org.apache.calcite.sql.SqlAggFunction function,
            List<Integer> inputs,
            org.apache.flink.table.types.logical.LogicalType output) {
        var types = new org.apache.flink.table.planner.calcite.FlinkTypeFactory(
                SharedSlicingWindowFixture.class.getClassLoader(),
                org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        return org.apache.calcite.rel.core.AggregateCall.create(
                function, false, inputs, -1, types.createFieldTypeFromLogicalType(output), "value");
    }

    static byte[] compose(byte[] fragment, int inputWidth, int outputWidth) throws Exception {
        var input = Operator.newBuilder()
                .setPlanNodeId(1)
                .setInput(Input.newBuilder())
                .build();
        var window = NativePlan.parseFrom(fragment).getRoot().toBuilder().setPlanNodeId(3);
        if (window.hasLocalWindowAggregate()) {
            window.getLocalWindowAggregateBuilder().setInput(calc(2, input, inputWidth));
        } else if (window.hasWindowAggregate()) {
            window.getWindowAggregateBuilder().setInput(calc(2, input, inputWidth));
        } else {
            throw new IllegalArgumentException("Expected a local or global window fragment");
        }
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(calc(4, window.build(), outputWidth))
                .build()
                .toByteArray();
    }

    static Schema schema(RowType type) {
        var schema = Schema.newBuilder();
        for (int i = 0; i < type.getFieldCount(); i++)
            schema.addFields(
                    Field.newBuilder().setName("f" + i).setType(FlinkLogicalTypeProto.serialize(type.getTypeAt(i))));
        return schema.build();
    }

    static Operator calc(long id, Operator child, int width) {
        var calc = Calc.newBuilder().setInput(child).setPreserveInputEnvelope(true);
        for (int i = 0; i < width; i++)
            calc.addProjections(Expression.newBuilder()
                    .setInputReference(InputReference.newBuilder().setIndex(i)));
        return Operator.newBuilder().setPlanNodeId(id).setCalc(calc).build();
    }
}
