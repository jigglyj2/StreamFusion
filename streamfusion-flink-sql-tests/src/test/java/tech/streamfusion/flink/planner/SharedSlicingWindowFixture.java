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

    static byte[] plan() {
        var input = Operator.newBuilder()
                .setPlanNodeId(1)
                .setInput(Input.newBuilder())
                .build();
        var window = WindowAggregate.newBuilder()
                .setInput(calc(2, input, 4))
                .addGroupingIndices(0)
                .setInputSchema(schema(INPUT))
                .setOutputSchema(schema(OUTPUT))
                .setKind(WindowKind.WINDOW_KIND_HOP)
                .setSizeMillis(6000)
                .setSlideOrStepMillis(2000)
                .setShiftTimeZone("UTC")
                .setPartialAccumulatorIndex(1)
                .setPartialWindowStartIndex(2)
                .setPartialSliceEndIndex(3)
                .setPartialWindowsAreSlices(true)
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_START)
                .addWindowProperties(WindowProperty.WINDOW_PROPERTY_END)
                .addAggregateCalls(AggregateCall.newBuilder()
                        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                        .setOutputType(FlinkLogicalTypeProto.serialize(new BigIntType(false))));
        return NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(calc(
                        4,
                        Operator.newBuilder()
                                .setPlanNodeId(3)
                                .setWindowAggregate(window)
                                .build(),
                        4))
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
