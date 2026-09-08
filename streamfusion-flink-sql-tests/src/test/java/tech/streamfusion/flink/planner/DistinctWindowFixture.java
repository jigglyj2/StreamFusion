/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import tech.streamfusion.flink.operator.StreamFusionNativeRegionOperatorFactory;
import tech.streamfusion.flink.planner.window.StreamFusionGlobalWindowAggregateTranslator;

/** DISTINCT-only TUMBLE with scalar BIGINT or composite BIGINT/VARCHAR grouping. */
final class DistinctWindowFixture {
    final boolean strings;
    final int keys;
    final RowType input, flinkInput, output;

    DistinctWindowFixture(boolean strings) {
        this.strings = strings;
        keys = strings ? 2 : 1;
        input = rowType(
                new VarBinaryType(false, VarBinaryType.MAX_LENGTH), new BigIntType(false), new BigIntType(false));
        flinkInput = rowType(new BigIntType(false));
        output = rowType(new TimestampType(false, 3), new TimestampType(false, 3));
    }

    private RowType rowType(LogicalType... tail) {
        var fields = new ArrayList<LogicalType>();
        fields.add(new BigIntType());
        if (strings) fields.add(new VarCharType(VarCharType.MAX_LENGTH));
        fields.addAll(List.of(tail));
        return RowType.of(fields.toArray(new LogicalType[0]));
    }

    byte[] plan() throws Exception {
        var rowtime = new TimestampType(false, org.apache.flink.table.types.logical.TimestampKind.ROWTIME, 3);
        var strategy = new org.apache.flink.table.planner.plan.logical.TimeAttributeWindowingStrategy(
                SharedSlicingWindowFixture.window(true), rowtime, keys);
        var fragment = StreamFusionGlobalWindowAggregateTranslator.createStagePlan(
                rowType(rowtime),
                input,
                output,
                keys,
                new AggregateCall[0],
                strategy,
                SharedSlicingWindowFixture.properties(),
                false,
                SharedSlicingWindowFixture.config());
        return SharedSlicingWindowFixture.compose(fragment, input.getFieldCount(), output.getFieldCount());
    }

    OneInputTransformation<?, ?> stage() throws Exception {
        var prefix = strings
                ? "WITH window_input AS (SELECT CAST(CHAR_LENGTH(k) AS BIGINT) AS id, k, ts FROM local_window_input) "
                : "";
        var grouping = strings ? "id, k" : "k";
        var table = strings ? "window_input" : "local_window_input";
        return SlicingWindowFlinkPlan.stage(
                "GlobalWindowAggregate",
                prefix + "SELECT " + grouping
                        + ", window_start, window_end FROM TABLE(TUMBLE(TABLE " + table
                        + ", DESCRIPTOR(ts), INTERVAL '2' SECOND)) GROUP BY " + grouping + ", window_start, window_end",
                strings);
    }

    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(
            boolean rocks, OperatorSubtaskState restored) throws Exception {
        return GlobalWindowFlinkOracle.create(stage(), rocks, restored);
    }

    StreamFusionNativeRegionOperatorFactory factory() throws Exception {
        return new StreamFusionNativeRegionOperatorFactory(List.of(input), output, plan(), List.of(3L));
    }

    GenericRowData row(int id, long end, boolean nativePartial) {
        var fields = new Object[keys + (nativePartial ? 3 : 1)];
        fields[0] = id % 13 == 0 ? null : (long) (id % 31);
        if (strings) {
            var values = new String[] {null, "", "1234567", "12345678", "é", "日本語", "🙂🙂", "é".repeat(257)};
            var value = values[Math.floorMod(id, values.length)];
            fields[1] = value == null ? null : StringData.fromString(value);
        }
        if (nativePartial) {
            fields[keys] = ByteBuffer.allocate(17)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .put(new byte[] {'S', 'F', 'G', 'A', 6})
                    .putLong(1)
                    .putInt(0)
                    .array();
            fields[keys + 1] = end - 2000;
        }
        fields[fields.length - 1] = end;
        return GenericRowData.of(fields);
    }
}
