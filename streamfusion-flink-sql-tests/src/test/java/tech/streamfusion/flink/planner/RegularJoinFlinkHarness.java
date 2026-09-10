/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.StreamingJoinOperator;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

/** Uses the same non-unique synchronous inner-join function selected by StreamExecJoin. */
final class RegularJoinFlinkHarness {
    private RegularJoinFlinkHarness() {}

    static FlinkRegularJoinMetricOracle create(
            RowType input, RowType output, GeneratedJoinCondition condition, boolean rocks, OperatorID id, String name)
            throws Exception {
        var join = new StreamingJoinOperator(
                InternalTypeInfo.of(input),
                InternalTypeInfo.of(input),
                condition,
                JoinInputSideSpec.withoutUniqueKey(),
                JoinInputSideSpec.withoutUniqueKey(),
                false,
                false,
                new boolean[] {true},
                0,
                0);
        var keys = KeySelectorUtil.getRowDataSelector(
                RegularJoinFlinkHarness.class.getClassLoader(), new int[] {0}, InternalTypeInfo.of(input));
        var harness = new KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>(
                join, keys, keys, keys.getProducedType(), 16, 1, 0);
        harness.setStateBackend(
                rocks
                        ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                        : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        harness.getStreamConfig().setOperatorID(id);
        harness.getStreamConfig().setOperatorName(name);
        harness.setup(new RowDataSerializer(output));
        harness.open();
        return new FlinkRegularJoinMetricOracle(harness);
    }
}
