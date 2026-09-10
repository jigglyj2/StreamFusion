/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.NativePlan;

/** Original Flink append Top-N, reusing SQL-generated nullable key selectors and comparators. */
final class AppendTopNFlinkFixture {
    private final TopOneFlinkFixture base;
    final int start;
    final int end;
    final boolean ascending;
    final boolean rankNumber;
    final boolean before;
    final RowType input;
    final RowType output;

    AppendTopNFlinkFixture(boolean ascending, boolean rankNumber, boolean before, int start, int end) throws Exception {
        base = new TopOneFlinkFixture(ascending, rankNumber, before);
        this.ascending = ascending;
        this.rankNumber = rankNumber;
        this.before = before;
        this.start = start;
        this.end = end;
        input = base.input;
        output = base.output;
    }

    byte[] plan() throws Exception {
        var plan = NativePlan.parseFrom(base.plan()).toBuilder();
        plan.getRootBuilder().getTopNBuilder().setRankStart(start).setRankEnd(end);
        return plan.build().toByteArray();
    }

    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean rocks) throws Exception {
        var harness = harness(rocks, 1, 0);
        harness.setup(new RowDataSerializer(output));
        harness.open();
        return harness;
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> harness(
            boolean rocks, int parallelism, int subtask) throws Exception {
        var original = ((KeyedProcessOperator<RowData, RowData, RowData>) base.stage.getOperator()).getUserFunction();
        var function = new AppendOnlyTopNFunction(
                (StateTtlConfig) FlinkExecNodeAccess.field(original, AbstractTopNFunction.class, "ttlConfig"),
                InternalTypeInfo.of(input),
                (GeneratedRecordComparator)
                        FlinkExecNodeAccess.field(original, AbstractTopNFunction.class, "generatedSortKeyComparator"),
                (RowDataKeySelector) FlinkExecNodeAccess.field(original, AbstractTopNFunction.class, "sortKeySelector"),
                RankType.ROW_NUMBER,
                new ConstantRankRange(start, end),
                base.before,
                base.rankNumber,
                org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RANK_TOPN_CACHE_SIZE
                        .defaultValue());
        var operator = new KeyedProcessOperator<>(function);
        function.setKeyContext(operator);
        var harness = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                operator,
                (KeySelector<RowData, RowData>) base.stage.getStateKeySelector(),
                (InternalTypeInfo<RowData>) base.stage.getStateKeyType(),
                16,
                parallelism,
                subtask);
        harness.setStateBackend(
                rocks
                        ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                        : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        return harness;
    }
}
