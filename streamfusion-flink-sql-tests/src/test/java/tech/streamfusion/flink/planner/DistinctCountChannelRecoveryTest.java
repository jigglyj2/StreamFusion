/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** The in-flight row duplicates the checkpointed member but has a different FILTER result. */
class DistinctCountChannelRecoveryTest extends SharedAggregateChannelRecoveryTest {
    @Override
    protected RowType inputType() {
        return DistinctCountFlinkOracle.INPUT;
    }

    @Override
    protected RowType outputType() {
        return DistinctCountFlinkOracle.OUTPUT;
    }

    @Override
    protected byte[] plan() {
        return DistinctCountFixture.plan();
    }

    @Override
    protected KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean rocks) throws Exception {
        return DistinctCountFlinkOracle.create(rocks);
    }

    @Override
    protected GenericRowData row(long value, RowKind kind) {
        var row = GenericRowData.of(StringData.fromString("é-group"), 7L, value == Long.MAX_VALUE);
        row.setRowKind(kind);
        return row;
    }
}
