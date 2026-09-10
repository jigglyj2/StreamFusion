/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Channel replay updates a retained variable-length maximum and an independent membership filter. */
class StringAggregateChannelRecoveryTest extends SharedAggregateChannelRecoveryTest {
    @Override
    protected RowType inputType() {
        return StringAggregateFlinkOracle.INPUT;
    }

    @Override
    protected RowType outputType() {
        return StringAggregateFlinkOracle.OUTPUT;
    }

    @Override
    protected RowData oracleInput(GenericRowData row) throws Exception {
        return StringAggregateFlinkOracle.binaryInput(row);
    }

    @Override
    protected int[] groupingIndices() {
        return new int[] {0, 1};
    }

    @Override
    protected byte[] plan() {
        return StringAggregateFixture.plan();
    }

    @Override
    protected KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean rocks) throws Exception {
        return StringAggregateFlinkOracle.create(rocks, null, true);
    }

    @Override
    protected GenericRowData row(long value, RowKind kind) {
        var row = GenericRowData.of(
                StringData.fromString("é-group"),
                StringData.fromString("part"),
                StringData.fromString(value == Long.MAX_VALUE ? "é".repeat(4096) : "\uD800\uDC00"),
                value != Long.MAX_VALUE,
                7L);
        row.setRowKind(RowKind.INSERT);
        return row;
    }
}
