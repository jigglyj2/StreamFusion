/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/** Append-only duplicates after channel replay must leave each independent presence bit intact. */
class AppendDistinctChannelRecoveryTest extends DistinctCountChannelRecoveryTest {
    @Override
    protected byte[] plan() {
        return DistinctCountFixture.plan(true);
    }

    @Override
    protected KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean rocks) throws Exception {
        return DistinctCountFlinkOracle.create(rocks, null, true, true);
    }

    @Override
    protected GenericRowData row(long value, RowKind ignored) {
        // Reuse the channel/barrier schedule with an INSERT-only stream on both engines.
        return super.row(value, RowKind.INSERT);
    }
}
