/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.junit.jupiter.api.Test;

/** Locks the native hash fixtures to the Flink 2.3 implementation used for rescaling. */
class FlinkKeyGroupFixtureTest {
    @Test
    void binaryRowHashAndKeyGroupMatchNativeFixture() {
        byte[] bytes = new byte[] {1, 0, 0, 0};
        BinaryRowData row = new BinaryRowData(0);
        row.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);

        assertThat(row.hashCode()).isEqualTo(-559_580_957);
        assertThat(KeyGroupRangeAssignment.assignToKeyGroup(row, 128)).isEqualTo(2);
    }

    @Test
    void flinkMurmurVectorsMatchFlussRustReference() {
        assertThat(KeyGroupRangeAssignment.computeKeyGroupForKeyHash(0, Integer.MAX_VALUE))
                .isEqualTo(0x2362_f9de);
        assertThat(KeyGroupRangeAssignment.computeKeyGroupForKeyHash(42, Integer.MAX_VALUE))
                .isEqualTo(0x43a4_6e1d);
        assertThat(KeyGroupRangeAssignment.computeKeyGroupForKeyHash(-77, Integer.MAX_VALUE))
                .isEqualTo(0x2eeb_27de);
    }
}
