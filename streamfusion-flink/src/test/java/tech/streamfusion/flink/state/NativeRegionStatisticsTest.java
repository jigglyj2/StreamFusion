/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.proto.plan.v1.NativeGaugeSchema;
import tech.streamfusion.proto.plan.v1.NativeGaugeValueKind;

class NativeRegionStatisticsTest {
    @Test
    void samplesEveryStageThroughJniAndPreservesGeneratedFlinkChangelogAcrossRestore(@TempDir Path root)
            throws Exception {
        var range = new KeyGroupRange(0, 15);
        var source = new NativeRegionCheckpointTest.Region(true, range, root.resolve("source"), true);
        try (source;
                var restored = new NativeRegionCheckpointTest.Region(true, range, root.resolve("restored"), true);
                var oracle = NativeRegionCheckpointTest.flink()) {
            var schema = NativeGaugeSchema.parseFrom(source.context.stateStatisticsSchema());
            assertThat(schema.getProtocolVersion()).isEqualTo(1);
            assertThat(schema.getGaugesCount()).isEqualTo(22);
            var names = java.nio.file.Files.readAllLines(
                    Path.of("../streamfusion-state-rocksdb/tests/fixtures/flink-rocksdb-ticker-names.txt"));
            for (int index = 0; index < schema.getGaugesCount(); index++) {
                var gauge = schema.getGauges(index);
                assertThat(gauge.getPlanNodeId()).isEqualTo(index < 11 ? 2L : 3L);
                assertThat(gauge.getName()).isEqualTo(names.get(index % 11));
                assertThat(gauge.getValueKind()).isEqualTo(NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT64);
                assertThat(gauge.getGroupsList()).isEmpty();
            }
            assertThat(source.context.stateStatisticsSnapshot()).containsOnly(0L);
            NativeRegionCheckpointTest.compare(source, oracle, NativeRegionCheckpointTest.rows(31));
            long[] written = source.context.stateStatisticsSnapshot();
            assertThat(written[7]).isPositive();
            assertThat(written[18]).isPositive();
            assertThat(written[5]).isZero();
            assertThat(written[16]).isZero();
            for (long id : new long[] {2, 3}) {
                Path checkpoint = root.resolve("checkpoint-" + id);
                source.context.state().checkpoint(id, checkpoint);
                restored.context.state().importCheckpoint(id, checkpoint, 0, 15, 4L << 20);
            }
            long[] imported = restored.context.stateStatisticsSnapshot();
            assertThat(imported[6]).isPositive();
            assertThat(imported[17]).isPositive();
            NativeRegionCheckpointTest.compare(restored, oracle, NativeRegionCheckpointTest.rows(32));
            for (int index = 0; index < 50; index++) {
                long available = restored.memory.available();
                assertThat(restored.context.stateStatisticsSnapshot()).hasSize(22);
                assertThat(restored.memory.available()).isEqualTo(available);
            }
        }
        assertThatThrownBy(source.context::stateStatisticsSnapshot).hasMessageContaining("closed");
    }
}
