/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;

/** Clock boundaries inside Arrow inputs must flush the same logical bundles as Flink. */
class SharedMiniBatchAssignerRegionTest {
    @ParameterizedTest
    @CsvSource({"false,true", "true,true", "false,false", "true,false"})
    void generatedAssignerAndAggregateCompositionMatchesFlink(boolean rocks, boolean processingTime) throws Exception {
        for (int chunk : List.of(7, 1, 64))
            try (var fixture = new MiniBatchAssignerRegionFixture(rocks, processingTime, 5);
                    var allocator = new RootAllocator(64L << 20)) {
                var live = new ArrayList<GenericRowData>();
                var random = new Random(51);
                long clock = 0;
                for (int phase = 0; phase < 5; phase++) {
                    var rows = SharedAggregateRescalingTest.changes(live, phase);
                    for (int offset = 0; offset < rows.size(); offset += chunk) {
                        var part = rows.subList(offset, Math.min(rows.size(), offset + chunk));
                        var samples = new ArrayList<Long>();
                        var kinds = new RowKind[part.size()];
                        var present = new boolean[part.size()];
                        var times = new long[part.size()];
                        for (int row = 0; row < part.size(); row++) {
                            clock += random.nextInt(4);
                            if (processingTime) samples.add(clock);
                            kinds[row] = part.get(row).getRowKind();
                            present[row] = row % 3 != 0;
                            times[row] = offset + row;
                        }
                        try (var batch = ArrowRowDataBatch.transpose(part, SharedAggregateFlinkOracle.INPUT, allocator)
                                .withEnvelope(kinds, present, times)) {
                            fixture.input(batch, samples);
                        }
                    }
                    // Empty transport batches must neither consume a logical-record clock sample
                    // nor generate a new processing-time boundary.
                    fixture.flinkClock.now = clock + 10;
                    fixture.nativeClock.now = clock + 10;
                    try (var empty = ArrowRowDataBatch.empty(SharedAggregateFlinkOracle.INPUT, allocator)) {
                        fixture.input(empty, List.of());
                    }
                    fixture.watermark(clock + 5);
                    if (phase == 2) {
                        fixture.flinkRegion.getOperator().prepareSnapshotPreBarrier(9);
                        fixture.nativeRegion.region().prepareSnapshotPreBarrier(9);
                        fixture.check();
                    }
                }
                fixture.watermark(Long.MAX_VALUE);
                fixture.watermark(Long.MAX_VALUE);
                fixture.finish();
            }
    }
}
