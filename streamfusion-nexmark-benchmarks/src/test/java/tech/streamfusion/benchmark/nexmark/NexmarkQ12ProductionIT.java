/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.TimeZone;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Official Q12 execution invariants; controlled-clock operator tests establish exact changelog parity. */
@ResourceLock("streamfusion-planner-property")
@ResourceLock("default-time-zone")
class NexmarkQ12ProductionIT {
    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void processingWindowsEmitValidOfficialNexmarkResults(String backend) throws Exception {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            for (boolean selected : new boolean[] {false, true}) {
                verify(backend, selected);
            }
        } finally {
            TimeZone.setDefault(previous);
            System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
            StreamFusionPlannerFactory.resetMetrics();
        }
    }

    private static void verify(String backend, boolean selected) throws Exception {
        // Keep the official ten-second window and max-speed source intact. This opt-in workload
        // must run long enough to emit a closed window; EOF does not flush an open window.
        long events = 50_000_000;
        var result = LocalRowDataNexmarkBenchmark.run(events, "q12", selected, backend, 4);
        assertThat(result.completed()).isTrue();
        assertThat(result.outputRows())
                .as("closed-window output, not just successful consumption")
                .isPositive();
        var keys = new HashSet<String>();
        long counted = 0;
        for (String row : result.debugRows()) {
            assertThat(row).startsWith("+I[").endsWith("]");
            String[] fields = row.substring(3, row.length() - 1).split(", ", -1);
            assertThat(fields).hasSize(4);
            assertThat(Long.parseLong(fields[0])).isNotNegative();
            long count = Long.parseLong(fields[1]);
            assertThat(count).isPositive();
            counted = Math.addExact(counted, count);
            long start =
                    LocalDateTime.parse(fields[2]).toInstant(ZoneOffset.UTC).toEpochMilli();
            long end = LocalDateTime.parse(fields[3]).toInstant(ZoneOffset.UTC).toEpochMilli();
            assertThat(Math.floorMod(start, 10_000)).isZero();
            assertThat(end - start).isEqualTo(10_000);
            assertThat(keys.add(fields[0] + ":" + start))
                    .as("one result per bidder/window")
                    .isTrue();
        }
        // Independent wall clocks assign different rows to windows and leave different final
        // partial windows behind. Do not compare their counts or absolute labels as byte parity.
        assertThat(counted).isBetween(1L, events);
        if (selected) {
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(result.nativePlanBatches()).isPositive();
            assertThat(result.nativeCalcBatches()).isPositive();
        } else {
            assertThat(result.nativePlanBatches()).isZero();
        }
    }
}
