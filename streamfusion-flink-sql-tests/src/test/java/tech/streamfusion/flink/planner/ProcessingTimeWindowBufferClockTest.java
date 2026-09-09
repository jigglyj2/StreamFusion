/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.ProcessingTimeWindowClockTest.assertRows;
import static tech.streamfusion.flink.planner.ProcessingTimeWindowClockTest.input;
import static tech.streamfusion.flink.planner.ProcessingTimeWindowClockTest.oracle;
import static tech.streamfusion.flink.planner.ProcessingTimeWindowClockTest.result;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Flink's buffer visibility is observable when timer timestamps repeat or the clock rolls back. */
class ProcessingTimeWindowBufferClockTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void repeatedDeadlineDoesNotFlushUntilCheckpoint(boolean rocks) throws Exception {
        for (int gap : List.of(1000, 10000, 37000)) {
            for (boolean checkpoint : List.of(false, true)) {
                try (var flink = oracle(rocks, gap, null)) {
                    flink.setProcessingTime(1);
                    input(flink, null, 2);
                    flink.setProcessingTime(gap - 1);
                    assertRows(flink, result(null, 2, 0, gap));
                    input(flink, null, 3);
                    if (checkpoint) flink.prepareSnapshotPreBarrier(7);
                    flink.setProcessingTime(gap - 1);
                    assertRows(flink, result(null, checkpoint ? 3 : 0, 0, gap));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rollbackTimerSeesPublishedStateButNotTheNewBufferedUpdates(boolean rocks) throws Exception {
        for (int gap : List.of(1000, 10000, 37000)) {
            try (var flink = oracle(rocks, gap, null)) {
                flink.setProcessingTime(gap + 1);
                input(flink, 7L, 2);
                flink.setProcessingTime(2L * gap - 1);
                assertRows(flink, result(7L, 2, gap, 2L * gap));
                flink.setProcessingTime(1);
                input(flink, 7L, 3);
                flink.setProcessingTime(gap - 1);
                assertRows(flink, result(7L, 0, 0, gap));
                flink.prepareSnapshotPreBarrier(9);
                flink.setProcessingTime(1);
                input(flink, 7L, 5);
                flink.setProcessingTime(gap - 1);
                assertRows(flink, result(7L, 3, 0, gap));
                // The five new records remain buffered even after the older timer clears state.
                flink.prepareSnapshotPreBarrier(10);
                flink.setProcessingTime(1);
                input(flink, 7L, 1);
                flink.setProcessingTime(gap - 1);
                assertRows(flink, result(7L, 5, 0, gap));
            }
        }
    }
}
