/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.minibatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import org.apache.flink.streaming.api.operators.StreamOperatorUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.wmassigners.ProcTimeMiniBatchAssignerOperator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.arrow.ArrowRowDataBatch;
import tech.streamfusion.flink.arrow.ArrowRowDataBatchSerializer;

/** Timers keep using clock intervals after terminal watermarks, as Flink's assigner does. */
class GeneratedMiniBatchTimerParityTest {
    @ParameterizedTest
    @ValueSource(longs = {1, 5, 100})
    void generatedTimerDeadlinesAndWatermarksMatchFlinkAfterEndOfInput(long interval) throws Exception {
        var original = new ProcTimeMiniBatchAssignerOperator(interval);
        var nativeOperator = new StreamFusionArrowProcTimeMiniBatchAssignerOperator(interval);
        var originalClock = new RecordingClock();
        var nativeClock = new RecordingClock();
        try (var oracle = new OneInputStreamOperatorTestHarness<RowData, RowData>(original);
                var target =
                        new OneInputStreamOperatorTestHarness<ArrowRowDataBatch, ArrowRowDataBatch>(nativeOperator)) {
            oracle.setup();
            target.setup(ArrowRowDataBatchSerializer.INSTANCE);
            StreamOperatorUtils.setProcessingTimeService(original, originalClock);
            StreamOperatorUtils.setProcessingTimeService(nativeOperator, nativeClock);
            oracle.open();
            target.open();
            var random = new Random(interval);
            for (int step = 0; step < 30; step++) {
                long now = originalClock.now + random.nextInt(3) * interval + (step % 2);
                originalClock.now = now;
                nativeClock.now = now;
                if (step >= 15) {
                    oracle.processWatermark(Watermark.MAX_WATERMARK);
                    target.processWatermark(Watermark.MAX_WATERMARK);
                } else {
                    oracle.processWatermark(new Watermark(now + 1));
                    target.processWatermark(new Watermark(now + 1));
                }
                // A queued callback can run after upstream end-of-input; inspect its
                // deadline without draining an incorrectly rearmed past timer forever.
                original.onProcessingTime(now - 1);
                nativeOperator.onProcessingTime(now - 1);
                assertThat(nativeClock.deadlines)
                        .as("interval=%s step=%s", interval, step)
                        .containsExactlyElementsOf(originalClock.deadlines);
                assertThat(new ArrayList<>(target.getOutput())).containsExactlyElementsOf(oracle.getOutput());
                target.getOutput().clear();
                oracle.getOutput().clear();
            }
        }
    }

    private static final class RecordingClock extends TestProcessingTimeService {
        long now;
        final List<Long> deadlines = new ArrayList<>();

        @Override
        public long getCurrentProcessingTime() {
            return now;
        }

        @Override
        public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
            deadlines.add(timestamp);
            return super.registerTimer(timestamp, callback);
        }
    }
}
