/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.minibatch;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class MiniBatchAssignerArchitectureTest {
    @Test
    void clockBoundariesSelectArrowRangesWithoutInspectingOrTransposingPayloadRows() throws Exception {
        var source = Files.readString(
                Path.of(
                        "src/main/java/tech/streamfusion/flink/minibatch/StreamFusionArrowProcTimeMiniBatchAssignerOperator.java"));
        assertThat(source)
                .contains("OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>", "batch.slice(")
                .doesNotContain("rowView(", ".transpose(", "RowDataSerializer", "NativeBridge", "JNI");
    }
}
