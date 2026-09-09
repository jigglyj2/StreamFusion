/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/** Processing-time integration must use the shared Arrow owner and the receiving Flink clock. */
class ProcessingWindowArchitectureTest {
    @Test
    void processingWindowsReuseTheSharedFactoryAndComputeWithoutAnotherNativeBoundary() throws Exception {
        var root = Path.of("../streamfusion-native/src/planner/operators/window_aggregate");
        assertThat(Files.readString(root.resolve("shared_execution.rs")))
                .contains("UnaryExec::new", "processing_time_input", "process_with_clock(&payload, clock)")
                .doesNotContain("JNIEnv", "process_arrow(", "new_current_thread", "SystemTime");
        assertThat(Files.readString(root.resolve("shared_processing.rs")))
                .contains("BufferedWindow::new_processing_time", "SharedSlices::new_processing_time")
                .doesNotContain("JNIEnv", "process_arrow(", "SystemTime", "Utc::now", "RecordBatch::concat");
        assertThat(Files.readString(root.resolve("shared_processing/execution.rs")))
                .contains("impl SharedWindowKernel", "clock.clone()", "batch.columns().to_vec()")
                .doesNotContain("JNIEnv", "new_current_thread", "SystemTime");
        assertThat(
                        Files.readString(
                                Path.of(
                                        "../streamfusion-flink-planner/src/main/java/tech/streamfusion/flink/planner/StreamFusionWindowAggregateConversions.java")))
                .doesNotContain("processingTimeWindowAggregate(", "folded.inputEdge", "folded.windowing");
        assertThat(Files.readString(
                        Path.of("src/main/java/tech/streamfusion/flink/window/NativeLocalWindowResources.java")))
                .contains("getProcessingTime()", "memoryBytes(environment, runtime)")
                .doesNotContain("ArrowRowDataBatch.transpose", "rowView(");
    }
}
