/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Prevents a StreamFusion internal operator from silently becoming row-shaped again. */
class StreamFusionArrowArchitectureTest {
    @Test
    void rowDataExistsOnlyAtTheTwoExplicitRuntimeBoundaries() throws IOException {
        Path sources = Path.of("src/main/java/tech/streamfusion/flink");
        try (var files = Files.walk(sources)) {
            List<Path> javaFiles =
                    files.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList());
            for (Path file : javaFiles) {
                String source = Files.readString(file);
                String name = file.getFileName().toString();
                if (!name.equals("RowDataToArrowBatchOperator.java")) {
                    assertThat(source)
                            .as("RowData input is forbidden in %s", file)
                            .doesNotContain("OneInputStreamOperator<RowData,");
                }
                if (!name.equals("ArrowBatchToRowDataOperator.java")) {
                    assertThat(source)
                            .as("RowData output is forbidden in %s", file)
                            .doesNotContain("AbstractStreamOperator<RowData>");
                }
                assertThat(source)
                        .as("RowData multi-input is forbidden in %s", file)
                        .doesNotContain("MultipleInputStreamOperator<RowData>");
                if (!name.equals("ArrowRowDataBatch.java")) {
                    assertThat(source)
                            .as("operator-local RowData transpose is forbidden in %s", file)
                            .doesNotContain("ArrowRowDataBatch.transpose(");
                }
            }
        }
    }
}
