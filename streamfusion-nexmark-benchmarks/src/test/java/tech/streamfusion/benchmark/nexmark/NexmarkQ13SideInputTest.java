/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NexmarkQ13SideInputTest {
    @TempDir
    Path directory;

    @Test
    void createsTheOriginalSideInputAndLegacyLookupSchema() throws Exception {
        assertThat(NexmarkQ13SideInput.prepare(directory))
                .contains(
                        "key BIGINT, `value` VARCHAR",
                        "'connector.type'='filesystem'",
                        "'format.type'='csv'",
                        directory.resolve("side_input.txt").toUri().toString());
        assertThat(Files.readAllLines(directory.resolve("side_input.txt")))
                .containsExactlyElementsOf(
                        IntStream.range(0, 10_000).mapToObj(i -> i + "," + i).collect(Collectors.toList()));
        assertThat(NexmarkRowDataJob.blackholeSinkDdl("q13")).contains("`value` STRING", "'connector'='blackhole'");
    }
}
