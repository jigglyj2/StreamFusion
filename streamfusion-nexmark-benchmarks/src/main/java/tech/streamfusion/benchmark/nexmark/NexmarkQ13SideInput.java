/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** The upstream Q13 fixture, shared by both engines and cleaned with the job's temporary files. */
final class NexmarkQ13SideInput {
    private NexmarkQ13SideInput() {}

    static String prepare(Path directory) throws IOException {
        Path file = directory.resolve("side_input.txt");
        // Exactly SideInputGenerator's default contents. This is benchmark setup, not a source/operator optimization.
        try (var writer = Files.newBufferedWriter(file)) {
            for (int i = 0; i < 10_000; i++) {
                writer.write(i + "," + i);
                writer.newLine();
            }
        }
        return "CREATE TABLE side_input (key BIGINT, `value` VARCHAR) WITH ("
                + "'connector.type'='filesystem','connector.path'='"
                + file.toUri().toString().replace("'", "''") + "','format.type'='csv')";
    }
}
