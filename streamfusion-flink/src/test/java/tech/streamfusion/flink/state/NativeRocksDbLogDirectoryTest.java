/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBResourceContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.rocksdb.RocksDB;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.NativeStateBindings;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class NativeRocksDbLogDirectoryTest {
    @TempDir
    Path temporary;

    @Test
    void resolvesTheSameDefaultAsFlinkForTaskManagerLogLocationsAndLongDatabasePaths() throws Exception {
        RocksDB.loadLibrary();
        Path log = Files.createFile(temporary.resolve("taskmanager.log"));
        String previous = System.getProperty("log.file");
        try {
            for (Path base : List.of(temporary.resolve("short"), temporary.resolve("x".repeat(230)))) {
                for (String value : new String[] {
                    null, log.toString(), temporary.resolve("missing.log").toString()
                }) {
                    if (value == null) System.clearProperty("log.file");
                    else System.setProperty("log.file", value);
                    try (var flink = new RocksDBResourceContainer(
                            new Configuration(), PredefinedOptions.DEFAULT, null, null, base.toFile(), false)) {
                        String expected = flink.getDbOptions().dbLogDir();
                        Path actual = NativeRocksDbLogDirectory.resolve(base.resolve("db"));
                        assertThat(actual == null ? "" : actual.toString()).isEqualTo(expected);
                    }
                }
            }
        } finally {
            if (previous == null) System.clearProperty("log.file");
            else System.setProperty("log.file", previous);
        }
    }

    @Test
    void serializesResolvedTaskConfigurationWithAnExplicitProtocolVersion() throws Exception {
        Path database = temporary.resolve("db");
        Path logs = temporary.resolve("logs");
        var relocated = NativeStateResources.rocksDb(2, 16, 0, 15, database, 64L << 20, logs);
        var decoded = NativeStateBindings.parseFrom(NativeStateResources.serialize(List.of(relocated)));
        assertThat(decoded.getProtocolVersion()).isEqualTo(2);
        assertThat(decoded.getBindings(0).getRocksdb().getLogDirectory()).isEqualTo(logs.toString());
        var local = NativeStateResources.rocksDb(2, 16, 0, 15, database, 64L << 20);
        decoded = NativeStateBindings.parseFrom(NativeStateResources.serialize(List.of(local)));
        assertThat(decoded.getProtocolVersion()).isEqualTo(1);
        assertThat(decoded.getBindings(0).getRocksdb().hasLogDirectory()).isFalse();
    }
}
