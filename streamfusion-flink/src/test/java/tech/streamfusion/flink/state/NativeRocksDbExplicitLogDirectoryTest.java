/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBResourceContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import tech.streamfusion.nativebridge.NativeStateResources;
import tech.streamfusion.proto.plan.v1.NativeRocksDbOptions;
import tech.streamfusion.proto.plan.v1.NativeStateBindings;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class NativeRocksDbExplicitLogDirectoryTest {
    @TempDir
    Path temporary;

    @Test
    void generatedExplicitPathsMatchFlinkAndRetainLogsAfterClose() throws Exception {
        RocksDB.loadLibrary();
        for (int seed = 0; seed < 4; seed++) {
            Path base = Files.createDirectories(temporary.resolve("case-" + seed));
            if (seed >= 2) base = Files.createDirectories(base.resolve("x".repeat(160)));
            Path database = base.resolve("db");
            Path logs = temporary.resolve("日志-" + seed);
            if (seed % 2 == 0) Files.createDirectory(logs);
            var config = configuration(logs.toString());
            var resolved = NativeRocksDbConfiguration.fromConfig(config);
            assertThat(NativeStateConfigurationSupport.unsupportedReason(config))
                    .isNull();
            assertThat(NativeRocksDbLogDirectory.resolve(database, resolved)).isNull();
            try (var flink =
                    new RocksDBResourceContainer(config, PredefinedOptions.DEFAULT, null, null, base.toFile(), false)) {
                var dbOptions = flink.getDbOptions();
                assertThat(resolved.getLogDirectory()).isEqualTo(dbOptions.dbLogDir());
                var handles = new ArrayList<ColumnFamilyHandle>();
                try (var db = RocksDB.open(
                        dbOptions,
                        database.toString(),
                        List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, flink.getColumnOptions())),
                        handles)) {
                    db.put(new byte[] {1}, new byte[] {2});
                    assertThat(db.get(new byte[] {1})).containsExactly((byte) 2);
                    handles.forEach(ColumnFamilyHandle::close);
                }
            }
            try (var files = Files.list(logs)) {
                assertThat(files.filter(path -> path.getFileName().toString().endsWith("_LOG"))
                                .count())
                        .isEqualTo(1);
            }
            assertThat(database.resolve("LOG")).doesNotExist();
            var binding = NativeRocksDbConfiguration.bind(
                    NativeStateResources.rocksDb(2, 16, 0, 15, database, 64L << 20), resolved);
            var wire = NativeStateBindings.parseFrom(NativeStateResources.serialize(List.of(binding)));
            assertThat(wire.getProtocolVersion()).isEqualTo(10);
            assertThat(wire.getBindings(0).getRocksdb().hasLogDirectory()).isFalse();
            assertThat(wire.getBindings(0).getRocksdb().getDatabaseOptions().getLogDirectory())
                    .isEqualTo(logs.toString());
        }
    }

    @Test
    void configuredDirectorySurvivesBackendSerializationAndInvalidPathsReportTheOption() throws Exception {
        var config = configuration(temporary.resolve("logs").toString());
        var backend = org.apache.flink.util.InstantiationUtil.clone(new StreamFusionStateBackend(
                new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true), config));
        var field = StreamFusionStateBackend.class.getDeclaredField("nativeRocksDbOptions");
        field.setAccessible(true);
        assertThat(NativeRocksDbOptions.parseFrom((byte[]) field.get(backend)).getLogDirectory())
                .isEqualTo(config.get(RocksDBConfigurableOptions.LOG_DIR));
        for (String invalid : List.of("", "relative", "/tmp/log\0dir")) {
            assertThat(NativeStateConfigurationSupport.unsupportedReason(configuration(invalid)))
                    .contains(RocksDBConfigurableOptions.LOG_DIR.key());
        }
        for (String invalid : List.of("", "relative")) {
            assertThatThrownBy(() -> new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                            .configure(configuration(invalid), getClass().getClassLoader()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(RocksDBConfigurableOptions.LOG_DIR.key());
        }
    }

    @Test
    void upstreamFailsForUnusableLogDirectoriesWithoutRemovingUserFiles() throws Exception {
        RocksDB.loadLibrary();
        var file = Files.writeString(temporary.resolve("file"), "owned by user");
        int index = 0;
        for (var logs : List.of(file, temporary.resolve("absent-parent/logs"))) {
            var base = Files.createDirectory(temporary.resolve("case-" + index++));
            try (var flink = new RocksDBResourceContainer(
                    configuration(logs.toString()), PredefinedOptions.DEFAULT, null, null, base.toFile(), false)) {
                var handles = new ArrayList<ColumnFamilyHandle>();
                assertThatThrownBy(() -> RocksDB.open(
                                flink.getDbOptions(),
                                base.resolve("db").toString(),
                                List.of(new ColumnFamilyDescriptor(
                                        RocksDB.DEFAULT_COLUMN_FAMILY, flink.getColumnOptions())),
                                handles))
                        .isInstanceOf(org.rocksdb.RocksDBException.class);
                assertThat(handles).isEmpty();
            }
        }
        assertThat(Files.readString(file)).isEqualTo("owned by user");
    }

    @Test
    void explicitDirectoryDoesNotSilentlySwitchToLocalLogsForAnOverlongDatabasePrefix() throws Exception {
        RocksDB.loadLibrary();
        var base = Files.createDirectories(temporary.resolve("a".repeat(140)).resolve("b".repeat(140)));
        var logs = temporary.resolve("logs");
        var config = configuration(logs.toString());
        assertThat(NativeRocksDbLogDirectory.resolve(base.resolve("db"), NativeRocksDbConfiguration.fromConfig(config)))
                .isNull();
        try (var flink =
                new RocksDBResourceContainer(config, PredefinedOptions.DEFAULT, null, null, base.toFile(), false)) {
            var handles = new ArrayList<ColumnFamilyHandle>();
            assertThat(flink.getDbOptions().dbLogDir()).isEqualTo(logs.toString());
            assertThatThrownBy(() -> RocksDB.open(
                            flink.getDbOptions(),
                            base.resolve("db").toString(),
                            List.of(new ColumnFamilyDescriptor(
                                    RocksDB.DEFAULT_COLUMN_FAMILY, flink.getColumnOptions())),
                            handles))
                    .isInstanceOf(org.rocksdb.RocksDBException.class)
                    .hasMessageContaining("File name too long");
            assertThat(handles).isEmpty();
            assertThat(base.resolve("db/LOG")).doesNotExist();
        }
    }

    private static Configuration configuration(String directory) {
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(RocksDBConfigurableOptions.LOG_DIR, directory);
        return config;
    }
}
