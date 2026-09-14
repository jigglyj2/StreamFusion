/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

class RocksDbMutableConfigurationParityTest {
    @TempDir
    Path temporary;

    @ParameterizedTest
    @CsvSource({"false,1", "false,2", "true,1", "true,2"})
    void changedPublicSettingsReachLiveAndRestoredNativeDatabasesWithoutChangingFlinkChangelogs(
            boolean explicit, int checkpointMode) throws Exception {
        var live = new ArrayList<GenericRowData>();
        try (var oracle = SharedAggregateFlinkOracle.create(true, 0, true);
                var allocator = new RootAllocator(64L << 20)) {
            OperatorSubtaskState snapshot;
            Path sourceRoot = temporary.resolve("source");
            try (var source = SharedAggregateRuntimeHarness.configuredBackend(
                    backend(sourceRoot, PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM, 50, explicit), null)) {
                checkOptions(sourceRoot, explicit ? 2 : 4, explicit ? 80L << 20 : 256L << 20);
                for (int seed = 0; seed < 3; seed++)
                    SharedAggregateCheckpointTest.compare(source, oracle, allocator, live, seed);
                snapshot = SharedAggregateCheckpointTest.snapshot(source, checkpointMode, 1);
                assertThat(snapshot.getManagedKeyedState()).isNotEmpty();
                assertThat(snapshot.getRawKeyedState()).isEmpty();
            }
            try {
                Path targetRoot = temporary.resolve("target");
                try (var target = SharedAggregateRuntimeHarness.configuredBackend(
                        backend(targetRoot, PredefinedOptions.DEFAULT, 0, explicit), snapshot)) {
                    checkOptions(targetRoot, 2, explicit ? 80L << 20 : 64L << 20);
                    for (int seed = 3; seed < 6; seed++)
                        SharedAggregateCheckpointTest.compare(target, oracle, allocator, live, seed);
                }
            } finally {
                snapshot.discardState();
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
        }
    }

    private static StreamFusionStateBackend backend(
            Path root, PredefinedOptions preset, long writeBatch, boolean explicit) throws Exception {
        var options = new Configuration();
        if (explicit) {
            options.set(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 2);
            options.set(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE, MemorySize.ofMebiBytes(80));
        }
        var flink = new EmbeddedRocksDBStateBackend(true)
                .configure(options, RocksDbMutableConfigurationParityTest.class.getClassLoader());
        flink.setDbStoragePath(root.toString());
        // Start with a different preset and threshold so reading the wrapper's original snapshot fails.
        flink.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED);
        flink.setWriteBatchSize(4096);
        var nativeBackend = new StreamFusionStateBackend(flink, options);
        flink.setPredefinedOptions(preset);
        flink.setWriteBatchSize(writeBatch);
        return InstantiationUtil.clone(nativeBackend);
    }

    private static void checkOptions(Path root, int backgroundJobs, long targetFileSize) throws Exception {
        try (var paths = Files.walk(root)) {
            var files = paths.filter(path -> path.getFileName().toString().startsWith("OPTIONS-"))
                    .collect(java.util.stream.Collectors.toList());
            assertThat(files).isNotEmpty();
            for (Path file : files) {
                assertThat(Files.readString(file))
                        .contains("max_background_jobs=" + backgroundJobs, "target_file_size_base=" + targetFileSize);
            }
        }
    }
}
