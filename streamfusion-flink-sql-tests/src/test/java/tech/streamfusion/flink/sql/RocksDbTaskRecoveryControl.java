/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.types.Row;
import tech.streamfusion.flink.state.StreamFusionKeyedStateBackend;
import tech.streamfusion.flink.state.StreamFusionStateBackend;

/** Test-only observation through Flink's StateBackend extension point; no production hooks. */
final class RocksDbTaskRecoveryControl {
    static final ConcurrentHashMap<String, RocksDbTaskRecoveryControl> RUNS = new ConcurrentHashMap<>();
    final List<Boolean> localAttempts = new CopyOnWriteArrayList<>();
    final List<Integer> restoredSourceOffsets = new CopyOnWriteArrayList<>();
    final boolean corrupt;
    final boolean nativeEngine;
    final Path backupRoot;
    volatile List<Row> result;
    volatile int corruptions;
    volatile int observedRows;
    volatile int completedCheckpointFailures;

    RocksDbTaskRecoveryControl(Path backupRoot, boolean corrupt, boolean nativeEngine) {
        this.backupRoot = backupRoot;
        this.corrupt = corrupt;
        this.nativeEngine = nativeEngine;
    }

    void beforeFailure(long checkpointId) throws Exception {
        if (!nativeEngine) return;
        try (var files = Files.walk(backupRoot)) {
            var manifests = files.filter(path -> path.getFileName().toString().equals("CURRENT")
                            && path.toString().contains("/chk_" + checkpointId + "/")
                            && path.getParent().getFileName().toString().startsWith("node-"))
                    .collect(java.util.stream.Collectors.toList());
            if (manifests.isEmpty()) throw new AssertionError("No native local backup for checkpoint " + checkpointId);
            for (Path manifest : manifests) {
                try (var filesInDatabase = Files.list(manifest.getParent())) {
                    if (filesInDatabase.noneMatch(path -> path.toString().endsWith(".sst")))
                        throw new AssertionError("Native checkpoint must contain persisted aggregate state");
                }
                if (corrupt) Files.writeString(manifest, "invalid-manifest\n");
            }
            if (corrupt) corruptions += manifests.size();
        }
    }

    static final class ObservedBackend implements StateBackend {
        private final String run;
        private final StateBackend delegate;
        private final boolean nativeEngine;

        ObservedBackend(String run, Configuration options, boolean nativeEngine, boolean incremental) {
            this.run = run;
            this.nativeEngine = nativeEngine;
            var rocks = new EmbeddedRocksDBStateBackend(incremental)
                    .configure(options, getClass().getClassLoader());
            delegate = nativeEngine ? new StreamFusionStateBackend(rocks, options) : rocks;
        }

        @Override
        public boolean useManagedMemory() {
            return delegate.useManagedMemory();
        }

        @Override
        public org.apache.flink.runtime.state.OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            return delegate.createOperatorStateBackend(parameters);
        }

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(KeyedStateBackendParameters<K> parameters)
                throws Exception {
            boolean restored = !parameters.getStateHandles().isEmpty();
            if (restored)
                RUNS.get(run)
                        .localAttempts
                        .add(
                                parameters.getStateHandles().iterator().next()
                                        instanceof IncrementalLocalKeyedStateHandle);
            var backend = delegate.createKeyedStateBackend(parameters);
            if (restored && nativeEngine && !((StreamFusionKeyedStateBackend<?>) backend).usesNativeFileCheckpoints())
                throw new AssertionError("Native file restore did not finish inside keyed-backend construction");
            return backend;
        }
    }
}
