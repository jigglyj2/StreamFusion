/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.util.FileUtils;

/** Flink selects the checkpoint root; each native backend owns only its child directory. */
final class NativeCheckpointLocalBackup {
    private NativeCheckpointLocalBackup() {}

    static String recoveryUnsupportedReason() {
        return "state backend: native RocksDB does not support " + StateRecoveryOptions.LOCAL_RECOVERY.key()
                + ": this native operator has no registered initialization in Flink's local-to-remote backend retry";
    }

    static Path prepare(LocalRecoveryConfig config, UUID backendId, long checkpointId) throws IOException {
        if (!config.isLocalBackupEnabled()) return null;
        var provider =
                config.getLocalStateDirectoryProvider().orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled());
        Path root = provider.subtaskSpecificCheckpointDirectory(checkpointId).toPath();
        Files.createDirectories(root);
        Path directory = root.resolve("streamfusion-" + backendId.toString().replace("-", ""));
        if (Files.exists(directory)) FileUtils.deleteDirectory(directory.toFile());
        // RocksDB requires a nonexistent checkpoint directory. Metadata is a sibling, created
        // by Flink's duplicating stream, so native region node namespaces stay unchanged.
        return directory;
    }
}
