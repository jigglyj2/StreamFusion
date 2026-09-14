/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;

/** Native file namespaces and SST reuse around Flink-owned parallel checkpoint I/O. */
final class NativeCheckpointUpload {
    private NativeCheckpointUpload() {}

    static Result upload(
            UUID backendId,
            KeyGroupRange range,
            long checkpointId,
            boolean incremental,
            Map<String, SharedFile> completed,
            Path directory,
            NativeCheckpointUploadResources resources,
            NativeRocksDbTransfers transfers)
            throws Exception {
        List<Path> paths;
        try (var files = Files.walk(directory)) {
            paths = files.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
        var files = new ArrayList<File>();
        var emptyFiles = new ArrayList<String>();
        var pending = new ArrayList<Callable<Void>>();
        for (Path path : paths) {
            resources.checkCancelled();
            String relative = directory.relativize(path).toString().replace('\\', '/');
            if (relative.isEmpty() || relative.startsWith("/") || relative.contains("../")) {
                throw new IOException("Unsafe native RocksDB checkpoint path " + relative);
            }
            long size = Files.size(path);
            if (size == 0) {
                emptyFiles.add(relative);
                continue;
            }
            var file = new File(relative, size, incremental && relative.endsWith(".sst"));
            var previous = file.shared ? completed.get(relative) : null;
            if (previous != null && previous.size == size && resources.canReuse(previous.handle)) {
                resources.reuse(previous.handle);
                file.handle = previous.handle;
                file.reused = true;
            } else {
                pending.add(() -> {
                    file.handle = resources.upload(
                            path, file.shared ? CheckpointedStateScope.SHARED : CheckpointedStateScope.EXCLUSIVE);
                    return null;
                });
            }
            files.add(file);
        }
        NativeRocksDbTransfers.execute(transfers, pending, resources::cancelIO);
        var shared = new ArrayList<HandleAndLocalPath>();
        var exclusive = new ArrayList<HandleAndLocalPath>();
        var next = new HashMap<String, SharedFile>();
        long uploadedBytes = 0;
        long reusedBytes = 0;
        // Restore the original sorted order independently of file-transfer completion order.
        for (var file : files) {
            if (file.reused) reusedBytes += file.handle.getStateSize();
            else uploadedBytes += file.handle.getStateSize();
            var handle = HandleAndLocalPath.of(file.handle, file.relative);
            if (file.shared) {
                shared.add(handle);
                next.put(file.relative, new SharedFile(file.handle, file.size));
            } else exclusive.add(handle);
        }
        byte[] metadata = NativeCheckpointMetadata.encode(emptyFiles);
        var metadataResult = resources.uploadMetadata(metadata);
        var metadataHandle = metadataResult.getJobManagerOwnedSnapshot();
        uploadedBytes += metadataHandle.getStateSize();
        KeyedStateHandle localHandle = metadataResult.getTaskLocalSnapshot() == null
                ? null
                : new IncrementalLocalKeyedStateHandle(
                        backendId,
                        checkpointId,
                        resources.localDirectoryHandle(),
                        range,
                        metadataResult.getTaskLocalSnapshot(),
                        shared);
        return new Result(
                new IncrementalRemoteKeyedStateHandle(
                        backendId, range, checkpointId, shared, exclusive, metadataHandle, uploadedBytes),
                localHandle,
                uploadedBytes,
                reusedBytes,
                next);
    }

    private static final class File {
        final String relative;
        final long size;
        final boolean shared;
        StreamStateHandle handle;
        boolean reused;

        File(String relative, long size, boolean shared) {
            this.relative = relative;
            this.size = size;
            this.shared = shared;
        }
    }

    static final class SharedFile {
        final StreamStateHandle handle;
        final long size;

        SharedFile(StreamStateHandle handle, long size) {
            this.handle = handle;
            this.size = size;
        }
    }

    static final class Result {
        final KeyedStateHandle handle;
        final KeyedStateHandle localHandle;
        final long uploadedBytes;
        final long reusedBytes;
        final Map<String, SharedFile> sharedFiles;

        SnapshotResult<KeyedStateHandle> snapshotResult() {
            return localHandle == null ? SnapshotResult.of(handle) : SnapshotResult.withLocalState(handle, localHandle);
        }

        Result(
                KeyedStateHandle handle,
                KeyedStateHandle localHandle,
                long uploadedBytes,
                long reusedBytes,
                Map<String, SharedFile> sharedFiles) {
            this.handle = handle;
            this.localHandle = localHandle;
            this.uploadedBytes = uploadedBytes;
            this.reusedBytes = reusedBytes;
            this.sharedFiles = sharedFiles;
        }
    }
}
