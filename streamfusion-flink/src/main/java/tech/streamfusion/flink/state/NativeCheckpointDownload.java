/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Callable;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;

/** Downloads native file namespaces under the destination Flink backend's selected root. */
final class NativeCheckpointDownload {
    private NativeCheckpointDownload() {}

    static Path materialize(IncrementalRemoteKeyedStateHandle handle, Path root, NativeRocksDbTransfers transfers)
            throws Exception {
        return materialize(handle, root, transfers, null);
    }

    static Path materialize(
            IncrementalRemoteKeyedStateHandle handle,
            Path root,
            NativeRocksDbTransfers transfers,
            CloseableRegistry cancellation)
            throws Exception {
        Path directory = root == null
                ? Files.createTempDirectory("streamfusion-rocks-restore-")
                : Files.createTempDirectory(Files.createDirectories(root).toRealPath(), "streamfusion-rocks-restore-");
        var io = new CloseableRegistry();
        try {
            if (cancellation != null) cancellation.registerCloseable(io);
            if (transfers != null) transfers.register(io);
            var files = new ArrayList<>(handle.getSharedState());
            files.addAll(handle.getPrivateState());
            var destinations = new HashSet<Path>();
            var work = new ArrayList<Callable<Void>>();
            for (var file : files) {
                Path target = destination(directory, file.getLocalPath(), destinations);
                work.add(() -> {
                    if (io.isClosed()) throw new IOException("Native checkpoint download cancelled");
                    Files.createDirectories(target.getParent());
                    var input = file.getHandle().openInputStream();
                    io.registerCloseable(input);
                    try {
                        var output = Files.newOutputStream(target);
                        io.registerCloseable(output);
                        try {
                            org.apache.flink.util.IOUtils.copyBytes(input, output, 16 * 1024, false);
                        } finally {
                            if (io.unregisterCloseable(output)) output.close();
                        }
                    } finally {
                        if (io.unregisterCloseable(input)) input.close();
                    }
                    return null;
                });
            }
            var empty = new ArrayList<Path>();
            for (String file : NativeCheckpointMetadata.emptyFiles(handle.getMetaDataStateHandle(), io)) {
                empty.add(destination(directory, file, destinations));
            }
            NativeRocksDbTransfers.execute(transfers, work, () -> org.apache.flink.util.IOUtils.closeQuietly(io));
            if (io.isClosed()) throw new IOException("Native checkpoint download cancelled");
            for (Path target : empty) {
                Files.createDirectories(target.getParent());
                Files.createFile(target);
            }
            return directory;
        } catch (Exception | Error failure) {
            try {
                org.apache.flink.util.FileUtils.deleteDirectory(directory.toFile());
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        } finally {
            if (cancellation != null) cancellation.unregisterCloseable(io);
            if (transfers != null) transfers.unregister(io);
            io.close();
        }
    }

    private static Path destination(Path root, String relative, HashSet<Path> paths) throws IOException {
        Path result = root.resolve(relative).normalize();
        if (!result.startsWith(root) || result.equals(root))
            throw new IOException("RocksDB checkpoint path escapes its restore directory");
        if (!paths.add(result)) throw new IOException("Duplicate native RocksDB checkpoint path " + relative);
        return result;
    }
}
