/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.util.FileUtils;

/** A private restore directory; Flink retains ownership of the source local checkpoint. */
final class NativeCheckpointLocalRestore {
    private NativeCheckpointLocalRestore() {}

    static Path materialize(IncrementalLocalKeyedStateHandle handle, Path root, CloseableRegistry cancellation)
            throws Exception {
        Path directory = root == null
                ? Files.createTempDirectory("streamfusion-rocks-restore-")
                : Files.createTempDirectory(Files.createDirectories(root).toRealPath(), "streamfusion-rocks-restore-");
        var io = new CloseableRegistry();
        try {
            cancellation.registerCloseable(io);
            var emptyFiles = NativeCheckpointMetadata.emptyFiles(handle.getMetaDataStateHandle(), io);
            var emptyTargets = new java.util.HashSet<Path>();
            for (String relative : emptyFiles) {
                Path target = directory.resolve(relative).normalize();
                if (!target.startsWith(directory) || target.equals(directory) || !emptyTargets.add(target)) {
                    throw new IOException("Invalid or duplicate empty file in native local checkpoint: " + relative);
                }
            }
            Path source = handle.getDirectoryStateHandle().getDirectory().toRealPath();
            if (!Files.isDirectory(source)) throw new IOException("Native local checkpoint directory is missing");
            try (var paths = Files.walk(source)) {
                var iterator = paths.iterator();
                while (iterator.hasNext()) {
                    checkCancelled(io);
                    Path file = iterator.next();
                    if (file.equals(source)) continue;
                    Path target = directory.resolve(source.relativize(file));
                    if (Files.isSymbolicLink(file))
                        throw new IOException("Native local checkpoint contains a symbolic link");
                    if (Files.isDirectory(file, LinkOption.NOFOLLOW_LINKS)) {
                        Files.createDirectories(target);
                    } else if (Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS)) {
                        copy(file, target, io);
                    } else throw new IOException("Native local checkpoint contains a non-regular file");
                }
            }
            for (Path target : emptyTargets) {
                if (!Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS) || Files.size(target) != 0) {
                    throw new IOException("Invalid or missing empty file in native local checkpoint: "
                            + directory.relativize(target));
                }
            }
            checkCancelled(io);
            return directory;
        } catch (Exception | Error failure) {
            try {
                FileUtils.deleteDirectory(directory.toFile());
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        } finally {
            cancellation.unregisterCloseable(io);
            io.close();
        }
    }

    private static void copy(Path source, Path target, CloseableRegistry io) throws IOException {
        // Match Flink RocksDBHandle: immutable SSTs can be linked; mutable files must be copied.
        if (source.getFileName().toString().endsWith(".sst")) {
            try {
                Files.createLink(target, source);
                return;
            } catch (IOException unsupportedLink) {
                /* Cross-filesystem restore uses a real copy. */
            }
        }
        checkCancelled(io);
        var input = Files.newInputStream(source);
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
    }

    private static void checkCancelled(CloseableRegistry io) throws IOException {
        if (io.isClosed()) throw new IOException("Native local checkpoint restore cancelled");
    }
}
