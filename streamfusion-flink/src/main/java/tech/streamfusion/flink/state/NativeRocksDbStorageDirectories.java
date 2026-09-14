/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.StateBackend;

/** Flink's local root selection, without constructing a second Java RocksDB backend. */
final class NativeRocksDbStorageDirectories {
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(NativeRocksDbStorageDirectories.class);
    private final String[] configured;
    private File[] usable;
    private int nextDirectory;

    private NativeRocksDbStorageDirectories(String[] configured) {
        this.configured = configured == null ? null : configured.clone();
    }

    static NativeRocksDbStorageDirectories fromBackend(StateBackend backend) throws ReflectiveOperationException {
        return new NativeRocksDbStorageDirectories(
                (String[]) backend.getClass().getMethod("getDbStoragePaths").invoke(backend));
    }

    static String key() throws ReflectiveOperationException {
        return option().key();
    }

    static void validate(ReadableConfig config) throws ReflectiveOperationException {
        String paths = (String) config.get(option());
        if (paths == null) return;
        // Reuse Flink's public URI/path parser and its comma/platform-separator grammar.
        // This performs no filesystem probes and does not load the RocksDB JNI library.
        var type = Class.forName(
                "org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend",
                false,
                Thread.currentThread().getContextClassLoader());
        try {
            type.getMethod("setDbStoragePaths", String[].class)
                    .invoke(type.getConstructor().newInstance(), (Object) paths.split(",|" + File.pathSeparator));
        } catch (InvocationTargetException failure) {
            throw new IllegalArgumentException(
                    "Invalid " + key() + ": " + failure.getCause().getMessage(), failure.getCause());
        }
    }

    private static ConfigOption<?> option() throws ReflectiveOperationException {
        return (ConfigOption<?>) Class.forName(
                        "org.apache.flink.state.rocksdb.RocksDBOptions",
                        false,
                        Thread.currentThread().getContextClassLoader())
                .getField("LOCAL_DIRECTORIES")
                .get(null);
    }

    synchronized Path next(Environment environment) throws IOException {
        if (usable == null) initialize(environment);
        nextDirectory = (nextDirectory + 1) % usable.length;
        return usable[nextDirectory].toPath();
    }

    private void initialize(Environment environment) throws IOException {
        // EmbeddedRocksDBStateBackend.lazyInitializeForJob uses the TaskManager working
        // directory by default, rather than choosing an IOManager spill directory.
        if (configured == null) {
            usable = new File[] {environment.getTaskManagerInfo().getTmpWorkingDirectory()};
        } else {
            var directories = new ArrayList<File>();
            var errors = new StringBuilder();
            for (String path : configured) {
                File root = new File(path);
                File probe = new File(root, UUID.randomUUID().toString());
                if (probe.mkdirs()) {
                    directories.add(root);
                } else {
                    String message = "Local DB files directory '" + root + "' does not exist and cannot be created. ";
                    LOG.error(message);
                    errors.append(message);
                }
                // Match Flink's best-effort probe cleanup; never delete the shared root.
                probe.delete();
            }
            if (directories.isEmpty()) throw new IOException("No local storage directories available. " + errors);
            usable = directories.toArray(new File[0]);
        }
        nextDirectory = new Random().nextInt(usable.length);
    }
}
