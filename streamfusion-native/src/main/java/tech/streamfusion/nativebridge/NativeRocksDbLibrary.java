/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.nativebridge;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Locates the independently packaged native RocksDB state component. */
final class NativeRocksDbLibrary {
    private static final String RESOURCE = "/META-INF/native/linux-x86_64/libstreamfusion_state_rocksdb.so";
    private static Path extractedLibrary;

    private NativeRocksDbLibrary() {}

    static boolean isAvailable() {
        return NativeRocksDbLibrary.class.getResource(RESOURCE) != null;
    }

    static synchronized Path path() {
        if (extractedLibrary != null) {
            return extractedLibrary;
        }
        try (InputStream library = NativeRocksDbLibrary.class.getResourceAsStream(RESOURCE)) {
            if (library == null) {
                throw new IllegalStateException(
                        "Flink selected RocksDB state, but streamfusion-state-rocksdb is not on the classpath");
            }
            Path extracted = Files.createTempFile("streamfusion-state-rocksdb-", ".so");
            Files.copy(library, extracted, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            extracted.toFile().deleteOnExit();
            extractedLibrary = extracted.toAbsolutePath();
            return extractedLibrary;
        } catch (IOException error) {
            throw new IllegalStateException("Could not extract the native RocksDB state component", error);
        }
    }
}
