/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.nativebridge;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

final class NativeLibraryLoader {
    private static boolean loaded;

    private NativeLibraryLoader() {}

    static synchronized void load() {
        if (loaded) {
            return;
        }
        String resource = "/META-INF/native/linux-x86_64/libstreamfusion_native.so";
        try (InputStream library = NativeLibraryLoader.class.getResourceAsStream(resource)) {
            if (library == null) {
                throw new IllegalStateException("StreamFusion native library is missing: " + resource);
            }
            Path extracted = Files.createTempFile("streamfusion-native-", ".so");
            Files.copy(library, extracted, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            extracted.toFile().deleteOnExit();
            System.load(extracted.toAbsolutePath().toString());
            loaded = true;
        } catch (IOException error) {
            throw new IllegalStateException("Could not extract the StreamFusion native library", error);
        }
    }
}
