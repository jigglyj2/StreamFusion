/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

final class NativeLibraryLoader {
    private static boolean loaded;

    private NativeLibraryLoader() {}

    static synchronized void load() {
        if (!loaded) {
            System.load(NativeArtifact.resolve("native").toString());
            loaded = true;
        }
    }
}
