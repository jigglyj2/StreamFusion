/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.net.URLClassLoader;
import org.junit.jupiter.api.Test;

class NativeExecutionDiagnosticsTest {
    @Test
    void counterAccessAndResetNeverLookUpOrLoadNativeLibraries() throws Exception {
        URL classes = NativeExecutionDiagnostics.class
                .getProtectionDomain()
                .getCodeSource()
                .getLocation();
        try (var isolated = new URLClassLoader(new URL[] {classes}, ClassLoader.getPlatformClassLoader()) {
            @Override
            public URL getResource(String name) {
                if (name.startsWith("META-INF/native/")) {
                    throw new AssertionError("Diagnostic access attempted to load native code: " + name);
                }
                return super.getResource(name);
            }
        }) {
            Class<?> diagnostics = isolated.loadClass(NativeExecutionDiagnostics.class.getName());
            diagnostics.getMethod("reset").invoke(null);
            assertThat(diagnostics.getMethod("planStreams").invoke(null)).isEqualTo(0L);
            assertThat(diagnostics.getMethod("calcBatches").invoke(null)).isEqualTo(0L);
        }
    }
}
