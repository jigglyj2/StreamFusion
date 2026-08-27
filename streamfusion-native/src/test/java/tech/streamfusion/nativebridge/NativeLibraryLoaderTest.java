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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NativeLibraryLoaderTest {
    @Test
    void ordersNewestCompatibleX86IsaFirst() {
        assertThat(NativeLibraryLoader.candidateResources(
                        "Linux", "amd64", "avx2 bmi1 bmi2 fma avx512f avx512bw avx512dq avx512vl"))
                .extracting(path -> path.replaceAll(".*/(v[234])/.*", "$1"))
                .startsWith("v4", "v3", "v2");
    }

    @Test
    void selectsArchitectureSpecificMacLibraries() {
        assertThat(NativeLibraryLoader.candidateResources("Mac OS X", "aarch64", ""))
                .containsExactly("/META-INF/native/darwin/aarch64/native/libstreamfusion_native.dylib");
        assertThat(NativeLibraryLoader.candidateResources("Mac OS X", "x86_64", ""))
                .containsExactly("/META-INF/native/darwin/x86_64/native/libstreamfusion_native.dylib");
    }
}
