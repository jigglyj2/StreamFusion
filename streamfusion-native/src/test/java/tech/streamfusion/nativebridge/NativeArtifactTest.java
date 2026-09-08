/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NativeArtifactTest {
    private static final String CPU = "vendor_id: GenuineIntel\ncpu family: 6\nmodel: 154\nflags: sse2 avx2 bmi1\n";

    @TempDir
    Path temporary;

    @Test
    void requiresCompleteMetadataAndChecksEveryProcessor() throws Exception {
        var builder = NativeCpuCapabilities.linux("amd64", CPU);
        var metadata = metadata(builder);
        assertThat(NativeArtifact.incompatible(metadata, builder)).isNull();
        var heterogeneous = NativeCpuCapabilities.linux("amd64", CPU + "\n" + CPU.replace(" avx2", ""));
        assertThat(NativeArtifact.incompatible(metadata, heterogeneous)).contains("missing CPU features", "avx2");
        assertThat(NativeArtifact.incompatible(
                        metadata, NativeCpuCapabilities.linux("amd64", CPU.replace("154", "151"))))
                .contains("CPU identity");
        metadata.remove("required-features");
        assertThat(NativeArtifact.incompatible(metadata, builder)).contains("requirements are missing");
        assertThat(NativeArtifact.incompatible(new Properties(), builder)).contains("metadata version");
    }

    @Test
    void portableTierRequiresAllDeclaredFeaturesAndExactArchitecture() throws Exception {
        var worker = NativeCpuCapabilities.linux("amd64", CPU);
        var metadata = metadata(worker);
        metadata.setProperty("cpu-target", "x86-64-v3");
        metadata.remove("cpu-identities");
        assertThat(NativeArtifact.incompatible(metadata, worker)).isNull();
        metadata.setProperty("required-features", "avx2 avx512cd");
        assertThat(NativeArtifact.incompatible(metadata, worker)).contains("avx512cd");
        metadata.setProperty("arch", "aarch64");
        assertThat(NativeArtifact.incompatible(metadata, worker)).contains("architecture");
    }

    @Test
    void matchesBuildMetadataIdentityEncodingOnLinuxAndDarwin() throws Exception {
        var linux = NativeCpuCapabilities.linux("amd64", CPU);
        assertThat(linux.identities())
                .containsExactly("260dec969707cba72a317a7e18dc56d00589add7c85246b707faa08087062985");
        var mac = NativeCpuCapabilities.darwin(
                "arm64",
                "hw.cputype: 16777228\nhw.cpusubtype: 2\nhw.optional.arm.FEAT_AES: 1\nhw.optional.arm.FEAT_SHA3: 0\n");
        assertThat(mac.arch()).isEqualTo("aarch64");
        assertThat(mac.features()).containsExactly("hw.optional.arm.feat_aes");
        assertThat(NativeArtifact.incompatible(metadata(mac), mac)).isNull();
        assertThatThrownBy(() -> NativeCpuCapabilities.linux("amd64", "processor: 0"))
                .isInstanceOf(IOException.class);
    }

    @Test
    void verifiesTheActualLibraryBytesBeforeTheyCanBeLoaded() throws Exception {
        byte[] bytes = "native fixture".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String digest = NativeCpuCapabilities.hex(NativeCpuCapabilities.sha256().digest(bytes));
        Path copy = temporary.resolve("library");
        NativeArtifact.copyVerified(new ByteArrayInputStream(bytes), copy, digest);
        assertThat(Files.readAllBytes(copy)).isEqualTo(bytes);
        assertThatThrownBy(() -> NativeArtifact.copyVerified(new ByteArrayInputStream(new byte[] {0}), copy, digest))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checksum differs");
    }

    @Test
    void nativeComponentsUseIndependentArchitectureSpecificResources() {
        assertThat(NativeArtifact.candidates("linux", "x86_64", "state_rocksdb"))
                .contains("/META-INF/native/linux/x86_64/v3/libstreamfusion_state_rocksdb.so")
                .doesNotContain("/META-INF/native/linux/x86_64/v3/libstreamfusion_native.so");
        assertThat(NativeArtifact.candidates("darwin", "aarch64", "native"))
                .containsExactly("/META-INF/native/darwin/aarch64/native/libstreamfusion_native.dylib");
    }

    private static Properties metadata(NativeCpuCapabilities cpu) {
        var properties = new Properties();
        properties.setProperty("format", "1");
        properties.setProperty("os", cpu.os());
        properties.setProperty("arch", cpu.arch());
        properties.setProperty("cpu-target", "native");
        properties.setProperty("required-features", String.join(" ", cpu.features()));
        properties.setProperty("cpu-identities", String.join(" ", cpu.identities()));
        properties.setProperty("sha256", "0".repeat(64));
        return properties;
    }
}
