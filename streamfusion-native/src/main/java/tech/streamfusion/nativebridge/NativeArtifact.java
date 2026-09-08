/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Validates build-time CPU requirements and the checksum before exposing native code. */
final class NativeArtifact {
    private static final Map<String, Path> EXTRACTED = new HashMap<>();

    private NativeArtifact() {}

    static synchronized Path resolve(String component) {
        Path ready = EXTRACTED.get(component);
        if (ready != null) return ready;
        try {
            NativeCpuCapabilities cpu = NativeCpuCapabilities.current();
            List<String> failures = new ArrayList<>();
            for (String resource : candidates(cpu.os(), cpu.arch(), component)) {
                if (NativeArtifact.class.getResource(resource) == null) continue;
                Properties properties = new Properties();
                try (InputStream metadata = NativeArtifact.class.getResourceAsStream(resource + ".properties")) {
                    if (metadata == null) {
                        failures.add(resource + ": missing CPU metadata");
                        continue;
                    }
                    properties.load(metadata);
                }
                String reason = incompatible(properties, cpu);
                if (reason != null) {
                    failures.add(resource + ": " + reason);
                    continue;
                }
                Path extracted = extract(resource, properties.getProperty("sha256"));
                EXTRACTED.put(component, extracted);
                return extracted;
            }
            throw new IllegalStateException("No compatible StreamFusion " + component + " native artifact: "
                    + (failures.isEmpty() ? "platform library is missing" : String.join("; ", failures)));
        } catch (IOException error) {
            throw new IllegalStateException(
                    "Cannot verify StreamFusion " + component + " native artifact: " + error.getMessage(), error);
        }
    }

    static List<String> candidates(String os, String arch, String component) {
        String extension = os.equals("darwin") ? ".dylib" : ".so";
        String library = "/libstreamfusion_" + component + extension;
        String base = "/META-INF/native/" + os + "/" + arch + "/";
        List<String> resources = new ArrayList<>();
        resources.add(base + "native" + library);
        if (os.equals("linux") && arch.equals("x86_64")) {
            for (String tier : List.of("v4", "v3", "v2")) resources.add(base + tier + library);
            resources.add("/META-INF/native/linux-x86_64" + library);
        }
        return resources;
    }

    static String incompatible(Properties properties, NativeCpuCapabilities cpu) {
        if (!properties.getProperty("format", "").equals("1")) return "unsupported CPU metadata version";
        if (!cpu.os().equals(properties.getProperty("os")) || !cpu.arch().equals(properties.getProperty("arch"))) {
            return "artifact OS or architecture differs from this worker";
        }
        String target = properties.getProperty("cpu-target", "");
        if (target.equals("native")) {
            if (!cpu.identities().equals(NativeCpuCapabilities.words(properties.getProperty("cpu-identities", "")))) {
                return "native build CPU identity differs from this worker";
            }
        } else if (!cpu.os().equals("linux")
                || !cpu.arch().equals("x86_64")
                || !List.of("x86-64-v2", "x86-64-v3", "x86-64-v4").contains(target)) {
            return "unsupported artifact CPU target";
        }
        var required = NativeCpuCapabilities.words(properties.getProperty("required-features", ""));
        if (required.isEmpty()) return "artifact CPU requirements are missing";
        if (!cpu.features().containsAll(required)) {
            required.removeAll(cpu.features());
            return "worker is missing CPU features "
                    + required.stream().sorted().collect(java.util.stream.Collectors.toList());
        }
        if (!properties.getProperty("sha256", "").matches("[0-9a-f]{64}")) return "artifact checksum is missing";
        return null;
    }

    private static Path extract(String resource, String expected) throws IOException {
        Path extracted = Files.createTempFile("streamfusion-verified-", resource.endsWith(".dylib") ? ".dylib" : ".so");
        boolean complete = false;
        try (InputStream library = NativeArtifact.class.getResourceAsStream(resource)) {
            if (library == null) throw new IOException("Missing native library " + resource);
            copyVerified(library, extracted, expected);
            extracted.toFile().deleteOnExit();
            complete = true;
            return extracted;
        } finally {
            if (!complete) Files.deleteIfExists(extracted);
        }
    }

    static void copyVerified(InputStream library, Path destination, String expected) throws IOException {
        var digest = NativeCpuCapabilities.sha256();
        Files.copy(
                new DigestInputStream(library, digest), destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        if (!NativeCpuCapabilities.hex(digest.digest()).equals(expected)) {
            throw new IOException("Native library checksum differs from its CPU metadata");
        }
    }
}
