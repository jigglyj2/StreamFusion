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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

final class NativeLibraryLoader {
    private static boolean loaded;

    private NativeLibraryLoader() {}

    static synchronized void load() {
        if (loaded) {
            return;
        }
        String resource = findResource();
        try (InputStream library = NativeLibraryLoader.class.getResourceAsStream(resource)) {
            if (library == null) {
                throw new IllegalStateException("StreamFusion native library is missing: " + resource);
            }
            String suffix = resource.endsWith(".dylib") ? ".dylib" : ".so";
            Path extracted = Files.createTempFile("streamfusion-native-", suffix);
            Files.copy(library, extracted, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            extracted.toFile().deleteOnExit();
            System.load(extracted.toAbsolutePath().toString());
            loaded = true;
        } catch (IOException error) {
            throw new IllegalStateException("Could not extract the StreamFusion native library", error);
        }
    }

    static List<String> candidateResources(String osName, String architecture, String cpuFeatures) {
        String os = osName.toLowerCase(Locale.ROOT);
        String arch = architecture.toLowerCase(Locale.ROOT);
        List<String> resources = new ArrayList<>();
        if (os.contains("linux") && (arch.equals("amd64") || arch.equals("x86_64"))) {
            String features = cpuFeatures.toLowerCase(Locale.ROOT);
            if (hasAll(features, "avx512f", "avx512bw", "avx512dq", "avx512vl")) {
                resources.add("/META-INF/native/linux/x86_64/v4/libstreamfusion_native.so");
            }
            if (hasAll(features, "avx2", "bmi1", "bmi2", "fma")) {
                resources.add("/META-INF/native/linux/x86_64/v3/libstreamfusion_native.so");
            }
            resources.add("/META-INF/native/linux/x86_64/v2/libstreamfusion_native.so");
            resources.add("/META-INF/native/linux-x86_64/libstreamfusion_native.so");
        } else if (os.contains("linux") && (arch.equals("aarch64") || arch.equals("arm64"))) {
            resources.add("/META-INF/native/linux/aarch64/native/libstreamfusion_native.so");
        } else if (os.contains("mac") || os.contains("darwin")) {
            String macArch = arch.equals("aarch64") || arch.equals("arm64") ? "aarch64" : "x86_64";
            resources.add("/META-INF/native/darwin/" + macArch + "/native/libstreamfusion_native.dylib");
        }
        return resources;
    }

    private static String findResource() {
        List<String> candidates = candidateResources(
                System.getProperty("os.name"), System.getProperty("os.arch"), readLinuxCpuFeatures());
        return candidates.stream()
                .filter(resource -> NativeLibraryLoader.class.getResource(resource) != null)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No compatible StreamFusion native library found; tried " + candidates));
    }

    private static boolean hasAll(String features, String... required) {
        for (String feature : required) {
            if (!features.contains(feature)) {
                return false;
            }
        }
        return true;
    }

    private static String readLinuxCpuFeatures() {
        try {
            return Files.readString(Path.of("/proc/cpuinfo"));
        } catch (IOException ignored) {
            return "";
        }
    }
}
