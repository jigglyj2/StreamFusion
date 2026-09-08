/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** CPU information is inspected before loading either JNI or an optional native component. */
final class NativeCpuCapabilities {
    private final String os;
    private final String arch;
    private final Set<String> features;
    private final Set<String> identities;

    private NativeCpuCapabilities(String os, String arch, Set<String> features, Set<String> identities) {
        this.os = os;
        this.arch = arch;
        this.features = Set.copyOf(features);
        this.identities = Set.copyOf(identities);
    }

    String os() {
        return os;
    }

    String arch() {
        return arch;
    }

    Set<String> features() {
        return features;
    }

    Set<String> identities() {
        return identities;
    }

    static NativeCpuCapabilities current() throws IOException {
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String arch = architecture(System.getProperty("os.arch"));
        if (os.contains("linux")) return linux(arch, Files.readString(Path.of("/proc/cpuinfo")));
        if (os.contains("mac") || os.contains("darwin")) {
            Process process =
                    new ProcessBuilder("sysctl", "-a").redirectErrorStream(true).start();
            try {
                String text = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                if (process.waitFor() != 0) throw new IOException("Cannot read macOS CPU capabilities");
                return darwin(arch, text);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted reading CPU capabilities", error);
            } finally {
                process.destroy();
            }
        }
        throw new IOException("Unsupported native artifact OS: " + os);
    }

    static String architecture(String arch) {
        String normalized = arch.toLowerCase(Locale.ROOT);
        if (normalized.equals("amd64") || normalized.equals("x86_64")) return "x86_64";
        if (normalized.equals("arm64") || normalized.equals("aarch64")) return "aarch64";
        return normalized;
    }

    static NativeCpuCapabilities linux(String arch, String text) throws IOException {
        Set<String> features = null;
        Set<String> identities = new HashSet<>();
        for (String block : text.strip().split("\\n\\s*\\n")) {
            var values = values(block);
            String flags = values.getOrDefault("flags", values.get("features"));
            if (flags == null) continue;
            Set<String> perCpu = words(flags);
            if (features == null) features = new HashSet<>(perCpu);
            else features.retainAll(perCpu); // Every schedulable processor must support the artifact.
            identities.add(identity(
                    values,
                    Set.of("vendor_id", "cpu family", "model", "cpu implementer", "cpu architecture", "cpu part")));
        }
        if (features == null || features.isEmpty() || identities.isEmpty()) {
            throw new IOException("Cannot determine native CPU capabilities");
        }
        return new NativeCpuCapabilities("linux", architecture(arch), features, identities);
    }

    static NativeCpuCapabilities darwin(String arch, String text) throws IOException {
        var values = values(text);
        Set<String> features = new HashSet<>();
        values.forEach((key, value) -> {
            if (key.startsWith("hw.optional.") && value.equals("1")) features.add(key);
        });
        for (String key : Set.of("machdep.cpu.features", "machdep.cpu.extfeatures", "machdep.cpu.leaf7_features")) {
            features.addAll(words(values.getOrDefault(key, "")));
        }
        if (features.isEmpty()) throw new IOException("Cannot determine macOS CPU capabilities");
        String identity = identity(values, Set.of("machdep.cpu.brand_string", "hw.cputype", "hw.cpusubtype"));
        return new NativeCpuCapabilities("darwin", architecture(arch), features, Set.of(identity));
    }

    static Set<String> words(String text) {
        return Arrays.stream(text.strip().split("\\s+"))
                .filter(word -> !word.isEmpty())
                .collect(Collectors.toSet());
    }

    private static Map<String, String> values(String text) {
        var values = new TreeMap<String, String>();
        for (String line : text.toLowerCase(Locale.ROOT).split("\\n")) {
            int colon = line.indexOf(':');
            if (colon >= 0)
                values.put(
                        line.substring(0, colon).strip(),
                        line.substring(colon + 1).strip());
        }
        return values;
    }

    private static String identity(Map<String, String> values, Set<String> keys) throws IOException {
        String identity = values.entrySet().stream()
                .filter(entry -> keys.contains(entry.getKey()))
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining("\n"));
        if (identity.isEmpty()) throw new IOException("Cannot identify native CPU");
        return hex(sha256().digest(identity.getBytes(StandardCharsets.UTF_8)));
    }

    static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 15, 16));
            result.append(Character.forDigit(value & 15, 16));
        }
        return result.toString();
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("JVM does not provide SHA-256", error);
        }
    }
}
