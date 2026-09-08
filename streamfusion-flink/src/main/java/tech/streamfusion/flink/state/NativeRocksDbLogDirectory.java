/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.state;

import java.io.File;
import java.nio.file.Path;

/** Resolves Flink RocksDBResourceContainer's default log location in the TaskManager JVM. */
final class NativeRocksDbLogDirectory {
    private NativeRocksDbLogDirectory() {}

    static Path resolve(Path database) {
        // Flink leaves logs local when the flattened database name plus "_LOG" would be
        // longer than its 255-character filename limit (FLINK-31743).
        if (database.toAbsolutePath().normalize().toString().length() > 251) return null;
        String configured = System.getProperty("log.file");
        if (configured == null) return null;
        File file = new File(configured);
        if (!file.exists() || !file.canRead()) return null;
        // Keep Flink's behavior for a configured relative filename without a parent as well.
        File parent = new File(file.getParent());
        return parent.exists() && parent.canRead() ? parent.toPath() : null;
    }
}
