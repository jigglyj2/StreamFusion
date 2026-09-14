/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.state.rocksdb.sstmerge.RocksDBManualCompactionOptions;

/** All six subordinate settings differ from defaults while Flink's manager remains disabled. */
public final class RocksDbDormantCompactionProfiles {
    private RocksDbDormantCompactionProfiles() {}

    public static Configuration disabled(long intervalNanos) {
        var options = new Configuration();
        options.set(RocksDBManualCompactionOptions.MIN_INTERVAL, Duration.ofNanos(intervalNanos));
        options.set(RocksDBManualCompactionOptions.MAX_PARALLEL_COMPACTIONS, 2);
        options.set(RocksDBManualCompactionOptions.MAX_FILE_SIZE_TO_COMPACT, new MemorySize(100 << 10));
        options.set(RocksDBManualCompactionOptions.MIN_FILES_TO_COMPACT, 3);
        options.set(RocksDBManualCompactionOptions.MAX_FILES_TO_COMPACT, 20);
        options.set(RocksDBManualCompactionOptions.MAX_OUTPUT_FILE_SIZE, MemorySize.ofMebiBytes(32));
        options.set(RocksDBManualCompactionOptions.MAX_AUTO_COMPACTIONS, 2);
        return options;
    }
}
