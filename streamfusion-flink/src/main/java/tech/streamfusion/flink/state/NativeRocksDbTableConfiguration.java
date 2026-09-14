/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import java.util.List;
import org.apache.flink.configuration.ReadableConfig;
import tech.streamfusion.proto.plan.v1.NativeRocksDbBloomFilter;
import tech.streamfusion.proto.plan.v1.NativeRocksDbCompression;
import tech.streamfusion.proto.plan.v1.NativeRocksDbOptions;

/** Typed mapping to codecs/filter policies compiled into the optional native state component. */
final class NativeRocksDbTableConfiguration {
    private NativeRocksDbTableConfiguration() {}

    static NativeRocksDbOptions.Builder apply(ReadableConfig config, NativeRocksDbOptions.Builder builder)
            throws ReflectiveOperationException {
        String log = ((Enum<?>) config.get(NativeRocksDbConfiguration.option("LOG_LEVEL"))).name();
        List<String> levels =
                List.of("DEBUG_LEVEL", "INFO_LEVEL", "WARN_LEVEL", "ERROR_LEVEL", "FATAL_LEVEL", "HEADER_LEVEL");
        int level = levels.indexOf(log);
        if (level < 0) throw unsupported("LOG_LEVEL", log);
        builder.setLogLevel(level);
        String style = ((Enum<?>) config.get(NativeRocksDbConfiguration.option("COMPACTION_STYLE"))).name();
        if (style.equals("LEVEL")) builder.setCompactionStyle(0);
        else if (style.equals("UNIVERSAL")) builder.setCompactionStyle(1);
        else if (style.equals("NONE")) builder.setCompactionStyle(3);
        else
            throw new UnsupportedOperationException("native RocksDB does not yet propagate "
                    + NativeRocksDbConfiguration.option("COMPACTION_STYLE").key() + "=" + style
                    + "; FIFO state-eviction parity has not been verified");
        var compression = NativeRocksDbCompression.newBuilder();
        for (Object value : (List<?>) config.get(NativeRocksDbConfiguration.option("COMPRESSION_PER_LEVEL"))) {
            String codec = ((Enum<?>) value).name();
            if (codec.equals("NO_COMPRESSION")) compression.addPerLevel(0);
            else if (codec.equals("SNAPPY_COMPRESSION")) compression.addPerLevel(1);
            else if (codec.equals("ZLIB_COMPRESSION")) compression.addPerLevel(2);
            else if (codec.equals("BZLIB2_COMPRESSION")) compression.addPerLevel(3);
            else if (codec.equals("LZ4_COMPRESSION")) compression.addPerLevel(4);
            else if (codec.equals("LZ4HC_COMPRESSION")) compression.addPerLevel(5);
            else if (codec.equals("ZSTD_COMPRESSION")) compression.addPerLevel(7);
            else throw unsupported("COMPRESSION_PER_LEVEL", codec);
        }
        builder.setCompression(compression);
        // Both Flink's FRocksDB 8.10 JNI and upstream RocksDB 11.8 round/clamp bits per key
        // identically (including NaN). Pass the value through; do not invent validation here.
        // The block-based-mode flag is obsolete and ignored by both upstream policies.
        return builder.setBloomFilter(NativeRocksDbBloomFilter.newBuilder()
                .setEnabled((Boolean) config.get(NativeRocksDbConfiguration.option("USE_BLOOM_FILTER")))
                .setBitsPerKey((Double) config.get(NativeRocksDbConfiguration.option("BLOOM_FILTER_BITS_PER_KEY")))
                .setBlockBasedMode(
                        (Boolean) config.get(NativeRocksDbConfiguration.option("BLOOM_FILTER_BLOCK_BASED_MODE"))));
    }

    private static UnsupportedOperationException unsupported(String field, String value)
            throws ReflectiveOperationException {
        return new UnsupportedOperationException("native RocksDB does not yet propagate "
                + NativeRocksDbConfiguration.option(field).key() + "=" + value);
    }
}
