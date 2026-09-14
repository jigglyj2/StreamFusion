/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.state.rocksdb.PredefinedOptions;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.state.rocksdb.RocksDBResourceContainer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;

/** The actual Flink resource container and FRocksDB SSTs are the codec configuration oracle. */
class NativeRocksDbCompressionConfigurationTest {
    @org.junit.jupiter.api.io.TempDir
    Path temporary;

    @ParameterizedTest
    @EnumSource(
            value = CompressionType.class,
            names = {
                "NO_COMPRESSION",
                "SNAPPY_COMPRESSION",
                "ZLIB_COMPRESSION",
                "BZLIB2_COMPRESSION",
                "LZ4_COMPRESSION",
                "LZ4HC_COMPRESSION",
                "ZSTD_COMPRESSION"
            })
    void generatedMixedListsMatchFlinkAndCreateReadableCompressedSsts(CompressionType codec) throws Exception {
        RocksDB.loadLibrary();
        var config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        var codecs = List.of(codec, CompressionType.NO_COMPRESSION, CompressionType.ZSTD_COMPRESSION);
        config.set(RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL, codecs);
        assertThat(NativeStateConfigurationSupport.unsupportedReason(config)).isNull();
        var resolved = NativeRocksDbConfiguration.fromConfig(config);
        var handles = new java.util.ArrayList<org.rocksdb.ColumnFamilyHandle>();
        var value = "compressible-state".repeat(512).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (var flink = new RocksDBResourceContainer(config, PredefinedOptions.DEFAULT, null, null, null, false)) {
            var cf = flink.getColumnOptions();
            assertThat(resolved.getCompression().getPerLevelList())
                    .containsExactlyElementsOf(cf.compressionPerLevel().stream()
                            .map(c -> (int) c.getValue())
                            .collect(java.util.stream.Collectors.toList()));
            try (var db = RocksDB.open(
                            flink.getDbOptions(),
                            temporary.resolve("db").toString(),
                            List.of(new org.rocksdb.ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cf)),
                            handles);
                    var flush = new FlushOptions().setWaitForFlush(true)) {
                try {
                    for (int key = 0; key < 512; key++)
                        db.put(java.nio.ByteBuffer.allocate(4).putInt(key).array(), value);
                    db.flush(flush);
                    var properties = db.getPropertiesOfAllTables().values();
                    assertThat(properties).isNotEmpty();
                    if (codec != CompressionType.NO_COMPRESSION)
                        assertThat(properties).allSatisfy(p -> {
                            assertThat(p.getCompressionName()).isNotEqualTo("NoCompression");
                            assertThat(p.getDataSize()).isLessThan(512L * value.length / 4);
                        });
                    for (int key = 0; key < 512; key++)
                        assertThat(db.get(java.nio.ByteBuffer.allocate(4)
                                        .putInt(key)
                                        .array()))
                                .isEqualTo(value);
                } finally {
                    handles.forEach(org.rocksdb.ColumnFamilyHandle::close);
                }
            }
        }
    }
}
