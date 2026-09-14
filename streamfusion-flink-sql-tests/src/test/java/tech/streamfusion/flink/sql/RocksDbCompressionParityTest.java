/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.rocksdb.CompressionType;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.RocksDbConfigurationProfiles;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

class RocksDbCompressionParityTest extends SqlParityTestSupport {
    @ParameterizedTest
    @EnumSource(
            value = CompressionType.class,
            names = {
                "ZLIB_COMPRESSION",
                "BZLIB2_COMPRESSION",
                "LZ4_COMPRESSION",
                "LZ4HC_COMPRESSION",
                "ZSTD_COMPRESSION"
            })
    void eachCompiledCodecPreservesGeneratedCompleteChangelogs(CompressionType codec) throws Exception {
        var config = RocksDbConfigurationProfiles.indexOptions(1);
        config.set(
                RocksDBConfigurableOptions.COMPRESSION_PER_LEVEL,
                List.of(codec, CompressionType.NO_COMPRESSION, CompressionType.SNAPPY_COMPRESSION));
        for (boolean rocks : new boolean[] {false, true}) {
            var expected = GroupAggregateAdmissionParityTest.executeFilteredRetractions(false, rocks, 0.7, 0.2, config);
            var actual = GroupAggregateAdmissionParityTest.executeFilteredRetractions(true, rocks, 0.7, 0.2, config);
            assertThat(actual).isEqualTo(expected);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
        }
    }
}
