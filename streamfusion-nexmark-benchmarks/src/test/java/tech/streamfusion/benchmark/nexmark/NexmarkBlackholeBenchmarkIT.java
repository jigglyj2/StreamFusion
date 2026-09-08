package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Opt-in source-to-sink admission checks; collecting-sink tests supply the output oracle. */
class NexmarkBlackholeBenchmarkIT {
    @ParameterizedTest
    @CsvSource({"q0,hashmap", "q0,rocksdb", "q1,hashmap", "q1,rocksdb", "q2,hashmap", "q2,rocksdb", "q3,hashmap"})
    void admittedQueriesExecuteThroughFlinksBlackhole(String query, String backend) throws Exception {
        NexmarkBlackholeBenchmark.main(new String[] {"10000", query, "flink", backend, "4"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        NexmarkBlackholeBenchmark.main(new String[] {"10000", query, "streamfusion", backend, "4"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }

    @Test
    void admittedInMemoryQ3MatchesFlinksCompleteCollectedChangelog() throws Exception {
        var flink = LocalRowDataNexmarkBenchmark.run(10_000, "q3", false, "hashmap", 4);
        var nativeResult = LocalRowDataNexmarkBenchmark.run(10_000, "q3", true, "hashmap", 4);
        assertThat(nativeResult.completed()).isTrue();
        assertThat(nativeResult.nativePlanBatches()).isPositive();
        assertThat(nativeResult.outputRows()).isPositive().isEqualTo(flink.outputRows());
        assertThat(nativeResult.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(nativeResult.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void rocksDbJoinReportsRemainingBackendRequirementAndMeasurementRejectsFallback() throws Exception {
        NexmarkBlackholeBenchmark.main(new String[] {"100", "q3", "streamfusion", "rocksdb", "4", "explain"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("default cache/write-buffer ratios");
        assertThatThrownBy(() ->
                        NexmarkBlackholeBenchmark.main(new String[] {"100", "q3", "streamfusion", "rocksdb", "4"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Refusing to measure a StreamFusion fallback");
    }
}
