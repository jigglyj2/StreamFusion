package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Opt-in source-to-sink admission checks; collecting-sink tests supply the output oracle. */
class NexmarkBlackholeBenchmarkIT {
    @ParameterizedTest
    @CsvSource({
        "q0,hashmap",
        "q0,rocksdb",
        "q1,hashmap",
        "q1,rocksdb",
        "q2,hashmap",
        "q2,rocksdb",
        "q3,hashmap",
        "q3,rocksdb"
    })
    void admittedQueriesExecuteThroughFlinksBlackhole(String query, String backend) throws Exception {
        NexmarkBlackholeBenchmark.main(new String[] {"10000", query, "flink", backend, "4"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        NexmarkBlackholeBenchmark.main(new String[] {"10000", query, "streamfusion", backend, "4"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void admittedQ3MatchesFlinksCompleteCollectedChangelog(String backend) throws Exception {
        var flink = LocalRowDataNexmarkBenchmark.run(10_000, "q3", false, backend, 4);
        var nativeResult = LocalRowDataNexmarkBenchmark.run(10_000, "q3", true, backend, 4);
        assertThat(nativeResult.completed()).isTrue();
        assertThat(nativeResult.nativePlanBatches()).isPositive();
        assertThat(nativeResult.outputRows()).isPositive().isEqualTo(flink.outputRows());
        assertThat(nativeResult.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(nativeResult.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void unadmittedQueryIsExplainedAndMeasurementRejectsFallback() throws Exception {
        NexmarkBlackholeBenchmark.main(new String[] {"100", "q4", "streamfusion", "rocksdb", "4", "explain"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: no");
        assertThatThrownBy(() ->
                        NexmarkBlackholeBenchmark.main(new String[] {"100", "q4", "streamfusion", "rocksdb", "4"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Refusing to measure a StreamFusion fallback");
    }
}
