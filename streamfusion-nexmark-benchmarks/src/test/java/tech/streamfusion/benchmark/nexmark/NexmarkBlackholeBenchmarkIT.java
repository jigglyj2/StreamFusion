package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Opt-in source-to-sink admission checks; collecting-sink tests supply the output oracle. */
class NexmarkBlackholeBenchmarkIT {
    @ParameterizedTest
    @CsvSource({"q0,hashmap", "q0,rocksdb", "q1,hashmap", "q1,rocksdb", "q2,hashmap", "q2,rocksdb"})
    void admittedQueriesExecuteThroughFlinksBlackhole(String query, String backend) throws Exception {
        NexmarkBlackholeBenchmark.main(new String[] {"10000", query, "flink", backend, "4"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        NexmarkBlackholeBenchmark.main(new String[] {"10000", query, "streamfusion", backend, "4"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isPositive();
    }

    @Test
    void explainReportsTheNextCheckpointAndMeasurementRejectsFallback() throws Exception {
        NexmarkBlackholeBenchmark.main(new String[] {"100", "q3", "streamfusion", "hashmap", "4", "explain"});
        assertThat(StreamFusionPlannerFactory.nativePlanBatchCount()).isZero();
        assertThatThrownBy(() ->
                        NexmarkBlackholeBenchmark.main(new String[] {"100", "q3", "streamfusion", "hashmap", "4"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Refusing to measure a StreamFusion fallback");
    }
}
