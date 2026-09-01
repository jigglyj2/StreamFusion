package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

@ResourceLock("streamfusion-planner-property")
class LocalRowDataNexmarkBenchmarkIT {
    @BeforeAll
    static void requireLocalNexmarkGenerator() {
        Assumptions.assumeTrue(
                isPresent("com.github.nexmark.flink.source.StreamFusionBoundedNexmarkTableSourceFactory"),
                "Enable -Prowdata-nexmark-integration with -Dnexmark.generator.jar=<path>");
    }

    @AfterEach
    void clearPlanner() {
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @ValueSource(strings = {"q0", "q1", "q2", "q22"})
    void runsWithFourCheckpointedRowDataPartitions(String query) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink = LocalRowDataNexmarkBenchmark.run(1_000, query, false);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion = LocalRowDataNexmarkBenchmark.run(1_000, query, true);

        assertThat(flink.completed()).isTrue();
        assertThat(streamFusion.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(StreamFusionPlannerFactory.nativeCalcBatchCount()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void runsNativeStatefulOperatorsOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(1_000, "group-aggregate", true, backend);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeGroupAggregateBatches()).isGreaterThan(0);

        streamFusion = LocalRowDataNexmarkBenchmark.run(1_000, "select-distinct", true, backend);
        assertThat(streamFusion.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeGroupAggregateBatches()).isGreaterThan(0);
    }

    private static boolean isPresent(String className) {
        try {
            Class.forName(className);
            return true;
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }
}
