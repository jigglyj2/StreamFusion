package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
        System.clearProperty("streamfusion.nexmark.batch-mode");
        StreamFusionPlannerFactory.resetMetrics();
    }

    @ParameterizedTest
    @ValueSource(strings = {"batch-unnest", "batch-window-tvf"})
    void boundedTableExpansionMatchesFlink(String query) throws Exception {
        System.setProperty("streamfusion.nexmark.batch-mode", "true");
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, query, false, "hashmap", 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, query, true, "hashmap", 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeCalcBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"q0", "q1", "q2", "q22"})
    void runsWithFourCheckpointedRowDataPartitions(String query) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink = LocalRowDataNexmarkBenchmark.run(1_000, query, false);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion = LocalRowDataNexmarkBenchmark.run(1_000, query, true);

        assertThat(flink.completed()).isTrue();
        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
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

        for (String query : List.of("aggregate-modifiers", "global-aggregate", "grouping-sets")) {
            LocalRowDataNexmarkBenchmark.RunResult flink =
                    LocalRowDataNexmarkBenchmark.run(1_000, query, false, backend, 1);
            streamFusion = LocalRowDataNexmarkBenchmark.run(1_000, query, true, backend, 1);
            assertThat(streamFusion.completed()).isTrue();
            assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
            assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
            assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(streamFusion.nativeGroupAggregateBatches()).isGreaterThan(0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void intersectAllMatchesMaterializedFlinkResultsOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "set-intersect-all", false, backend, 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "set-intersect-all", true, backend, 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.materializedDebugRows()).containsExactlyElementsOf(flink.materializedDebugRows());
        assertThat(streamFusion.materializedRows()).isEqualTo(flink.materializedRows());
        assertThat(streamFusion.materializedSha256()).isEqualTo(flink.materializedSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeGroupAggregateBatches()).isGreaterThan(0);
        assertThat(streamFusion.nativeCalcBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void everySynchronousDeduplicateModeMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        for (String query :
                List.of("q18", "deduplicate-processing-time-keep-first", "deduplicate-processing-time-keep-last")) {
            LocalRowDataNexmarkBenchmark.RunResult flink =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, false, backend, 1);
            LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, true, backend, 1);

            assertThat(streamFusion.completed()).as(query).isTrue();
            assertThat(streamFusion.debugRows()).as(query).containsExactlyElementsOf(flink.debugRows());
            assertThat(streamFusion.outputRows()).as(query).isEqualTo(flink.outputRows());
            assertThat(streamFusion.outputSha256()).as(query).isEqualTo(flink.outputSha256());
            assertThat(streamFusion.materializedDebugRows())
                    .as(query)
                    .containsExactlyElementsOf(flink.materializedDebugRows());
            assertThat(streamFusion.materializedRows()).as(query).isEqualTo(flink.materializedRows());
            assertThat(streamFusion.materializedSha256()).as(query).isEqualTo(flink.materializedSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).as(query).contains("Accelerated: yes");
            assertThat(streamFusion.nativeDeduplicateBatches()).as(query).isGreaterThan(0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void incrementalGroupAggregateMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        System.setProperty("streamfusion.nexmark.mini-batch", "true");
        System.setProperty("streamfusion.nexmark.mini-batch-size", "32");
        System.setProperty(
                "streamfusion.nexmark.checkpoint-interval-ms",
                Long.toString(Duration.ofDays(1).toMillis()));
        try {
            LocalRowDataNexmarkBenchmark.RunResult flink =
                    LocalRowDataNexmarkBenchmark.run(2_000, "incremental-group-aggregate", false, backend, 1);
            LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                    LocalRowDataNexmarkBenchmark.run(2_000, "incremental-group-aggregate", true, backend, 1);

            assertThat(streamFusion.completed()).isTrue();
            assertThat(streamFusion.materializedDebugRows()).containsExactlyElementsOf(flink.materializedDebugRows());
            assertThat(streamFusion.materializedRows()).isEqualTo(flink.materializedRows());
            assertThat(streamFusion.materializedSha256()).isEqualTo(flink.materializedSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
            assertThat(streamFusion.nativeGroupAggregateBatches()).isGreaterThan(0);
        } finally {
            System.clearProperty("streamfusion.nexmark.mini-batch");
            System.clearProperty("streamfusion.nexmark.mini-batch-size");
            System.clearProperty("streamfusion.nexmark.checkpoint-interval-ms");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void q11SessionWindowsMatchFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(1_000, "q11", false, backend, 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(1_000, "q11", true, backend, 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeWindowAggregateBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void legacyWindowAggregateMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "legacy-window-aggregate", false, backend, 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "legacy-window-aggregate", true, backend, 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeWindowAggregateBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void matchRecognizeMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "match-recognize", false, backend, 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "match-recognize", true, backend, 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeMatchRecognizeBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void q8WindowCompositionMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink = LocalRowDataNexmarkBenchmark.run(1_000, "q8", false, backend, 4);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(1_000, "q8", true, backend, 4);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        // Flink 2.3 may retain an attached WindowJoin or normalize the same equality shape to a
        // binary MultiJoin. StreamFusion lowers the latter through its regular-join kernel.
        assertThat(streamFusion.nativeWindowJoinBatches() + streamFusion.nativeRegularJoinBatches())
                .isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void q3RegularJoinMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink = LocalRowDataNexmarkBenchmark.run(1_000, "q3", false, backend, 4);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(1_000, "q3", true, backend, 4);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeRegularJoinBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void residualJoinNexmarkQueriesMatchFlinkOnBothStateBackends(String backend) throws Exception {
        for (String query : List.of("q4", "q9")) {
            LocalRowDataNexmarkBenchmark.RunResult flink =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, false, backend, 1);
            LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, true, backend, 1);

            assertThat(streamFusion.completed()).as(query).isTrue();
            assertThat(streamFusion.materializedDebugRows())
                    .as(query)
                    .containsExactlyElementsOf(flink.materializedDebugRows());
            assertThat(streamFusion.materializedRows()).as(query).isEqualTo(flink.materializedRows());
            assertThat(streamFusion.materializedSha256()).as(query).isEqualTo(flink.materializedSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).as(query).contains("Accelerated: yes");
            assertThat(streamFusion.nativeRegularJoinBatches()).as(query).isGreaterThan(0);
            if (query.equals("q4")) {
                assertThat(streamFusion.nativeGroupAggregateBatches()).as(query).isGreaterThan(0);
            } else {
                assertThat(streamFusion.nativeTopNBatches()).as(query).isGreaterThan(0);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void officialWindowCompositionQueriesMatchFlinkOnBothStateBackends(String backend) throws Exception {
        for (String query : List.of("q5", "q7")) {
            LocalRowDataNexmarkBenchmark.RunResult flink =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, false, backend, 1);
            LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, true, backend, 1);

            assertThat(streamFusion.completed()).as(query).isTrue();
            assertThat(streamFusion.debugRows()).as(query).containsExactlyElementsOf(flink.debugRows());
            assertThat(streamFusion.outputRows()).as(query).isEqualTo(flink.outputRows());
            assertThat(streamFusion.outputSha256()).as(query).isEqualTo(flink.outputSha256());
            assertThat(streamFusion.materializedDebugRows())
                    .as(query)
                    .containsExactlyElementsOf(flink.materializedDebugRows());
            assertThat(streamFusion.materializedSha256()).as(query).isEqualTo(flink.materializedSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).as(query).contains("Accelerated: yes");
            assertThat(streamFusion.nativeWindowAggregateBatches()).as(query).isGreaterThan(0);
            assertThat(streamFusion.nativeRegularJoinBatches()).as(query).isGreaterThan(0);
        }
    }

    @Test
    void q5NativeWindowStateFitsTheStandardManagedMemoryEnvelopeAtBenchmarkScale() throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(250_000, "q5", true, "hashmap", 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeWindowAggregateBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void intervalJoinQueriesMatchFlinkOnBothStateBackends(String backend) throws Exception {
        String query = "interval-join";
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, query, false, backend, 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, query, true, backend, 1);

        assertThat(streamFusion.completed()).as(query).isTrue();
        assertThat(streamFusion.debugRows()).as(query).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).as(query).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).as(query).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).as(query).contains("Accelerated: yes");
        assertThat(streamFusion.nativeIntervalJoinBatches()).as(query).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void overAggregateMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        for (String query : new String[] {
            "over-aggregate",
            "over-aggregate-event-time",
            "over-aggregate-processing-time",
            "over-aggregate-bounded-rows",
            "over-aggregate-bounded-range"
        }) {
            LocalRowDataNexmarkBenchmark.RunResult flink =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, false, backend, 1);
            LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, true, backend, 1);

            assertThat(streamFusion.completed()).as(query).isTrue();
            assertThat(streamFusion.debugRows()).as(query).containsExactlyElementsOf(flink.debugRows());
            assertThat(streamFusion.outputRows()).as(query).isEqualTo(flink.outputRows());
            assertThat(streamFusion.outputSha256()).as(query).isEqualTo(flink.outputSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).as(query).contains("Accelerated: yes");
            assertThat(streamFusion.nativeOverAggregateBatches()).as(query).isGreaterThan(0);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void composedRegularJoinsAndTopNMatchFlink(String backend) throws Exception {
        for (String query : new String[] {"q19", "q20"}) {
            int parallelism = query.equals("q19") ? 1 : 4;
            LocalRowDataNexmarkBenchmark.RunResult flink =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, false, backend, parallelism);
            LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                    LocalRowDataNexmarkBenchmark.run(2_000, query, true, backend, parallelism);

            assertThat(streamFusion.completed()).as(query).isTrue();
            assertThat(streamFusion.debugRows()).as(query).containsExactlyElementsOf(flink.debugRows());
            assertThat(streamFusion.outputRows()).as(query).isEqualTo(flink.outputRows());
            assertThat(streamFusion.outputSha256()).as(query).isEqualTo(flink.outputSha256());
            assertThat(StreamFusionPlanningDiagnostics.explain()).as(query).contains("Accelerated: yes");
            if (query.equals("q19")) {
                assertThat(streamFusion.nativeTopNBatches()).as(query).isGreaterThan(0);
            } else {
                assertThat(streamFusion.nativeRegularJoinBatches()).as(query).isGreaterThan(0);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void q23MultiJoinMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "q23", false, backend, 4);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "q23", true, backend, 4);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeMultiJoinBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void topNMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "top-n", false, backend, 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "top-n", true, backend, 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeTopNBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void limitMatchesFlinkOnBothStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "limit", false, backend, 1);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "limit", true, backend, 1);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeTopNBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void temporalSortMatchesFlinkIncludingGlobalOutputOrder(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "temporal-sort", false, backend, 4);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "temporal-sort", true, backend, 4);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(streamFusion.orderedSha256()).isEqualTo(flink.orderedSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeTemporalSortBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void boundedSortMatchesFlinkIncludingGlobalOutputOrder(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "bounded-sort", false, backend, 4);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "bounded-sort", true, backend, 4);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(streamFusion.orderedSha256()).isEqualTo(flink.orderedSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeBoundedSortBatches()).isGreaterThan(0);
    }

    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void temporalJoinMatchesFlinkOnBothNativeStateBackends(String backend) throws Exception {
        LocalRowDataNexmarkBenchmark.RunResult flink =
                LocalRowDataNexmarkBenchmark.run(2_000, "temporal-join", false, backend, 4);
        LocalRowDataNexmarkBenchmark.RunResult streamFusion =
                LocalRowDataNexmarkBenchmark.run(2_000, "temporal-join", true, backend, 4);

        assertThat(streamFusion.completed()).isTrue();
        assertThat(streamFusion.debugRows()).containsExactlyElementsOf(flink.debugRows());
        assertThat(streamFusion.outputRows()).isEqualTo(flink.outputRows());
        assertThat(streamFusion.outputSha256()).isEqualTo(flink.outputSha256());
        assertThat(streamFusion.materializedRows()).isEqualTo(flink.materializedRows());
        assertThat(streamFusion.materializedSha256()).isEqualTo(flink.materializedSha256());
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
        assertThat(streamFusion.nativeTemporalJoinBatches()).isGreaterThan(0);
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
