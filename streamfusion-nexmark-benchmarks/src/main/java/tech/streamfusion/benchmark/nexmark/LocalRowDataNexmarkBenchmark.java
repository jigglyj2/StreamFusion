package tech.streamfusion.benchmark.nexmark;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import jdk.jfr.Configuration;
import jdk.jfr.Recording;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;

/** Runs fully accelerable Nexmark queries with checkpointed RowData inputs and RowData outputs. */
public final class LocalRowDataNexmarkBenchmark {
    private LocalRowDataNexmarkBenchmark() {}

    public static void main(String[] args) throws Exception {
        long events = args.length == 0 ? 100_000 : Long.parseLong(args[0]);
        List<String> queries =
                args.length < 2 ? NexmarkRowDataQueryCatalog.supportedQueries() : Arrays.asList(args[1].split(","));
        List<Boolean> engines = args.length < 3 ? List.of(false, true) : engines(args[2]);
        List<String> backends = args.length < 4 ? List.of("hashmap") : backends(args[3]);
        int parallelism = args.length < 5 ? NexmarkRowDataJob.PARALLELISM : Integer.parseInt(args[4]);
        String recordingPath = System.getProperty("streamfusion.nexmark.jfr");
        try (Recording recording =
                recordingPath == null ? null : new Recording(Configuration.getConfiguration("profile"))) {
            if (recording != null) {
                recording.start();
            }
            for (String query : queries) {
                for (boolean streamFusion : engines) {
                    for (String backend : backends) {
                        RunResult result = run(events, query, streamFusion, backend, parallelism);
                        String explain = streamFusion ? StreamFusionPlanningDiagnostics.explain() : "";
                        System.out.printf(
                                "%s engine=%s state_backend=%s accelerated=%s input_events=%d elapsed_seconds=%.6f input_events_per_second=%.2f native_calc_batches=%d native_deduplicate_batches=%d native_group_aggregate_batches=%d native_top_n_batches=%d native_window_aggregate_batches=%d native_window_join_batches=%d native_regular_join_batches=%d native_interval_join_batches=%d native_temporal_join_batches=%d native_over_aggregate_batches=%d native_temporal_sort_batches=%d output_rows=%d output_sha256=%s ordered_sha256=%s materialized_rows=%d materialized_sha256=%s%n",
                                query,
                                streamFusion ? "streamfusion" : "flink",
                                backend,
                                streamFusion && explain.contains("Accelerated: yes"),
                                events,
                                result.elapsedSeconds(),
                                result.recordsPerSecond(events),
                                result.nativeCalcBatches(),
                                result.nativeDeduplicateBatches(),
                                result.nativeGroupAggregateBatches(),
                                result.nativeTopNBatches(),
                                result.nativeWindowAggregateBatches(),
                                result.nativeWindowJoinBatches(),
                                result.nativeRegularJoinBatches(),
                                result.nativeIntervalJoinBatches(),
                                result.nativeTemporalJoinBatches(),
                                result.nativeOverAggregateBatches(),
                                result.nativeTemporalSortBatches(),
                                result.outputRows(),
                                result.outputSha256(),
                                result.orderedSha256(),
                                result.materializedRows(),
                                result.materializedSha256());
                        if (streamFusion && !explain.contains("Accelerated: yes")) {
                            System.out.println("NEXMARK_EXPLAIN " + explain.replace('\n', ' '));
                        }
                        if (Boolean.getBoolean("streamfusion.nexmark.debug-rows")) {
                            result.debugRows().forEach(row -> System.out.println("NEXMARK_ROW " + row));
                            result.materializedDebugRows()
                                    .forEach(row -> System.out.println("NEXMARK_MATERIALIZED_ROW " + row));
                        }
                    }
                }
            }
            if (recording != null) {
                recording.stop();
                recording.dump(Path.of(recordingPath));
            }
        }
    }

    static RunResult run(long events, String query, boolean streamFusion) throws Exception {
        return run(events, query, streamFusion, "hashmap");
    }

    static RunResult run(long events, String query, boolean streamFusion, String backend) throws Exception {
        return run(events, query, streamFusion, backend, NexmarkRowDataJob.PARALLELISM);
    }

    static RunResult run(long events, String query, boolean streamFusion, String backend, int parallelism)
            throws Exception {
        StreamFusionPlannerFactory.resetMetrics();
        long start = System.nanoTime();
        BenchmarkResultStore.Result output = NexmarkRowDataJob.run(events, query, streamFusion, backend, parallelism);
        return new RunResult(
                System.nanoTime() - start,
                StreamFusionPlannerFactory.nativeCalcBatchCount(),
                StreamFusionPlannerFactory.nativeDeduplicateBatchCount(),
                StreamFusionPlannerFactory.nativeGroupAggregateBatchCount(),
                StreamFusionPlannerFactory.nativeTopNBatchCount(),
                StreamFusionPlannerFactory.nativeWindowAggregateBatchCount(),
                StreamFusionPlannerFactory.nativeWindowJoinBatchCount(),
                StreamFusionPlannerFactory.nativeRegularJoinBatchCount(),
                StreamFusionPlannerFactory.nativeIntervalJoinBatchCount(),
                StreamFusionPlannerFactory.nativeTemporalJoinBatchCount(),
                StreamFusionPlannerFactory.nativeOverAggregateBatchCount(),
                StreamFusionPlannerFactory.nativeTemporalSortBatchCount(),
                output.rowCount(),
                output.sha256(),
                output.orderedSha256(),
                output.debugRows(),
                output.materializedRowCount(),
                output.materializedSha256(),
                output.materializedDebugRows());
    }

    static List<Boolean> engines(String engine) {
        switch (engine) {
            case "flink":
                return List.of(false);
            case "streamfusion":
                return List.of(true);
            case "both":
                return List.of(false, true);
            default:
                throw new IllegalArgumentException("engine must be flink, streamfusion, or both: " + engine);
        }
    }

    static List<String> backends(String backend) {
        switch (backend) {
            case "memory":
            case "hashmap":
                return List.of("hashmap");
            case "rocksdb":
                return List.of("rocksdb");
            case "both":
                return List.of("hashmap", "rocksdb");
            default:
                throw new IllegalArgumentException("backend must be memory, hashmap, rocksdb, or both: " + backend);
        }
    }

    static final class RunResult {
        private final long elapsedNanos;
        private final long nativeCalcBatches;
        private final long nativeDeduplicateBatches;
        private final long nativeGroupAggregateBatches;
        private final long nativeTopNBatches;
        private final long nativeWindowAggregateBatches;
        private final long nativeWindowJoinBatches;
        private final long nativeRegularJoinBatches;
        private final long nativeIntervalJoinBatches;
        private final long nativeTemporalJoinBatches;
        private final long nativeOverAggregateBatches;
        private final long nativeTemporalSortBatches;
        private final long outputRows;
        private final String outputSha256;
        private final String orderedSha256;
        private final List<String> debugRows;
        private final long materializedRows;
        private final String materializedSha256;
        private final List<String> materializedDebugRows;

        private RunResult(
                long elapsedNanos,
                long nativeCalcBatches,
                long nativeDeduplicateBatches,
                long nativeGroupAggregateBatches,
                long nativeTopNBatches,
                long nativeWindowAggregateBatches,
                long nativeWindowJoinBatches,
                long nativeRegularJoinBatches,
                long nativeIntervalJoinBatches,
                long nativeTemporalJoinBatches,
                long nativeOverAggregateBatches,
                long nativeTemporalSortBatches,
                long outputRows,
                String outputSha256,
                String orderedSha256,
                List<String> debugRows,
                long materializedRows,
                String materializedSha256,
                List<String> materializedDebugRows) {
            this.elapsedNanos = elapsedNanos;
            this.nativeCalcBatches = nativeCalcBatches;
            this.nativeDeduplicateBatches = nativeDeduplicateBatches;
            this.nativeGroupAggregateBatches = nativeGroupAggregateBatches;
            this.nativeTopNBatches = nativeTopNBatches;
            this.nativeWindowAggregateBatches = nativeWindowAggregateBatches;
            this.nativeWindowJoinBatches = nativeWindowJoinBatches;
            this.nativeRegularJoinBatches = nativeRegularJoinBatches;
            this.nativeIntervalJoinBatches = nativeIntervalJoinBatches;
            this.nativeTemporalJoinBatches = nativeTemporalJoinBatches;
            this.nativeOverAggregateBatches = nativeOverAggregateBatches;
            this.nativeTemporalSortBatches = nativeTemporalSortBatches;
            this.outputRows = outputRows;
            this.outputSha256 = outputSha256;
            this.orderedSha256 = orderedSha256;
            this.debugRows = List.copyOf(debugRows);
            this.materializedRows = materializedRows;
            this.materializedSha256 = materializedSha256;
            this.materializedDebugRows = List.copyOf(materializedDebugRows);
        }

        private double recordsPerSecond(long inputRows) {
            return inputRows / (elapsedNanos / 1_000_000_000.0);
        }

        private double elapsedSeconds() {
            return elapsedNanos / 1_000_000_000.0;
        }

        private long nativeCalcBatches() {
            return nativeCalcBatches;
        }

        long nativeDeduplicateBatches() {
            return nativeDeduplicateBatches;
        }

        long nativeGroupAggregateBatches() {
            return nativeGroupAggregateBatches;
        }

        long nativeTopNBatches() {
            return nativeTopNBatches;
        }

        long nativeWindowAggregateBatches() {
            return nativeWindowAggregateBatches;
        }

        long nativeWindowJoinBatches() {
            return nativeWindowJoinBatches;
        }

        long nativeRegularJoinBatches() {
            return nativeRegularJoinBatches;
        }

        long nativeIntervalJoinBatches() {
            return nativeIntervalJoinBatches;
        }

        long nativeTemporalJoinBatches() {
            return nativeTemporalJoinBatches;
        }

        long nativeOverAggregateBatches() {
            return nativeOverAggregateBatches;
        }

        long nativeTemporalSortBatches() {
            return nativeTemporalSortBatches;
        }

        long outputRows() {
            return outputRows;
        }

        String outputSha256() {
            return outputSha256;
        }

        String orderedSha256() {
            return orderedSha256;
        }

        List<String> debugRows() {
            return debugRows;
        }

        long materializedRows() {
            return materializedRows;
        }

        String materializedSha256() {
            return materializedSha256;
        }

        List<String> materializedDebugRows() {
            return materializedDebugRows;
        }

        boolean completed() {
            return elapsedNanos > 0;
        }
    }
}
