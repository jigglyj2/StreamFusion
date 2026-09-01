package tech.streamfusion.benchmark.nexmark;

import java.util.Arrays;
import java.util.List;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Runs fully accelerable Nexmark queries with checkpointed RowData inputs and RowData outputs. */
public final class LocalRowDataNexmarkBenchmark {
    private LocalRowDataNexmarkBenchmark() {}

    public static void main(String[] args) throws Exception {
        long events = args.length == 0 ? 100_000 : Long.parseLong(args[0]);
        List<String> queries =
                args.length < 2 ? NexmarkRowDataQueryCatalog.supportedQueries() : Arrays.asList(args[1].split(","));
        List<Boolean> engines = args.length < 3 ? List.of(false, true) : engines(args[2]);
        List<String> backends = args.length < 4 ? List.of("hashmap") : backends(args[3]);
        for (String query : queries) {
            for (boolean streamFusion : engines) {
                for (String backend : backends) {
                    RunResult result = run(events, query, streamFusion, backend);
                    System.out.printf(
                            "%s engine=%s state_backend=%s input_events=%d elapsed_seconds=%.6f input_events_per_second=%.2f native_calc_batches=%d native_group_aggregate_batches=%d native_window_aggregate_batches=%d output_rows=%d output_sha256=%s%n",
                            query,
                            streamFusion ? "streamfusion" : "flink",
                            backend,
                            events,
                            result.elapsedSeconds(),
                            result.recordsPerSecond(events),
                            result.nativeCalcBatches(),
                            result.nativeGroupAggregateBatches(),
                            result.nativeWindowAggregateBatches(),
                            result.outputRows(),
                            result.outputSha256());
                }
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
                StreamFusionPlannerFactory.nativeGroupAggregateBatchCount(),
                StreamFusionPlannerFactory.nativeWindowAggregateBatchCount(),
                output.rowCount(),
                output.sha256(),
                output.debugRows());
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
        private final long nativeGroupAggregateBatches;
        private final long nativeWindowAggregateBatches;
        private final long outputRows;
        private final String outputSha256;
        private final List<String> debugRows;

        private RunResult(
                long elapsedNanos,
                long nativeCalcBatches,
                long nativeGroupAggregateBatches,
                long nativeWindowAggregateBatches,
                long outputRows,
                String outputSha256,
                List<String> debugRows) {
            this.elapsedNanos = elapsedNanos;
            this.nativeCalcBatches = nativeCalcBatches;
            this.nativeGroupAggregateBatches = nativeGroupAggregateBatches;
            this.nativeWindowAggregateBatches = nativeWindowAggregateBatches;
            this.outputRows = outputRows;
            this.outputSha256 = outputSha256;
            this.debugRows = List.copyOf(debugRows);
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

        long nativeGroupAggregateBatches() {
            return nativeGroupAggregateBatches;
        }

        long nativeWindowAggregateBatches() {
            return nativeWindowAggregateBatches;
        }

        long outputRows() {
            return outputRows;
        }

        String outputSha256() {
            return outputSha256;
        }

        List<String> debugRows() {
            return debugRows;
        }

        boolean completed() {
            return elapsedNanos > 0;
        }
    }
}
