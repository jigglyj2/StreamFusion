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
        for (String query : queries) {
            for (boolean streamFusion : engines) {
                RunResult result = run(events, query, streamFusion);
                System.out.printf(
                        "%s engine=%s input_events=%d elapsed_seconds=%.6f input_events_per_second=%.2f native_calc_batches=%d%n",
                        query,
                        streamFusion ? "streamfusion" : "flink",
                        events,
                        result.elapsedSeconds(),
                        result.recordsPerSecond(events),
                        result.nativeCalcBatches());
            }
        }
    }

    static RunResult run(long events, String query, boolean streamFusion) throws Exception {
        StreamFusionPlannerFactory.resetMetrics();
        long start = System.nanoTime();
        NexmarkRowDataJob.run(events, query, streamFusion);
        return new RunResult(System.nanoTime() - start, StreamFusionPlannerFactory.nativeCalcBatchCount());
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

    static final class RunResult {
        private final long elapsedNanos;
        private final long nativeCalcBatches;

        private RunResult(long elapsedNanos, long nativeCalcBatches) {
            this.elapsedNanos = elapsedNanos;
            this.nativeCalcBatches = nativeCalcBatches;
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

        boolean completed() {
            return elapsedNanos > 0;
        }
    }
}
