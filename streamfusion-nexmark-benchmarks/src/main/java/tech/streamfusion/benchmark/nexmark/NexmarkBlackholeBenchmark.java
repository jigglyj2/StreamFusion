package tech.streamfusion.benchmark.nexmark;

import java.util.Locale;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.planner.StreamFusionPlanningDiagnostics;
import tech.streamfusion.nativebridge.NativeExecutionDiagnostics;

/** One engine/query/backend per JVM, using Flink's unmodified blackhole connector. */
public final class NexmarkBlackholeBenchmark {
    private NexmarkBlackholeBenchmark() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 4 || args.length > 6) {
            throw new IllegalArgumentException(
                    "Usage: <events> <query> <flink|streamfusion> <hashmap|rocksdb> [parallelism] [run|explain]");
        }
        long events = Long.parseLong(args[0]);
        String query = args[1];
        var engines = LocalRowDataNexmarkBenchmark.engines(args[2]);
        var backends = LocalRowDataNexmarkBenchmark.backends(args[3]);
        if (engines.size() != 1 || backends.size() != 1) {
            throw new IllegalArgumentException("Use a separate JVM for each engine and backend");
        }
        int parallelism = args.length < 5 ? NexmarkRowDataJob.PARALLELISM : Integer.parseInt(args[4]);
        String mode = args.length < 6 ? "run" : args[5];
        if (!mode.equals("run") && !mode.equals("explain")) {
            throw new IllegalArgumentException("mode must be run or explain: " + mode);
        }
        boolean nativeEngine = engines.get(0);
        long started = System.nanoTime();
        NativeExecutionDiagnostics.reset();
        long outputRows = NexmarkRowDataJob.runBlackhole(
                events, query, nativeEngine, backends.get(0), parallelism, mode.equals("explain"));
        double elapsed = (System.nanoTime() - started) / 1_000_000_000.0;
        if (mode.equals("explain")) return;
        long batches = StreamFusionPlannerFactory.nativePlanBatchCount();
        if (nativeEngine && (!StreamFusionPlanningDiagnostics.explain().contains("Accelerated: yes") || batches == 0)) {
            throw new IllegalStateException("StreamFusion measurement requires acceleration and native plan activity");
        }
        System.out.printf(
                Locale.ROOT,
                "NEXMARK_BLACKHOLE query=%s engine=%s state_backend=%s parallelism=%d runtime_mode=%s mini_batch=%s "
                        + "timing=end_to_end input_events=%d output_records=%d elapsed_seconds=%.6f input_events_per_second=%.2f "
                        + "accelerated=%s native_plan_batches=%d native_calc_batches=%d%n",
                query,
                args[2],
                backends.get(0),
                parallelism,
                Boolean.getBoolean("streamfusion.nexmark.batch-mode") ? "batch" : "streaming",
                Boolean.getBoolean("streamfusion.nexmark.mini-batch"),
                events,
                outputRows,
                elapsed,
                events / elapsed,
                nativeEngine,
                batches,
                StreamFusionPlannerFactory.nativeCalcBatchCount());
    }
}
