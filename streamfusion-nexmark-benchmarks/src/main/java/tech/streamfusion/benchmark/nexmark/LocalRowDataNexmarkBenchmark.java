package tech.streamfusion.benchmark.nexmark;

import java.util.Arrays;
import java.util.List;

/** Runs fully accelerable Nexmark queries with checkpointed RowData inputs and RowData outputs. */
public final class LocalRowDataNexmarkBenchmark {
    private LocalRowDataNexmarkBenchmark() {}

    public static void main(String[] args) throws Exception {
        long events = args.length == 0 ? 100_000 : Long.parseLong(args[0]);
        List<String> queries =
                args.length < 2 ? NexmarkRowDataQueryCatalog.supportedQueries() : Arrays.asList(args[1].split(","));
        for (String query : queries) {
            RunResult flink = run(events, query, false);
            RunResult streamFusion = run(events, query, true);
            System.out.printf(
                    "%s flink=%.2f records/s streamfusion=%.2f records/s%n",
                    query, flink.recordsPerSecond(events), streamFusion.recordsPerSecond(events));
        }
    }

    static RunResult run(long events, String query, boolean streamFusion) throws Exception {
        long start = System.nanoTime();
        NexmarkRowDataJob.run(events, query, streamFusion);
        return new RunResult(System.nanoTime() - start);
    }

    static final class RunResult {
        private final long elapsedNanos;

        private RunResult(long elapsedNanos) {
            this.elapsedNanos = elapsedNanos;
        }

        private double recordsPerSecond(long inputRows) {
            return inputRows / (elapsedNanos / 1_000_000_000.0);
        }

        boolean completed() {
            return elapsedNanos > 0;
        }
    }
}
