package tech.streamfusion.benchmark.nexmark;

import java.util.List;
import java.util.Locale;

public final class BenchmarkReport {
    private BenchmarkReport() {}

    public static String render(List<BenchmarkResult> results) {
        StringBuilder out = new StringBuilder("# Nexmark throughput (records/second)\n\n")
                .append("| Case | Flink | StreamFusion | Speedup |\n")
                .append("|---|---:|---:|---:|\n");
        for (BenchmarkResult result : results) {
            out.append(String.format(
                    Locale.ROOT,
                    "| %s | %,.0f | %,.0f | %.2fx |%n",
                    result.benchmarkCase().label(),
                    result.flink(),
                    result.streamFusion(),
                    result.streamFusion() / result.flink()));
        }
        out.append("\n```mermaid\nxychart-beta\n    title \"Nexmark exactly-once Kafka throughput\"\n")
                .append("    x-axis [mem_off, mem_on, rocks_off, rocks_on]\n")
                .append("    y-axis \"records/second\" 0 --> ")
                .append((long) Math.ceil(results.stream()
                        .flatMapToDouble(r -> java.util.stream.DoubleStream.of(r.flink(), r.streamFusion()))
                        .max()
                        .orElse(1)))
                .append("\n    bar [");
        appendValues(out, results, false);
        out.append("]\n    bar [");
        appendValues(out, results, true);
        return out.append("]\n```\n\nBars are Flink, then StreamFusion.\n").toString();
    }

    private static void appendValues(StringBuilder out, List<BenchmarkResult> results, boolean streamFusion) {
        for (int i = 0; i < results.size(); i++) {
            if (i > 0) out.append(", ");
            out.append((long)
                    (streamFusion
                            ? results.get(i).streamFusion()
                            : results.get(i).flink()));
        }
    }
}
