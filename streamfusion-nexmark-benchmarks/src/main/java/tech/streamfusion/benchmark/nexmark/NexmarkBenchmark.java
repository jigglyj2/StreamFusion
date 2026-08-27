package tech.streamfusion.benchmark.nexmark;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class NexmarkBenchmark {
    private NexmarkBenchmark() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> options = parse(args);
        Path output = Path.of(options.getOrDefault("output", "benchmarks.txt"));
        NexmarkCaseExecutor executor = new NexmarkCaseExecutor(
                requiredPath(options, "executor"),
                Path.of(options.getOrDefault("nexmark-home", System.getProperty("user.home") + "/data/nexmark")),
                required(options, "kafka-bootstrap"),
                options.getOrDefault("queries", "q0,q1"));

        List<BenchmarkResult> results = new ArrayList<>();
        for (BenchmarkCase benchmarkCase : BenchmarkCase.ALL) {
            double flink = executor.run("flink", benchmarkCase);
            double streamFusion = executor.run("streamfusion", benchmarkCase);
            results.add(new BenchmarkResult(benchmarkCase, flink, streamFusion));
        }
        String report = BenchmarkReport.render(results);
        System.out.print(report);
        Files.writeString(output, report, StandardCharsets.UTF_8);
    }

    private static Map<String, String> parse(String[] args) {
        if (args.length % 2 != 0) throw new IllegalArgumentException("Arguments must be --name value pairs");
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("Expected option, got " + args[i]);
            result.put(args[i].substring(2), args[i + 1]);
        }
        return result;
    }

    private static String required(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing --" + name);
        return value;
    }

    private static Path requiredPath(Map<String, String> options, String name) {
        return Path.of(required(options, name));
    }
}
