package tech.streamfusion.benchmark.nexmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class NexmarkCaseExecutor {
    private static final Pattern THROUGHPUT =
            Pattern.compile("NEXMARK_THROUGHPUT records_per_second=([0-9]+(?:\\.[0-9]+)?)");

    private final Path executable;
    private final Path nexmarkHome;
    private final String bootstrapServers;
    private final String queries;

    NexmarkCaseExecutor(Path executable, Path nexmarkHome, String bootstrapServers, String queries) {
        this.executable = executable;
        this.nexmarkHome = nexmarkHome;
        this.bootstrapServers = bootstrapServers;
        this.queries = queries;
    }

    double run(String engine, BenchmarkCase benchmarkCase) throws IOException, InterruptedException {
        List<String> command = command(engine, benchmarkCase);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = process.waitFor();
        if (exit != 0) throw new IOException("Nexmark executor failed (" + exit + "):\n" + output);
        Matcher matcher = THROUGHPUT.matcher(output);
        if (!matcher.find()) throw new IOException("Executor did not print a NEXMARK_THROUGHPUT marker:\n" + output);
        return Double.parseDouble(matcher.group(1));
    }

    List<String> command(String engine, BenchmarkCase benchmarkCase) {
        List<String> command = new ArrayList<>(List.of(
                executable.toString(),
                "--engine",
                engine,
                "--state-backend",
                benchmarkCase.backend(),
                "--mini-batch",
                Boolean.toString(benchmarkCase.miniBatch()),
                "--checkpointing-mode",
                "EXACTLY_ONCE",
                "--source",
                "kafka",
                "--sink",
                "kafka",
                "--kafka-bootstrap",
                bootstrapServers,
                "--nexmark-home",
                nexmarkHome.toString(),
                "--queries",
                queries));
        return command;
    }
}
