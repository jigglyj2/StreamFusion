package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class NexmarkBenchmarkTest {
    @Test
    void definesTheFourNorthStarCases() {
        assertThat(BenchmarkCase.ALL)
                .extracting(BenchmarkCase::label)
                .containsExactly(
                        "memory / mini-batch off", "memory / mini-batch on",
                        "rocksdb / mini-batch off", "rocksdb / mini-batch on");
    }

    @Test
    void everyInvocationRequiresExactlyOnceKafkaInputAndOutput() {
        NexmarkCaseExecutor executor =
                new NexmarkCaseExecutor(Path.of("runner"), Path.of("/data/nexmark"), "kafka:9092", "q0,q1");
        List<String> command = executor.command("streamfusion", BenchmarkCase.ALL.get(0));
        assertThat(command)
                .containsSubsequence("--checkpointing-mode", "EXACTLY_ONCE")
                .containsSubsequence("--source", "kafka")
                .containsSubsequence("--sink", "kafka")
                .containsSubsequence("--state-backend", "hashmap")
                .containsSubsequence("--mini-batch", "false");
    }

    @Test
    void rendersComparisonTableAndChart() {
        String report = BenchmarkReport.render(List.of(
                new BenchmarkResult(BenchmarkCase.ALL.get(0), 100, 125),
                new BenchmarkResult(BenchmarkCase.ALL.get(1), 200, 300),
                new BenchmarkResult(BenchmarkCase.ALL.get(2), 80, 80),
                new BenchmarkResult(BenchmarkCase.ALL.get(3), 90, 135)));
        assertThat(report)
                .contains("| memory / mini-batch off | 100 | 125 | 1.25x |")
                .contains("xychart-beta")
                .contains("bar [100, 200, 80, 90]")
                .contains("bar [125, 300, 80, 135]");
    }
}
