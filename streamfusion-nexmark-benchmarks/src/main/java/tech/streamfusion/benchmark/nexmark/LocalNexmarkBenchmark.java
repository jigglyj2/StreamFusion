package tech.streamfusion.benchmark.nexmark;

import java.time.Duration;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public final class LocalNexmarkBenchmark {
    private LocalNexmarkBenchmark() {}

    public static void main(String[] args) throws Exception {
        long events = args.length == 0 ? 100_000 : Long.parseLong(args[0]);
        String query = args.length < 2 ? "q0" : args[1];
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"))) {
            kafka.start();
            String bootstrap = kafka.getBootstrapServers();
            String input = "nexmark-input";
            String flinkOutput = "nexmark-flink-output";
            String streamFusionOutput = "nexmark-streamfusion-output";
            KafkaBenchmarkTopics.create(bootstrap, input, flinkOutput, streamFusionOutput);
            long expectedRows = NexmarkEventPublisher.publish(bootstrap, input, events);

            double flink = run(bootstrap, input, flinkOutput, query, events, expectedRows, false);
            double streamFusion = run(bootstrap, input, streamFusionOutput, query, events, expectedRows, true);
            System.out.println(BenchmarkReport.render(
                    java.util.List.of(new BenchmarkResult(BenchmarkCase.ALL.get(0), flink, streamFusion))));
        }
    }

    private static double run(
            String bootstrap,
            String input,
            String output,
            String query,
            long inputRows,
            long expectedOutputRows,
            boolean streamFusion)
            throws Exception {
        long start = System.nanoTime();
        NexmarkSqlJob.run(bootstrap, input, output, query, streamFusion);
        long count = KafkaBenchmarkTopics.countCommitted(bootstrap, output, expectedOutputRows, Duration.ofSeconds(30));
        if (count != expectedOutputRows) {
            throw new IllegalStateException("Expected " + expectedOutputRows + " committed rows, got " + count);
        }
        return inputRows / ((System.nanoTime() - start) / 1_000_000_000.0);
    }
}
