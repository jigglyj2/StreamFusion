package tech.streamfusion.benchmark.nexmark;

public final class BenchmarkResult {
    private final BenchmarkCase benchmarkCase;
    private final double flink;
    private final double streamFusion;

    public BenchmarkResult(BenchmarkCase benchmarkCase, double flink, double streamFusion) {
        this.benchmarkCase = benchmarkCase;
        this.flink = flink;
        this.streamFusion = streamFusion;
    }

    public BenchmarkCase benchmarkCase() {
        return benchmarkCase;
    }

    public double flink() {
        return flink;
    }

    public double streamFusion() {
        return streamFusion;
    }
}
