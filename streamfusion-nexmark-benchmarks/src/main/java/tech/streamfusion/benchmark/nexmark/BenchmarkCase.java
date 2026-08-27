package tech.streamfusion.benchmark.nexmark;

import java.util.List;

public final class BenchmarkCase {
    public static final List<BenchmarkCase> ALL = List.of(
            new BenchmarkCase("memory / mini-batch off", "hashmap", false),
            new BenchmarkCase("memory / mini-batch on", "hashmap", true),
            new BenchmarkCase("rocksdb / mini-batch off", "rocksdb", false),
            new BenchmarkCase("rocksdb / mini-batch on", "rocksdb", true));

    private final String label;
    private final String backend;
    private final boolean miniBatch;

    private BenchmarkCase(String label, String backend, boolean miniBatch) {
        this.label = label;
        this.backend = backend;
        this.miniBatch = miniBatch;
    }

    public String label() {
        return label;
    }

    public String backend() {
        return backend;
    }

    public boolean miniBatch() {
        return miniBatch;
    }
}
