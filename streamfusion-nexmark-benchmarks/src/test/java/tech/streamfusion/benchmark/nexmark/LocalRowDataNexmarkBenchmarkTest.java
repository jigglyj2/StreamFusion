package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class LocalRowDataNexmarkBenchmarkTest {
    @Test
    void selectsOneEngineOrTheFairBaselineOrder() {
        assertThat(LocalRowDataNexmarkBenchmark.engines("flink")).containsExactly(false);
        assertThat(LocalRowDataNexmarkBenchmark.engines("streamfusion")).containsExactly(true);
        assertThat(LocalRowDataNexmarkBenchmark.engines("both")).containsExactly(false, true);
    }

    @Test
    void rejectsUnknownEngines() {
        assertThatThrownBy(() -> LocalRowDataNexmarkBenchmark.engines("other"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flink, streamfusion, or both");
    }

    @Test
    void selectsMemoryRocksOrBothStateBackends() {
        assertThat(LocalRowDataNexmarkBenchmark.backends("memory")).containsExactly("hashmap");
        assertThat(LocalRowDataNexmarkBenchmark.backends("rocksdb")).containsExactly("rocksdb");
        assertThat(LocalRowDataNexmarkBenchmark.backends("both")).containsExactly("hashmap", "rocksdb");
    }

    @Test
    void rejectsUnknownStateBackend() {
        assertThatThrownBy(() -> LocalRowDataNexmarkBenchmark.backends("other"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("memory, hashmap, rocksdb, or both");
    }
}
