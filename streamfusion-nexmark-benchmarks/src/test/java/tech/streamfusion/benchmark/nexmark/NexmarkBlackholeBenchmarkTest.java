package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NexmarkBlackholeBenchmarkTest {
    @Test
    void preservesTheResultSchemaWhileUsingFlinksBlackhole() throws Exception {
        for (String query : NexmarkRowDataQueryCatalog.supportedQueries()) {
            String blackhole = NexmarkRowDataJob.blackholeSinkDdl(query);
            String collecting = NexmarkRowDataJob.sinkDdl(query);
            assertThat(blackhole.substring(0, blackhole.indexOf(") WITH")))
                    .as(query)
                    .isEqualTo(collecting.substring(0, collecting.indexOf(") WITH")));
            assertThat(blackhole).endsWith(") WITH ('connector'='blackhole')").doesNotContain("run-id");
        }
    }

    @Test
    void refusesCombinedJvmMeasurementsAndUnknownModes() {
        for (String[] args : new String[][] {
            {"100", "q0", "both", "hashmap"},
            {"100", "q0", "flink", "both"},
            {"100", "q0", "flink", "hashmap", "4", "unknown"}
        }) assertThatThrownBy(() -> NexmarkBlackholeBenchmark.main(args)).isInstanceOf(IllegalArgumentException.class);
    }
}
