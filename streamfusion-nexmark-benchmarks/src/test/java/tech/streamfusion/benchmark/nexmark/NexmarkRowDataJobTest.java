package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NexmarkRowDataJobTest {
    @Test
    void configuresTheLocalNexmarkRowDataConnector() {
        String ddl = NexmarkRowDataJob.sourceDdl(100);

        assertThat(ddl)
                .contains("'connector'='streamfusion-nexmark-bounded'")
                .contains("'events.num'='100'")
                .contains("'max-emit-speed'='true'")
                .contains("WATERMARK FOR event_time");
    }

    @Test
    void createsResultSinksForEveryAccelerableQuery() throws Exception {
        for (String query : NexmarkRowDataQueryCatalog.supportedQueries()) {
            assertThat(NexmarkRowDataJob.sinkDdl(query))
                    .as(query)
                    .contains("CREATE TABLE nexmark_output")
                    .contains("'connector'='streamfusion-benchmark-result'")
                    .contains("'run-id'='test-run'");
        }
        assertThat(NexmarkRowDataJob.sinkDdl("q1")).contains("price DECIMAL(23, 3)");
    }

    @Test
    void rejectsQueriesWithoutCompleteNativeCoverage() {
        assertThatThrownBy(() -> NexmarkRowDataJob.sinkDdl("q3"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("not fully accelerable");
    }
}
