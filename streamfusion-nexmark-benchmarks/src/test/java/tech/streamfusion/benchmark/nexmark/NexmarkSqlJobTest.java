package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NexmarkSqlJobTest {
    @Test
    void loadsTheCheckedInNexmarkQuery() throws Exception {
        assertThat(NexmarkSqlJob.loadQuery("q0"))
                .contains("INSERT INTO nexmark_output")
                .contains("FROM bid");
    }
}
