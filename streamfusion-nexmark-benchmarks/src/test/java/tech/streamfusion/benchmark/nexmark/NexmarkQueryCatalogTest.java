package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NexmarkQueryCatalogTest {
    @Test
    void containsEveryFlinkSupportedQuery() throws Exception {
        assertThat(NexmarkQueryCatalog.supportedQueries()).hasSize(23).doesNotContain("q6");
        for (String query : NexmarkQueryCatalog.supportedQueries()) {
            assertThat(NexmarkQueryCatalog.load(query))
                    .as(query)
                    .contains("CREATE TABLE nexmark_" + query)
                    .contains("INSERT INTO nexmark_" + query);
        }
    }

    @Test
    void rejectsFlinkUnsupportedQuerySix() {
        assertThatThrownBy(() -> NexmarkQueryCatalog.load("q6"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("Unsupported Nexmark query");
    }
}
