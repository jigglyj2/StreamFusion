package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NexmarkRowDataQueryCatalogTest {
    @Test
    void containsOnlyQueriesWithCompleteNativeOperatorCoverage() throws Exception {
        assertThat(NexmarkRowDataQueryCatalog.supportedQueries()).containsExactly("q0", "q1", "q2", "q22");
        for (String query : NexmarkRowDataQueryCatalog.supportedQueries()) {
            assertThat(NexmarkRowDataQueryCatalog.load(query))
                    .contains("SELECT")
                    .doesNotContain("INSERT INTO");
        }
    }

    @Test
    void rejectsAQueryWhoseJoinHasNoNativeImplementation() {
        assertThatThrownBy(() -> NexmarkRowDataQueryCatalog.load("q3"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("not fully accelerable");
    }
}
