package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NexmarkRowDataQueryCatalogTest {
    @Test
    void containsOnlyQueriesWithCompleteNativeOperatorCoverage() throws Exception {
        assertThat(NexmarkRowDataQueryCatalog.supportedQueries())
                .containsExactly(
                        "q0",
                        "q1",
                        "q2",
                        "q3",
                        "q8",
                        "q11",
                        "q12",
                        "q22",
                        "group-aggregate",
                        "select-distinct",
                        "top-n",
                        "limit");
        for (String query : NexmarkRowDataQueryCatalog.supportedQueries()) {
            assertThat(NexmarkRowDataQueryCatalog.load(query))
                    .contains("SELECT")
                    .doesNotContain("INSERT INTO");
        }
    }

    @Test
    void rejectsAQueryWhoseOperatorTreeHasNoNativeImplementation() {
        assertThatThrownBy(() -> NexmarkRowDataQueryCatalog.load("q5"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("not fully accelerable");
    }
}
