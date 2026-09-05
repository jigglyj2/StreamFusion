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
                        "q4",
                        "q5",
                        "q7",
                        "q8",
                        "q9",
                        "q11",
                        "q12",
                        "q18",
                        "q19",
                        "q20",
                        "q22",
                        "q23",
                        "aggregate-modifiers",
                        "batch-unnest",
                        "batch-window-tvf",
                        "bounded-sort",
                        "bounded-sort-limit",
                        "bounded-limit",
                        "bounded-rank",
                        "incremental-group-aggregate",
                        "group-aggregate",
                        "global-aggregate",
                        "grouping-sets",
                        "legacy-window-aggregate",
                        "match-recognize",
                        "interval-join",
                        "temporal-join",
                        "over-aggregate",
                        "over-aggregate-event-time",
                        "over-aggregate-processing-time",
                        "over-aggregate-bounded-rows",
                        "over-aggregate-bounded-range",
                        "select-distinct",
                        "set-intersect-all",
                        "deduplicate-processing-time-keep-first",
                        "deduplicate-processing-time-keep-last",
                        "temporal-sort",
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
        assertThatThrownBy(() -> NexmarkRowDataQueryCatalog.load("q6"))
                .isInstanceOf(java.io.IOException.class)
                .hasMessageContaining("not fully accelerable");
    }
}
