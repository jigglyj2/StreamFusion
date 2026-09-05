package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class BenchmarkResultStoreTest {
    @Test
    void materializesDuplicatesRetractionsAndUpdatesAsAMultiset() {
        BenchmarkResultStore.Result baseline = collect("baseline", new Change(true, "a"), new Change(true, "b"));
        BenchmarkResultStore.Result changelog = collect(
                "changelog",
                new Change(true, "a"),
                new Change(true, "a"),
                new Change(false, "a"),
                new Change(true, "old"),
                new Change(false, "old"),
                new Change(true, "b"));

        assertThat(changelog.rowCount()).isEqualTo(6);
        assertThat(changelog.sha256()).isNotEqualTo(baseline.sha256());
        assertThat(changelog.materializedRowCount()).isEqualTo(2);
        assertThat(changelog.materializedSha256()).isEqualTo(baseline.materializedSha256());
        assertThat(changelog.materializedDebugRows()).containsExactly("+I[a]", "+I[b]");
    }

    @Test
    void rejectsAChangelogWithANetRetraction() {
        BenchmarkResultStore.begin("net-retraction");
        add("net-retraction", false, "missing");

        assertThatThrownBy(() -> BenchmarkResultStore.finish("net-retraction"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retracts a row that is not present");
    }

    private static BenchmarkResultStore.Result collect(String runId, Change... changes) {
        BenchmarkResultStore.begin(runId);
        for (Change change : changes) {
            add(runId, change.accumulate, change.value);
        }
        return BenchmarkResultStore.finish(runId);
    }

    private static void add(String runId, boolean accumulate, String value) {
        byte[] materialized = value.getBytes(StandardCharsets.UTF_8);
        byte[] changelog = ((accumulate ? "+" : "-") + value).getBytes(StandardCharsets.UTF_8);
        BenchmarkResultStore.add(
                runId,
                changelog,
                materialized,
                null,
                accumulate,
                (accumulate ? "+I[" : "-D[") + value + "]",
                "+I[" + value + "]");
    }

    @Test
    void materializesUpdateAfterAsAnUpsertWhenAPrimaryKeyIsPresent() {
        BenchmarkResultStore.begin("keyed-upsert");
        addKeyed("keyed-upsert", true, "auction-1", "old");
        addKeyed("keyed-upsert", true, "auction-1", "winner");

        BenchmarkResultStore.Result result = BenchmarkResultStore.finish("keyed-upsert");

        assertThat(result.rowCount()).isEqualTo(2);
        assertThat(result.materializedRowCount()).isEqualTo(1);
        assertThat(result.materializedDebugRows()).containsExactly("+I[winner]");
    }

    private static void addKeyed(String runId, boolean accumulate, String key, String value) {
        BenchmarkResultStore.add(
                runId,
                ((accumulate ? "+" : "-") + value).getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8),
                key.getBytes(StandardCharsets.UTF_8),
                accumulate,
                (accumulate ? "+I[" : "-D[") + value + "]",
                "+I[" + value + "]");
    }

    private static final class Change {
        private final boolean accumulate;
        private final String value;

        private Change(boolean accumulate, String value) {
            this.accumulate = accumulate;
            this.value = value;
        }
    }
}
