package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.streamfusion.flink.StreamFusionPlannerFactory;

/** Checks the actual reporter/sink lifecycle, including zero output and multiple sink subtasks. */
class NexmarkBlackholeMetricsIT {
    @ParameterizedTest
    @ValueSource(strings = {"hashmap", "rocksdb"})
    void blackholeCounterMatchesTheCollectingSink(String backend) throws Exception {
        for (boolean selected : new boolean[] {false, true})
            for (String query : new String[] {"q0", "q2"}) {
                var expected = LocalRowDataNexmarkBenchmark.run(10_000, query, selected, backend, 4);
                long actual = NexmarkRowDataJob.runBlackhole(10_000, query, selected, backend, 4, false);
                assertThat(actual).isEqualTo(expected.outputRows());
                assertThat(actual).isPositive();
                if (selected)
                    assertThat(StreamFusionPlannerFactory.nativePlanBatchCount())
                            .isPositive();
            }
        for (boolean selected : new boolean[] {false, true}) {
            // Nexmark's first event is a person; Q0 selects only bids.
            assertThat(NexmarkRowDataJob.runBlackhole(1, "q0", selected, backend, 1, false))
                    .isZero();
        }
    }
}
