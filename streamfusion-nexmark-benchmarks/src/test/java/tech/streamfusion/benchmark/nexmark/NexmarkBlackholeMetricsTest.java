package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Properties;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

class NexmarkBlackholeMetricsTest {
    @Test
    void observesFinalSinkCountsWithoutCountingAliasesOtherStagesOrOtherRuns() {
        var factory = new NexmarkBlackholeMetrics();
        try (var first = NexmarkBlackholeMetrics.begin();
                var second = NexmarkBlackholeMetrics.begin()) {
            var properties = new Properties();
            properties.setProperty("run-id", first.id);
            var reporter = factory.createMetricReporter(properties);
            var sink = group("nexmark_output[17]: Writer");
            var one = new SimpleCounter();
            var two = new SimpleCounter();
            reporter.notifyOfAddedMetric(one, "numRecordsIn", sink);
            reporter.notifyOfAddedMetric(one, "numRecordsIn", sink);
            reporter.notifyOfAddedMetric(two, "numRecordsIn", sink);
            assertThat(first.outputRows()).isZero();
            one.inc(11);
            reporter.notifyOfRemovedMetric(one, "numRecordsIn", sink);
            two.inc(17);
            reporter.notifyOfAddedMetric(new SimpleCounter(), "numRecordsOut", sink);
            var unrelated = new SimpleCounter();
            unrelated.inc(99);
            reporter.notifyOfAddedMetric(unrelated, "numRecordsIn", group("WindowAggregate"));
            reporter.notifyOfAddedMetric(unrelated, "numRecordsIn", group("another_table[17]: Writer"));
            assertThat(first.outputRows()).isEqualTo(28);
            assertThatThrownBy(second::outputRows).hasMessageContaining("did not observe");
            first.close();
            reporter.notifyOfAddedMetric(new SimpleCounter(), "numRecordsIn", sink);
            assertThatThrownBy(first::outputRows).hasMessageContaining("did not observe");
            assertThatThrownBy(() -> factory.createMetricReporter(properties)).hasMessageContaining("Unknown local");
        }
    }

    private static UnregisteredMetricsGroup group(String operator) {
        return new UnregisteredMetricsGroup() {
            @Override
            public Map<String, String> getAllVariables() {
                return Map.of("<operator_name>", operator);
            }
        };
    }
}
