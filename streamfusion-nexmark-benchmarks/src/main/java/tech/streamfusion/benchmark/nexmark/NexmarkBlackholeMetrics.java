package tech.streamfusion.benchmark.nexmark;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

/** Reads Flink's existing sink I/O counters without adding operators or work to the record path. */
public final class NexmarkBlackholeMetrics implements MetricReporterFactory {
    private static final Map<String, Capture> RUNS = new ConcurrentHashMap<>();

    static Capture begin() {
        var capture = new Capture(UUID.randomUUID().toString());
        RUNS.put(capture.id, capture);
        return capture;
    }

    static void configure(Configuration config, String runId) {
        config.setString("metrics.reporter.nexmark-output.factory.class", NexmarkBlackholeMetrics.class.getName());
        config.setString("metrics.reporter.nexmark-output.run-id", runId);
    }

    @Override
    public MetricReporter createMetricReporter(Properties properties) {
        var capture = RUNS.get(properties.getProperty("run-id"));
        if (capture == null) throw new IllegalArgumentException("Unknown local blackhole measurement run");
        return new Reporter(capture);
    }

    private static final class Reporter implements MetricReporter {
        private final Capture capture;

        Reporter(Capture capture) {
            this.capture = capture;
        }

        @Override
        public void open(MetricConfig config) {}

        @Override
        public void close() {}

        @Override
        public void notifyOfAddedMetric(Metric metric, String name, MetricGroup group) {
            String operator = group.getAllVariables().get("<operator_name>");
            if (metric instanceof Counter
                    && name.equals("numRecordsIn")
                    && operator != null
                    && operator.startsWith("nexmark_output[")
                    && operator.endsWith("]: Writer")) {
                capture.add((Counter) metric);
            }
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String name, MetricGroup group) {
            // Keep the final counter until the job completes. Its removal may precede await().
        }
    }

    static final class Capture implements AutoCloseable {
        final String id;
        private final Set<Counter> counters = Collections.newSetFromMap(new IdentityHashMap<>());
        private boolean closed;

        Capture(String id) {
            this.id = id;
        }

        synchronized void add(Counter counter) {
            if (!closed) counters.add(counter);
        }

        synchronized long outputRows() {
            if (closed || counters.isEmpty())
                throw new IllegalStateException("Blackhole measurement did not observe Flink's sink input counters");
            long count = 0;
            for (var counter : counters) {
                long value = counter.getCount();
                if (value < 0) throw new IllegalStateException("Negative blackhole input count");
                count = Math.addExact(count, value);
            }
            return count;
        }

        @Override
        public synchronized void close() {
            closed = true;
            counters.clear();
            RUNS.remove(id, this);
        }
    }
}
