package tech.streamfusion.benchmark.nexmark;

import org.junit.jupiter.api.Test;

class LocalNexmarkBenchmarkIT {
    @Test
    void runsOfficialNexmarkThroughFlinkAndStreamFusionWithKafkaExactlyOnce() throws Exception {
        LocalNexmarkBenchmark.main(new String[] {"10000", "q0"});
    }
}
