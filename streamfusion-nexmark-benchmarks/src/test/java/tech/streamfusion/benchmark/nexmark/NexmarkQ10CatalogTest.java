/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.benchmark.nexmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NexmarkQ10CatalogTest {
    @Test
    void blackholeWorkloadKeepsTheOfficialSelectAndPartitionLabelColumns() throws Exception {
        String official = NexmarkQueryCatalog.load("q10");
        String select = official.substring(official.indexOf("SELECT auction, bidder"));
        assertThat(NexmarkRowDataQueryCatalog.load("q10").trim()).isEqualTo(select.trim());
        assertThat(NexmarkRowDataQueryCatalog.supportedQueries()).contains("q10");
        assertThat(NexmarkRowDataQueryCatalog.sinkColumns("q10"))
                .isEqualTo(
                        "auction BIGINT, bidder BIGINT, price BIGINT, `dateTime` TIMESTAMP(3), extra STRING, dt STRING, hm STRING");
        // Filesystem commit/rolling behavior belongs to the unchanged original Q10 sink.
        assertThat(official)
                .contains(
                        "'sink.partition-commit.delay' = '1 min'", "'sink.rolling-policy.rollover-interval' = '1min'");
    }
}
