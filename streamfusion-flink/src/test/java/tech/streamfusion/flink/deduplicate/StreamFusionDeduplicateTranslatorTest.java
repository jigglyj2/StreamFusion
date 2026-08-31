/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.deduplicate;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

class StreamFusionDeduplicateTranslatorTest {
    private static final RowType Q18_TYPE = RowType.of(
            new LogicalType[] {
                new BigIntType(false), new BigIntType(false), new TimestampType(false, TimestampKind.ROWTIME, 3)
            },
            new String[] {"auction", "bidder", "dateTime"});

    @Test
    void acceptsTimerFreeQ18Shape() {
        assertThat(reason(true, true, false, 0L, new Configuration())).isNull();
    }

    @Test
    void rejectsShapesThatNeedTimersOrUnsupportedMiniBatchState() {
        assertThat(reason(true, false, false, 0L, new Configuration())).contains("event-time timers");
        assertThat(reason(true, true, false, 1L, new Configuration())).contains("TTL");

        Configuration miniBatch = new Configuration();
        miniBatch.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        assertThat(reason(true, true, false, 0L, miniBatch)).contains("mini-batch");

        Configuration changelogState = new Configuration();
        changelogState.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        assertThat(reason(true, true, false, 0L, changelogState)).contains("changelog-state");
    }

    @Test
    void rejectsUpdateBeforeUntilStoredRowsCanRemainArrowBacked() {
        assertThat(StreamFusionDeduplicateTranslator.unsupportedReason(
                        Q18_TYPE, Q18_TYPE, new int[] {1, 0}, true, true, false, true, 0L, new Configuration()))
                .contains("UPDATE_BEFORE");
    }

    private static String reason(
            boolean isRowtime,
            boolean keepLastRow,
            boolean outputInsertOnly,
            long stateRetention,
            Configuration configuration) {
        return StreamFusionDeduplicateTranslator.unsupportedReason(
                Q18_TYPE,
                Q18_TYPE,
                new int[] {1, 0},
                isRowtime,
                keepLastRow,
                outputInsertOnly,
                false,
                stateRetention,
                configuration);
    }
}
