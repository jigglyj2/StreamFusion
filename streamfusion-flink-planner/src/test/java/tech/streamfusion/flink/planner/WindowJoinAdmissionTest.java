/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.logical.TumblingWindowSpec;
import org.apache.flink.table.planner.plan.logical.WindowAttachedWindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecWindowJoin;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.planner.window.StreamFusionWindowJoinTranslator;

class WindowJoinAdmissionTest {
    private static final RowType TYPE = RowType.of(new BigIntType(), new BigIntType(false));
    private static final WindowAttachedWindowingStrategy WINDOW = new WindowAttachedWindowingStrategy(
            new TumblingWindowSpec(Duration.ofSeconds(1), null), new TimestampType(false, TimestampKind.ROWTIME, 3), 1);

    @Test
    void admitsKeyedAndKeylessInnerWindowsWhileOtherModesKeepTheirReason() {
        for (var mode : FlinkJoinType.values()) {
            for (boolean keyed : new boolean[] {false, true}) {
                var node = node(mode, keyed);
                var reason = StreamFusionPersistentAdmission.unsupportedReason(node, new Configuration());
                if (mode == FlinkJoinType.INNER) assertThat(reason).isNull();
                else assertThat(reason).contains("window join", "INNER");
            }
        }
    }

    @Test
    void activeMiniBatchSettingCannotDisappearWhenOnlyTheTimeZoneIsPersisted() {
        var config = new Configuration();
        config.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
        var node = node(FlinkJoinType.INNER, false);
        assertThat(StreamFusionPersistentAdmission.unsupportedReason(node, config))
                .contains("mini-batch");
        assertThat(StreamFusionPersistentAdmission.unsupportedReason(node, null))
                .isNull();
    }

    @Test
    void admissionDoesNotRemoveBackendSemanticChecks() {
        var spec = spec(FlinkJoinType.INNER, true);
        for (var option : java.util.List.of(
                ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)) {
            var config = new Configuration();
            config.set(option, true);
            assertThat(StreamFusionWindowJoinTranslator.unsupportedReason(
                            TYPE, TYPE, TYPE, spec, WINDOW, WINDOW, config))
                    .startsWith("state:")
                    .contains("not implemented");
        }
    }

    private static JoinSpec spec(FlinkJoinType mode, boolean keyed) {
        return new JoinSpec(
                mode,
                keyed ? new int[] {0} : new int[0],
                keyed ? new int[] {0} : new int[0],
                keyed ? new boolean[] {true} : new boolean[0],
                null);
    }

    private static StreamExecWindowJoin node(FlinkJoinType mode, boolean keyed) {
        return new StreamExecWindowJoin(
                new Configuration(),
                spec(mode, keyed),
                WINDOW,
                WINDOW,
                InputProperty.DEFAULT,
                InputProperty.DEFAULT,
                TYPE,
                "window join admission");
    }
}
