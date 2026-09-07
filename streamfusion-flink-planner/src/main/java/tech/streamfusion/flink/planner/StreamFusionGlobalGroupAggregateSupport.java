/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.StateMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.types.logical.RowType;

/** Global fragment admission uses the same effective Flink settings as selected-node lowering. */
final class StreamFusionGlobalGroupAggregateSupport {
    private StreamFusionGlobalGroupAggregateSupport() {}

    static String unsupportedReason(StreamExecGlobalGroupAggregate global, ProcessorContext context) {
        int[] grouping = globalGroupGrouping(global);
        if (!Arrays.equals(grouping, IntStream.range(0, grouping.length).toArray())) {
            return "two-phase aggregate: global grouping must address the local grouping prefix";
        }
        RowType output = (RowType) global.getOutputType();
        if (output.getFieldCount() < grouping.length) {
            return "schema: global aggregate output is missing grouping fields";
        }
        Configuration config = Configuration.fromMap(
                context.getPlanner().getTableConfig().getConfiguration().toMap());
        config.addAll(Configuration.fromMap(global.getPersistedConfig().toMap()));
        long retention = StateMetadata.getStateTtlForOneInputOperator(
                ExecNodeConfig.ofNodeConfig(config, false), globalGroupStateMetadata(global));
        try {
            return (String) Class.forName(
                            "tech.streamfusion.flink.aggregate.StreamFusionGlobalGroupAggregateTranslator",
                            true,
                            context.getPlanner().getFlinkContext().getClassLoader())
                    .getMethod(
                            "unsupportedStageReason",
                            RowType.class,
                            RowType.class,
                            RowType.class,
                            int.class,
                            AggregateCall[].class,
                            boolean[].class,
                            boolean.class,
                            long.class,
                            ReadableConfig.class)
                    .invoke(
                            null,
                            globalGroupOriginalInputType(global),
                            nativeGroupAccumulatorType(output, grouping),
                            output,
                            grouping.length,
                            globalGroupAggregateCalls(global),
                            globalGroupCallNeedRetractions(global),
                            globalGroupNeedRetraction(global),
                            retention,
                            config);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Global aggregate support inspection failed", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not inspect shared global aggregate support", failure);
        }
    }
}
