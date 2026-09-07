/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;

import java.lang.reflect.InvocationTargetException;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.types.logical.RowType;

/** Local buffering has no keyed backend, but uses the same validation as its selected fragment. */
final class StreamFusionLocalGroupAggregateSupport {
    private StreamFusionLocalGroupAggregateSupport() {}

    static String unsupportedReason(StreamExecLocalGroupAggregate local, ProcessorContext context) {
        RowType input = (RowType) local.getInputEdges().get(0).getOutputType();
        int[] grouping = localGroupGrouping(local);
        for (int index : grouping)
            if (index < 0 || index >= input.getFieldCount()) return "local aggregate key is outside its input";
        Configuration config = Configuration.fromMap(
                context.getPlanner().getTableConfig().getConfiguration().toMap());
        config.addAll(Configuration.fromMap(local.getPersistedConfig().toMap()));
        try {
            return (String) Class.forName(
                            "tech.streamfusion.flink.aggregate.StreamFusionLocalGroupAggregateTranslator",
                            true,
                            context.getPlanner().getFlinkContext().getClassLoader())
                    .getMethod(
                            "unsupportedStageReason",
                            RowType.class,
                            RowType.class,
                            int[].class,
                            AggregateCall[].class,
                            boolean[].class,
                            boolean.class,
                            ReadableConfig.class)
                    .invoke(
                            null,
                            input,
                            nativeGroupAccumulatorType(input, grouping),
                            grouping,
                            localGroupAggregateCalls(local),
                            localGroupCallNeedRetractions(local),
                            localGroupNeedRetraction(local),
                            config);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Local aggregate support inspection failed", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not inspect shared local aggregate support", failure);
        }
    }
}
