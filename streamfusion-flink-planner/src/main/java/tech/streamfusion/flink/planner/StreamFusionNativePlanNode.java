/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;

/** A selected physical node that builds its own protobuf fragment, never its neighbors' runtime
 * operators. Shared region discovery and composition connect those fragments. */
interface StreamFusionNativePlanNode extends ExecNode<RowData> {
    StreamFusionNativeNodeMetadata nativeMetadata();

    byte[] nativePlanFragment(PlannerBase planner);

    /** Task-open immutable source, carried separately from the portable plan. */
    default tech.streamfusion.flink.arrow.CsvLookupSnapshotSource lookupSource() {
        return null;
    }

    /** Resource capability, not a fusion rule. The region binds every owner in one native context. */
    default boolean ownsNativeKeyedState() {
        return false;
    }

    /** Window buffers need the original Flink capacity in addition to the runtime allocation budget. */
    default boolean ownsWindowBuffer() {
        return false;
    }

    static byte[] invokeBuilder(PlannerBase planner, String owner, Class<?>[] types, Object... arguments) {
        try {
            return (byte[]) Class.forName(owner, true, StreamFusionRuntimeClasses.class.getClassLoader())
                    .getMethod("createStagePlan", types)
                    .invoke(null, arguments);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Selected native node failed protobuf lowering", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not invoke native node protobuf builder " + owner, failure);
        }
    }
}
