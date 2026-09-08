/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.InvocationTargetException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;

/** Retains the original physical node's identity without translating its Flink runtime operator. */
final class StreamFusionNativeNodeMetadata {
    private ExecNodeBase<?> original;
    private StreamFusionOriginalWindowResources resources;
    private StreamFusionSharedNativeRegion sharedRegion;

    void bindSharedRegion(StreamFusionSharedNativeRegion owner) {
        if (sharedRegion != null && sharedRegion != owner)
            throw new IllegalStateException("A selected native stage cannot belong to two shared regions");
        sharedRegion = owner;
    }

    StreamFusionSharedNativeRegion sharedRegion() {
        return sharedRegion;
    }

    void bindResources(StreamFusionOriginalWindowResources resources) {
        if (this.resources != null && this.resources != resources)
            throw new IllegalStateException("A native stage cannot change its original resource graph");
        this.resources = resources;
    }

    StreamFusionOriginalWindowResources resources() {
        return resources;
    }

    void recordOutput(org.apache.flink.api.dag.Transformation<?> output) {
        if (resources != null) resources.recordOutput(original, output);
    }

    void bindOriginal(ExecNode<?> node) {
        if (!(node instanceof ExecNodeBase<?>)) {
            throw new IllegalArgumentException("Native metric origin must be a Flink physical node");
        }
        if (original != null && original != node) {
            throw new IllegalStateException("A native physical stage cannot change its original metric identity");
        }
        original = (ExecNodeBase<?>) node;
    }

    int physicalNodeId(ExecNode<?> selected) {
        return original == null ? selected.getId() : original.getId();
    }

    String metricName(ExecNode<?> selected, ReadableConfig tableConfig) {
        ExecNodeBase<?> origin = original == null ? (ExecNodeBase<?>) selected : original;
        Configuration config = effectiveConfig(origin, tableConfig);
        try {
            var name = ExecNodeBase.class.getDeclaredMethod("createTransformationName", ReadableConfig.class);
            name.setAccessible(true);
            return (String) name.invoke(origin, config);
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Could not derive the original Flink metric name", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not access Flink transformation naming", failure);
        }
    }

    @javax.annotation.Nullable String metricUid(ReadableConfig tableConfig) {
        if (original == null || ExecNodeMetadataUtil.isUnsupported(original.getClass())) return null;
        try {
            var compiled = ExecNodeBase.class.getDeclaredField("isCompiled");
            compiled.setAccessible(true);
            var config =
                    ExecNodeConfig.ofNodeConfig(effectiveConfig(original, tableConfig), compiled.getBoolean(original));
            if (!config.shouldSetUid()) return null;
            var metadata = ExecNodeMetadataUtil.extractMetadataFromAnnotation(original.getClass()).stream()
                    .filter(annotation -> annotation.version() == original.getVersion())
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Original Flink node version has no UID metadata"));
            String[] transformations = metadata.producedTransformations();
            // UNION is wiring and produces no operator. Multi-transformation nodes must expose
            // their individual physical stages before this contract can assign their UIDs.
            if (transformations.length == 0) return null;
            if (transformations.length != 1)
                throw new IllegalStateException("Original Flink node has multiple transformation UIDs");
            var create = ExecNodeBase.class.getDeclaredMethod(
                    "createTransformationMeta", String.class, ExecNodeConfig.class);
            create.setAccessible(true);
            var result = (TransformationMetadata) create.invoke(original, transformations[0], config);
            String uid = result.getUid();
            // Flink's JobGraph/OperatorIDPair rejects an explicitly empty UID. Do not turn
            // it into absence and accidentally bypass that validation inside a fused region.
            if (uid != null && uid.isEmpty())
                throw new IllegalStateException("Empty string operator uid is not allowed");
            return uid;
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Could not derive the original Flink operator UID", failure.getCause());
        } catch (ReflectiveOperationException failure) {
            throw new IllegalStateException("Could not access Flink transformation UID generation", failure);
        }
    }

    private static Configuration effectiveConfig(ExecNodeBase<?> origin, ReadableConfig tableConfig) {
        Configuration config = Configuration.fromMap(tableConfig.toMap());
        // This is ExecNodeConfig's precedence: persisted node values override table settings.
        config.addAll(Configuration.fromMap(origin.getPersistedConfig().toMap()));
        return config;
    }
}
