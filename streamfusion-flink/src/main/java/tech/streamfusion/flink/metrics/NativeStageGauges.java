/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.metrics;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import tech.streamfusion.proto.plan.v1.NativeGaugeDescriptor;
import tech.streamfusion.proto.plan.v1.NativeGaugeSchema;

/** Schema is discovered once; reporters read a Java snapshot without entering native execution. */
final class NativeStageGauges {
    private static final Set<String> STANDARD_METRICS = Set.of(
            "numRecordsIn",
            "numRecordsOut",
            "numBytesIn",
            "numBytesOut",
            "numRecordsInPerSecond",
            "numRecordsOutPerSecond",
            "numBytesInPerSecond",
            "numBytesOutPerSecond",
            "currentInputWatermark",
            "currentOutputWatermark");
    private final List<NativeGaugeDescriptor> descriptors;
    private final Supplier<long[]> source;
    private volatile long[] values = new long[0];

    NativeStageGauges(byte[] schema, LongFunction<MetricGroup> groups, Supplier<long[]> source) {
        final NativeGaugeSchema decoded;
        try {
            decoded = NativeGaugeSchema.parseFrom(schema);
        } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid native gauge schema", failure);
        }
        if (decoded.getProtocolVersion() != 1) {
            throw new IllegalArgumentException("Unsupported native gauge schema version");
        }
        descriptors = decoded.getGaugesList();
        this.source = source;
        Set<List<Object>> seen = new HashSet<>();
        // Validate the complete schema and initial sample before registering any metrics.
        for (var gauge : descriptors) {
            if (gauge.getPlanNodeId() <= 0 || groups.apply(gauge.getPlanNodeId()) == null) {
                throw new IllegalArgumentException("Unknown native gauge physical stage: " + gauge.getPlanNodeId());
            }
            validateSegment(gauge.getName());
            gauge.getGroupsList().forEach(NativeStageGauges::validateSegment);
            switch (gauge.getValueKind()) {
                case NATIVE_GAUGE_VALUE_KIND_INT32:
                case NATIVE_GAUGE_VALUE_KIND_INT64:
                case NATIVE_GAUGE_VALUE_KIND_FLOAT64:
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported native gauge value kind");
            }
            if ((gauge.getGroupsCount() == 0 && STANDARD_METRICS.contains(gauge.getName()))
                    || !seen.add(List.of(gauge.getPlanNodeId(), gauge.getGroupsList(), gauge.getName()))) {
                throw new IllegalArgumentException("Duplicate native gauge metric: " + gauge.getName());
            }
        }
        update();
        for (int index = 0; index < descriptors.size(); index++) {
            var gauge = descriptors.get(index);
            MetricGroup group = groups.apply(gauge.getPlanNodeId());
            for (String child : gauge.getGroupsList()) group = group.addGroup(child);
            final int slot = index;
            switch (gauge.getValueKind()) {
                case NATIVE_GAUGE_VALUE_KIND_INT32:
                    group.gauge(gauge.getName(), (Gauge<Integer>) () -> (int) values[slot]);
                    break;
                case NATIVE_GAUGE_VALUE_KIND_INT64:
                    group.gauge(gauge.getName(), (Gauge<Long>) () -> values[slot]);
                    break;
                case NATIVE_GAUGE_VALUE_KIND_FLOAT64:
                    group.gauge(gauge.getName(), (Gauge<Double>) () -> Double.longBitsToDouble(values[slot]));
                    break;
                default:
                    throw new AssertionError("Gauge kind was validated");
            }
        }
    }

    void update() {
        if (descriptors.isEmpty()) {
            return;
        }
        long[] next = source.get();
        if (next.length != descriptors.size()) {
            throw new IllegalArgumentException("Native gauge snapshot has an invalid shape");
        }
        for (int index = 0; index < next.length; index++) {
            if (descriptors.get(index).getValueKind()
                            == tech.streamfusion.proto.plan.v1.NativeGaugeValueKind.NATIVE_GAUGE_VALUE_KIND_INT32
                    && next[index] != (int) next[index]) {
                throw new IllegalArgumentException("Native INT32 gauge is not sign extended");
            }
        }
        // JNI returns a fresh array. Never mutate a published sample: reporters run concurrently.
        values = next;
    }

    private static void validateSegment(String segment) {
        if (segment.isEmpty() || segment.contains("/")) {
            throw new IllegalArgumentException("Invalid native gauge metric path");
        }
    }
}
