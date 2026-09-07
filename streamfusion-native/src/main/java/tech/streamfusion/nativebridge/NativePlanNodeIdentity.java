/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.nativebridge;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Assigns stable pre-order identities to the native physical-plan tree before JNI. */
final class NativePlanNodeIdentity {
    private NativePlanNodeIdentity() {}

    static byte[] assign(byte[] serializedPlan) {
        try {
            NativePlan.Builder plan = NativePlan.parseFrom(serializedPlan).toBuilder();
            if (!plan.hasRoot()) {
                return serializedPlan;
            }
            Set<Long> existing = new HashSet<>();
            collectExisting(plan.getRoot(), existing);
            assign(plan.getRootBuilder(), new AtomicLong(1), existing);
            return plan.build().toByteArray();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid StreamFusion native plan", failure);
        }
    }

    private static void collectExisting(Message message, Set<Long> existing) {
        if (message instanceof Operator) {
            long id = ((Operator) message).getPlanNodeId();
            if (id < 0 || (id != 0 && !existing.add(id))) {
                throw new IllegalArgumentException("Native plan has an invalid or duplicate physical node id: " + id);
            }
        }
        for (Object value : message.getAllFields().values()) {
            if (value instanceof Message) {
                collectExisting((Message) value, existing);
            } else if (value instanceof Iterable<?>) {
                for (Object item : (Iterable<?>) value) {
                    if (item instanceof Message) {
                        collectExisting((Message) item, existing);
                    }
                }
            }
        }
    }

    static long rootId(byte[] serializedPlan) {
        try {
            NativePlan plan = NativePlan.parseFrom(serializedPlan);
            return plan.hasRoot() ? plan.getRoot().getPlanNodeId() : 0;
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid StreamFusion native plan", failure);
        }
    }

    private static void assign(Message.Builder message, AtomicLong nextId, Set<Long> existing) {
        if (message instanceof Operator.Builder) {
            Operator.Builder operator = (Operator.Builder) message;
            if (operator.getPlanNodeId() == 0) {
                while (existing.contains(nextId.get())) {
                    nextId.set(Math.addExact(nextId.get(), 1));
                }
                operator.setPlanNodeId(nextId.get());
                existing.add(nextId.get());
            }
        }
        // Traverse the protobuf shape, not an operator-family registry. Only visit present
        // fields: identity assignment must not manufacture missing physical children.
        for (var entry : message.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (field.isRepeated()) {
                for (int index = 0; index < message.getRepeatedFieldCount(field); index++) {
                    Message.Builder child = ((Message) message.getRepeatedField(field, index)).toBuilder();
                    assign(child, nextId, existing);
                    message.setRepeatedField(field, index, child.build());
                }
            } else {
                Message.Builder child = ((Message) entry.getValue()).toBuilder();
                assign(child, nextId, existing);
                message.setField(field, child.build());
            }
        }
    }
}
