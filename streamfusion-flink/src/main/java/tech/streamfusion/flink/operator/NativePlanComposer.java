/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import tech.streamfusion.proto.plan.v1.Operator;

/** Binds a physical fragment's input slots without knowing its operator family. */
final class NativePlanComposer {
    private NativePlanComposer() {}

    static int inputCount(Operator stage) {
        return edges(stage).size();
    }

    static Operator bind(Operator stage, List<Operator> inputs) {
        List<Edge> edges = edges(stage);
        if (edges.size() != inputs.size()) {
            throw new IllegalArgumentException("Native fragment input arity does not match its bindings");
        }
        var payload = payload(stage);
        Message.Builder node = ((Message) stage.getField(payload)).toBuilder();
        for (Edge edge : edges) {
            Operator child = inputs.get(edge.slot);
            if (edge.index < 0) {
                node.setField(edge.field, child);
            } else {
                node.setRepeatedField(edge.field, edge.index, child);
            }
        }
        return stage.toBuilder().setField(payload, node.build()).build();
    }

    private static List<Edge> edges(Operator stage) {
        Message node = (Message) stage.getField(payload(stage));
        List<Edge> result = new ArrayList<>();
        for (FieldDescriptor field : node.getDescriptorForType().getFields()) {
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || !field.getMessageType().equals(Operator.getDescriptor())) {
                continue;
            }
            if (field.isRepeated()) {
                for (int index = 0; index < node.getRepeatedFieldCount(field); index++) {
                    result.add(edge(field, index, (Operator) node.getRepeatedField(field, index)));
                }
            } else {
                if (!node.hasField(field)) {
                    throw new IllegalArgumentException("Native fragment is missing physical input " + field.getName());
                }
                result.add(edge(field, -1, (Operator) node.getField(field)));
            }
        }
        boolean[] used = new boolean[result.size()];
        for (Edge edge : result) {
            if (edge.slot < 0 || edge.slot >= used.length || used[edge.slot]) {
                throw new IllegalArgumentException(
                        "Native fragment input slots must be unique and contiguous from zero");
            }
            used[edge.slot] = true;
        }
        return result;
    }

    private static FieldDescriptor payload(Operator stage) {
        var fields = stage.getAllFields().entrySet().stream()
                .filter(entry -> entry.getValue() instanceof Message)
                .collect(java.util.stream.Collectors.toList());
        if (fields.size() != 1 || stage.getOperatorCase() == Operator.OperatorCase.INPUT) {
            throw new IllegalArgumentException(
                    "A native fragment must contain one physical operator, not an input edge");
        }
        return fields.get(0).getKey();
    }

    private static Edge edge(FieldDescriptor field, int index, Operator input) {
        if (input.getOperatorCase() != Operator.OperatorCase.INPUT) {
            throw new IllegalArgumentException(
                    "A native fragment must declare external Input slots, not nested stages");
        }
        return new Edge(field, index, input.getInput().getInputIndex());
    }

    private static final class Edge {
        private final FieldDescriptor field;
        private final int index;
        private final int slot;

        private Edge(FieldDescriptor field, int index, int slot) {
            this.field = field;
            this.index = index;
            this.slot = slot;
        }
    }
}
