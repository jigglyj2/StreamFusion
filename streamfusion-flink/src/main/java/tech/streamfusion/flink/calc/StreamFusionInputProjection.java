/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;

/** Pushes first-stage input references into the source-edge Arrow writer. */
final class StreamFusionInputProjection {
    private StreamFusionInputProjection() {}

    static Projection create(RowType inputType, List<Expression> projections, Expression condition) {
        TreeSet<Integer> referenced = new TreeSet<>();
        for (Expression expression : projections) {
            collectInputReferences(expression, referenced);
        }
        if (condition != null) {
            collectInputReferences(condition, referenced);
        }
        if (referenced.isEmpty() && inputType.getFieldCount() != 0) {
            referenced.add(0);
        }
        int[] ordinals = referenced.stream().mapToInt(Integer::intValue).toArray();
        int[] remapping = new int[inputType.getFieldCount()];
        Arrays.fill(remapping, -1);
        List<RowType.RowField> fields = new ArrayList<>(ordinals.length);
        for (int projected = 0; projected < ordinals.length; projected++) {
            int original = ordinals[projected];
            if (original < 0 || original >= inputType.getFieldCount()) {
                throw new IllegalArgumentException("Calc input reference is outside its RowType");
            }
            remapping[original] = projected;
            fields.add(inputType.getFields().get(original));
        }
        List<Expression> rewritten = projections.stream()
                .map(expression -> rewrite(expression, remapping))
                .collect(Collectors.toList());
        return new Projection(
                new RowType(inputType.isNullable(), fields),
                ordinals,
                rewritten,
                condition == null ? null : rewrite(condition, remapping));
    }

    private static void collectInputReferences(Message message, TreeSet<Integer> referenced) {
        if (message instanceof InputReference) {
            referenced.add(((InputReference) message).getIndex());
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            if (entry.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (entry.getKey().isRepeated()) {
                for (Object child : (List<?>) entry.getValue()) {
                    collectInputReferences((Message) child, referenced);
                }
            } else {
                collectInputReferences((Message) entry.getValue(), referenced);
            }
        }
    }

    private static Expression rewrite(Expression expression, int[] remapping) {
        return (Expression) rewriteMessage(expression, remapping);
    }

    private static Message rewriteMessage(Message message, int[] remapping) {
        if (message instanceof InputReference) {
            InputReference reference = (InputReference) message;
            int rewritten = reference.getIndex() < remapping.length ? remapping[reference.getIndex()] : -1;
            if (rewritten < 0) {
                throw new IllegalArgumentException("Calc input reference was not retained");
            }
            return reference.toBuilder().setIndex(rewritten).build();
        }
        Message.Builder builder = message.toBuilder();
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            FieldDescriptor field = entry.getKey();
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (field.isRepeated()) {
                List<?> children = (List<?>) entry.getValue();
                for (int index = 0; index < children.size(); index++) {
                    builder.setRepeatedField(field, index, rewriteMessage((Message) children.get(index), remapping));
                }
            } else {
                builder.setField(field, rewriteMessage((Message) entry.getValue(), remapping));
            }
        }
        return builder.build();
    }

    static final class Projection {
        private final RowType inputType;
        private final int[] fieldOrdinals;
        private final List<Expression> projections;
        private final Expression condition;

        private Projection(RowType inputType, int[] fieldOrdinals, List<Expression> projections, Expression condition) {
            this.inputType = inputType;
            this.fieldOrdinals = fieldOrdinals;
            this.projections = projections;
            this.condition = condition;
        }

        RowType inputType() {
            return inputType;
        }

        int[] fieldOrdinals() {
            return fieldOrdinals;
        }

        List<Expression> projections() {
            return projections;
        }

        Expression condition() {
            return condition;
        }
    }
}
