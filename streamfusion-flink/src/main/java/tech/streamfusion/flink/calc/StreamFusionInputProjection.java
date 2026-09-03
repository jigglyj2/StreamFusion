/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.proto.plan.v1.Expression;
import tech.streamfusion.proto.plan.v1.InputReference;

/** Pushes first-stage top-level and nested input references into the source-edge Arrow writer. */
final class StreamFusionInputProjection {
    private StreamFusionInputProjection() {}

    static Projection create(RowType inputType, List<Expression> projections, Expression condition) {
        TreeMap<PathKey, ProjectedPath> referenced = new TreeMap<>(PathKey.COMPARATOR);
        for (Expression expression : projections) {
            collectInputReferences(expression, inputType, referenced);
        }
        if (condition != null) {
            collectInputReferences(condition, inputType, referenced);
        }
        if (referenced.isEmpty() && inputType.getFieldCount() != 0) {
            ProjectedPath first = projectedPath(inputType, new int[] {0});
            referenced.put(new PathKey(first.path), first);
        }

        int[][] paths = new int[referenced.size()][];
        int[][] rowArities = new int[referenced.size()][];
        Map<PathKey, Integer> remapping = new HashMap<>();
        List<RowType.RowField> fields = new ArrayList<>(referenced.size());
        int projected = 0;
        for (Map.Entry<PathKey, ProjectedPath> entry : referenced.entrySet()) {
            ProjectedPath path = entry.getValue();
            paths[projected] = path.path;
            rowArities[projected] = path.rowArities;
            remapping.put(entry.getKey(), projected);
            fields.add(new RowType.RowField("__streamfusion_input_" + projected, path.type));
            projected++;
        }
        List<Expression> rewritten = projections.stream()
                .map(expression -> rewrite(expression, inputType, remapping))
                .collect(Collectors.toList());
        return new Projection(
                new RowType(inputType.isNullable(), fields),
                paths,
                rowArities,
                rewritten,
                condition == null ? null : rewrite(condition, inputType, remapping));
    }

    private static void collectInputReferences(
            Message message, RowType inputType, Map<PathKey, ProjectedPath> referenced) {
        if (message instanceof Expression) {
            ProjectedPath path = projectedPath((Expression) message, inputType);
            if (path != null) {
                referenced.put(new PathKey(path.path), path);
                return;
            }
        }
        if (message instanceof InputReference) {
            ProjectedPath path = projectedPath(inputType, new int[] {((InputReference) message).getIndex()});
            referenced.put(new PathKey(path.path), path);
            return;
        }
        for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
            if (entry.getKey().getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            if (entry.getKey().isRepeated()) {
                for (Object child : (List<?>) entry.getValue()) {
                    collectInputReferences((Message) child, inputType, referenced);
                }
            } else {
                collectInputReferences((Message) entry.getValue(), inputType, referenced);
            }
        }
    }

    private static Expression rewrite(Expression expression, RowType inputType, Map<PathKey, Integer> remapping) {
        return (Expression) rewriteMessage(expression, inputType, remapping);
    }

    private static Message rewriteMessage(Message message, RowType inputType, Map<PathKey, Integer> remapping) {
        if (message instanceof Expression) {
            ProjectedPath path = projectedPath((Expression) message, inputType);
            if (path != null) {
                Integer rewritten = remapping.get(new PathKey(path.path));
                if (rewritten == null) {
                    throw new IllegalArgumentException("Calc input path was not retained");
                }
                return StreamFusionCalcPlan.inputReference(rewritten, StreamFusionCalcPlan.logicalType(path.type));
            }
        }
        if (message instanceof InputReference) {
            InputReference reference = (InputReference) message;
            Integer rewritten = remapping.get(new PathKey(new int[] {reference.getIndex()}));
            if (rewritten == null) {
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
                    builder.setRepeatedField(
                            field, index, rewriteMessage((Message) children.get(index), inputType, remapping));
                }
            } else {
                builder.setField(field, rewriteMessage((Message) entry.getValue(), inputType, remapping));
            }
        }
        return builder.build();
    }

    private static ProjectedPath projectedPath(Expression expression, RowType inputType) {
        List<String> fields = new ArrayList<>();
        Expression current = expression;
        while (current.hasStructField()) {
            fields.add(0, current.getStructField().getFieldName());
            current = current.getStructField().getOperand();
        }
        if (!current.hasInputReference()) {
            return null;
        }

        int inputIndex = current.getInputReference().getIndex();
        int[] path = new int[fields.size() + 1];
        path[0] = inputIndex;
        LogicalType type = inputTypeAt(inputType, inputIndex);
        boolean nullable = type.isNullable();
        for (int depth = 0; depth < fields.size(); depth++) {
            if (!(type instanceof RowType)) {
                return null;
            }
            RowType row = (RowType) type;
            int fieldIndex = row.getFieldNames().indexOf(fields.get(depth));
            if (fieldIndex < 0) {
                return null;
            }
            path[depth + 1] = fieldIndex;
            type = row.getTypeAt(fieldIndex);
            nullable |= type.isNullable();
        }
        return projectedPath(inputType, path, type.copy(nullable));
    }

    private static ProjectedPath projectedPath(RowType inputType, int[] path) {
        return projectedPath(inputType, path, inputTypeAt(inputType, path[0]));
    }

    private static ProjectedPath projectedPath(RowType inputType, int[] path, LogicalType projectedType) {
        int[] rowArities = new int[Math.max(0, path.length - 1)];
        LogicalType current = inputTypeAt(inputType, path[0]);
        for (int depth = 0; depth < rowArities.length; depth++) {
            if (!(current instanceof RowType)) {
                throw new IllegalArgumentException("Calc nested input path traverses a non-row field");
            }
            RowType row = (RowType) current;
            rowArities[depth] = row.getFieldCount();
            int child = path[depth + 1];
            if (child < 0 || child >= row.getFieldCount()) {
                throw new IllegalArgumentException("Calc nested input path is outside its RowType");
            }
            current = row.getTypeAt(child);
        }
        return new ProjectedPath(path.clone(), rowArities, projectedType);
    }

    private static LogicalType inputTypeAt(RowType inputType, int index) {
        if (index < 0 || index >= inputType.getFieldCount()) {
            throw new IllegalArgumentException("Calc input reference is outside its RowType");
        }
        return inputType.getTypeAt(index);
    }

    private static final class ProjectedPath {
        private final int[] path;
        private final int[] rowArities;
        private final LogicalType type;

        private ProjectedPath(int[] path, int[] rowArities, LogicalType type) {
            this.path = path;
            this.rowArities = rowArities;
            this.type = type;
        }
    }

    private static final class PathKey {
        private static final Comparator<PathKey> COMPARATOR = (left, right) -> {
            int common = Math.min(left.path.length, right.path.length);
            for (int index = 0; index < common; index++) {
                int compared = Integer.compare(left.path[index], right.path[index]);
                if (compared != 0) {
                    return compared;
                }
            }
            return Integer.compare(left.path.length, right.path.length);
        };
        private final int[] path;

        private PathKey(int[] path) {
            this.path = path.clone();
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof PathKey && java.util.Arrays.equals(path, ((PathKey) other).path);
        }

        @Override
        public int hashCode() {
            return java.util.Arrays.hashCode(path);
        }
    }

    static final class Projection {
        private final RowType inputType;
        private final int[][] fieldPaths;
        private final int[][] rowArities;
        private final List<Expression> projections;
        private final Expression condition;

        private Projection(
                RowType inputType,
                int[][] fieldPaths,
                int[][] rowArities,
                List<Expression> projections,
                Expression condition) {
            this.inputType = inputType;
            this.fieldPaths = fieldPaths;
            this.rowArities = rowArities;
            this.projections = projections;
            this.condition = condition;
        }

        RowType inputType() {
            return inputType;
        }

        int[][] fieldPaths() {
            return fieldPaths;
        }

        int[][] rowArities() {
            return rowArities;
        }

        List<Expression> projections() {
            return projections;
        }

        Expression condition() {
            return condition;
        }
    }
}
