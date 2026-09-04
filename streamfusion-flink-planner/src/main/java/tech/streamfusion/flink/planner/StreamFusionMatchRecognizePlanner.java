/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MatchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMatch;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.RowType;

/** Eligibility and processing-time marker folding for native fixed MATCH_RECOGNIZE. */
final class StreamFusionMatchRecognizePlanner {
    private static final String TRANSLATOR_CLASS = "tech.streamfusion.flink.match.StreamFusionMatchRecognizeTranslator";

    private StreamFusionMatchRecognizePlanner() {}

    static ProcessingTimeMatchRecognize processingTimeMatchRecognize(StreamExecMatch node) {
        FixedMatchRecognize fixed = fixedMatchRecognize(node);
        if (fixed.rejectionReason != null || node.getInputEdges().size() != 1) {
            return fixed.rejectionReason == null
                    ? null
                    : new ProcessingTimeMatchRecognize(null, null, null, null, null, null, fixed);
        }
        ExecEdge matchInput = node.getInputEdges().get(0);
        if (!(matchInput.getSource() instanceof StreamExecExchange)) {
            return null;
        }
        StreamExecExchange exchange = (StreamExecExchange) matchInput.getSource();
        if (exchange.getInputEdges().size() != 1
                || !(exchange.getInputEdges().get(0).getSource() instanceof StreamExecCalc)) {
            return null;
        }
        StreamExecCalc inputCalc =
                (StreamExecCalc) exchange.getInputEdges().get(0).getSource();
        if (inputCalc.getInputEdges().size() != 1) {
            return null;
        }
        MatchSpec spec = matchSpec(node);
        int timeIndex = spec.getOrderKeys().getFieldSpec(0).getFieldIndex();
        List<RexNode> originalProjection = projection(inputCalc);
        if (timeIndex < 0
                || timeIndex >= originalProjection.size()
                || !isProctimeCall(originalProjection.get(timeIndex))) {
            return null;
        }
        // Flink appends the synthetic processing-time column after the physical payload. Keeping
        // this restriction makes exchange distribution and all existing field ordinals stable.
        if (timeIndex != originalProjection.size() - 1) {
            return null;
        }
        List<RexNode> inputProjection = new ArrayList<>(originalProjection);
        inputProjection.remove(timeIndex);
        if (inputProjection.isEmpty()) {
            return null;
        }
        for (RexNode condition : fixed.conditions) {
            if (condition != null && referencesField(condition, timeIndex)) {
                return null;
            }
        }
        for (int key : fixed.partitionKeys) {
            if (key == timeIndex) {
                return null;
            }
        }
        for (int measure : fixed.measureFields) {
            if (measure == timeIndex) {
                return null;
            }
        }
        return new ProcessingTimeMatchRecognize(
                inputCalc,
                exchange,
                inputCalc.getInputEdges().get(0),
                inputProjection,
                condition(inputCalc),
                withoutField((RowType) inputCalc.getOutputType(), timeIndex),
                fixed);
    }

    static String unsupportedReason(FixedMatchRecognize match, ProcessorContext context) {
        ExecEdge input = match.node.getInputEdges().get(0);
        try {
            Class<?> translator = Class.forName(
                    TRANSLATOR_CLASS,
                    true,
                    context.getPlanner().getFlinkContext().getClassLoader());
            Method method = translator.getMethod(
                    "unsupportedReason",
                    RowType.class,
                    RowType.class,
                    int[].class,
                    List.class,
                    List.class,
                    int[].class,
                    int[].class,
                    ReadableConfig.class);
            return (String) method.invoke(
                    null,
                    (RowType) input.getOutputType(),
                    (RowType) match.node.getOutputType(),
                    match.partitionKeys,
                    match.variableNames,
                    match.conditions,
                    match.measureVariables,
                    match.measureFields,
                    match.node.getPersistedConfig());
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Could not inspect StreamFusion MATCH_RECOGNIZE support", e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("StreamFusion MATCH_RECOGNIZE support inspection failed", e.getCause());
        }
    }

    private static FixedMatchRecognize fixedMatchRecognize(StreamExecMatch node) {
        MatchSpec spec = matchSpec(node);
        RowType inputType = (RowType) node.getInputEdges().get(0).getOutputType();
        SortSpec order = spec.getOrderKeys();
        if (order.getFieldSize() != 1) {
            return FixedMatchRecognize.rejected(
                    node, "order: native fixed MATCH_RECOGNIZE requires only the processing-time field");
        }
        int orderIndex = order.getFieldSpec(0).getFieldIndex();
        if (!order.getAscendingOrders()[0] || !TypeCheckUtils.isProcTime(inputType.getTypeAt(orderIndex))) {
            return FixedMatchRecognize.rejected(
                    node, "order: native fixed MATCH_RECOGNIZE currently requires ascending processing time");
        }
        if (spec.isAllRows()) {
            return FixedMatchRecognize.rejected(node, "rows per match: Flink does not support ALL ROWS PER MATCH");
        }
        if (!spec.getSubsets().isEmpty()) {
            return FixedMatchRecognize.rejected(node, "subsets: native fixed MATCH_RECOGNIZE does not support SUBSET");
        }
        if (spec.getInterval().isPresent()) {
            return FixedMatchRecognize.rejected(
                    node, "interval: native fixed MATCH_RECOGNIZE does not yet support WITHIN");
        }
        if (!(spec.getAfter() instanceof RexLiteral)) {
            return FixedMatchRecognize.rejected(
                    node, "after match: only SKIP TO NEXT ROW and SKIP PAST LAST ROW are supported");
        }
        SqlMatchRecognize.AfterOption after =
                ((RexLiteral) spec.getAfter()).getValueAs(SqlMatchRecognize.AfterOption.class);
        if (after != SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW
                && after != SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW) {
            return FixedMatchRecognize.rejected(
                    node, "after match: only SKIP TO NEXT ROW and SKIP PAST LAST ROW are supported");
        }

        List<String> variables = new ArrayList<>();
        String patternReason = flattenFixedPattern(spec.getPattern(), variables);
        if (patternReason != null) {
            return FixedMatchRecognize.rejected(node, patternReason);
        }
        if (variables.isEmpty() || new HashSet<>(variables).size() != variables.size()) {
            return FixedMatchRecognize.rejected(
                    node, "pattern: variables in a native fixed sequence must be non-empty and unique");
        }
        List<RexNode> conditions = new ArrayList<>(variables.size());
        for (String variable : variables) {
            RexNode definition = spec.getPatternDefinitions().get(variable);
            if (definition == null) {
                conditions.add(null);
                continue;
            }
            CurrentRowConditionNormalizer normalizer = new CurrentRowConditionNormalizer(variable);
            RexNode normalized = definition.accept(normalizer);
            if (normalizer.rejectionReason != null) {
                return FixedMatchRecognize.rejected(node, "define[" + variable + "]: " + normalizer.rejectionReason);
            }
            conditions.add(normalized);
        }

        int[] measureVariables = new int[spec.getMeasures().size()];
        int[] measureFields = new int[spec.getMeasures().size()];
        int measure = 0;
        for (Map.Entry<String, RexNode> entry : spec.getMeasures().entrySet()) {
            MatchMeasureReference reference = matchMeasure(entry.getValue(), variables);
            if (reference == null) {
                return FixedMatchRecognize.rejected(
                        node,
                        "measure[" + entry.getKey()
                                + "]: only direct FINAL/FIRST/LAST variable fields with offset zero are supported");
            }
            measureVariables[measure] = reference.variable;
            measureFields[measure] = reference.field;
            measure++;
        }
        return new FixedMatchRecognize(
                node,
                spec.getPartition().getFieldIndices(),
                variables,
                conditions,
                measureVariables,
                measureFields,
                after == SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW,
                null);
    }

    private static String flattenFixedPattern(RexNode pattern, List<String> variables) {
        if (pattern instanceof RexLiteral) {
            String name = ((RexLiteral) pattern).getValueAs(String.class);
            if (name == null) {
                return "pattern: a variable literal did not contain a name";
            }
            variables.add(name);
            return null;
        }
        if (!(pattern instanceof RexCall)
                || ((RexCall) pattern).getOperator() != SqlStdOperatorTable.PATTERN_CONCAT
                || ((RexCall) pattern).operands.size() != 2) {
            return "pattern: native MATCH_RECOGNIZE currently supports only fixed strict concatenation";
        }
        RexCall concat = (RexCall) pattern;
        String left = flattenFixedPattern(concat.operands.get(0), variables);
        return left != null ? left : flattenFixedPattern(concat.operands.get(1), variables);
    }

    private static MatchMeasureReference matchMeasure(RexNode expression, List<String> variables) {
        RexNode current = expression;
        if (current instanceof RexCall) {
            RexCall call = (RexCall) current;
            String name = call.getOperator().getName().toUpperCase(java.util.Locale.ROOT);
            boolean direct = name.contains("FINAL") || name.contains("FIRST") || name.contains("LAST");
            if (!direct || call.operands.isEmpty() || call.operands.size() > 2) {
                return null;
            }
            if (call.operands.size() == 2 && !isZeroLiteral(call.operands.get(1))) {
                return null;
            }
            current = call.operands.get(0);
        }
        if (!(current instanceof RexPatternFieldRef)) {
            return null;
        }
        RexPatternFieldRef field = (RexPatternFieldRef) current;
        int variable = variables.indexOf(field.getAlpha());
        return variable < 0 ? null : new MatchMeasureReference(variable, field.getIndex());
    }

    private static boolean isZeroLiteral(RexNode expression) {
        if (!(expression instanceof RexLiteral)) {
            return false;
        }
        Number value = ((RexLiteral) expression).getValueAs(Number.class);
        return value != null && value.longValue() == 0;
    }

    private static boolean referencesField(RexNode expression, int index) {
        boolean[] observed = {false};
        expression.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                observed[0] |= inputRef.getIndex() == index;
                return inputRef;
            }
        });
        return observed[0];
    }

    private static boolean isProctimeCall(RexNode expression) {
        return expression instanceof RexCall
                && ((RexCall) expression).getOperator().getName().equalsIgnoreCase("PROCTIME");
    }

    private static RowType withoutField(RowType type, int removed) {
        List<RowType.RowField> fields = new ArrayList<>(type.getFields());
        fields.remove(removed);
        return new RowType(type.isNullable(), fields);
    }

    private static MatchSpec matchSpec(StreamExecMatch match) {
        return (MatchSpec) field(match, CommonExecMatch.class, "matchSpec");
    }

    @SuppressWarnings("unchecked")
    private static List<RexNode> projection(StreamExecCalc calc) {
        return (List<RexNode>) field(calc, CommonExecCalc.class, "projection");
    }

    private static RexNode condition(StreamExecCalc calc) {
        return (RexNode) field(calc, CommonExecCalc.class, "condition");
    }

    private static Object field(Object node, Class<?> declaringClass, String name) {
        try {
            Field field = declaringClass.getDeclaredField(name);
            field.setAccessible(true);
            return field.get(node);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IllegalStateException("Could not read Flink exec node " + name, e);
        }
    }

    private static final class CurrentRowConditionNormalizer extends RexShuttle {
        private final String variable;
        private String rejectionReason;

        private CurrentRowConditionNormalizer(String variable) {
            this.variable = variable;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            String name = call.getOperator().getName().toUpperCase(java.util.Locale.ROOT);
            if (name.contains("LAST")) {
                if (call.operands.size() == 2 && isZeroLiteral(call.operands.get(1))) {
                    return call.operands.get(0).accept(this);
                }
                rejectionReason = "navigation requires LAST(current field, 0)";
                return call;
            }
            if (name.contains("FIRST") || name.contains("PREV") || name.contains("NEXT") || name.contains("FINAL")) {
                rejectionReason = "cross-row navigation is not implemented";
                return call;
            }
            return super.visitCall(call);
        }

        @Override
        public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
            if (!"*".equals(fieldRef.getAlpha()) && !variable.equals(fieldRef.getAlpha())) {
                rejectionReason = "condition references pattern variable " + fieldRef.getAlpha();
                return fieldRef;
            }
            return new RexInputRef(fieldRef.getIndex(), fieldRef.getType());
        }
    }

    private static final class MatchMeasureReference {
        private final int variable;
        private final int field;

        private MatchMeasureReference(int variable, int field) {
            this.variable = variable;
            this.field = field;
        }
    }

    static final class FixedMatchRecognize {
        final StreamExecMatch node;
        final int[] partitionKeys;
        final List<String> variableNames;
        final List<RexNode> conditions;
        final int[] measureVariables;
        final int[] measureFields;
        final boolean skipPastLastRow;
        final String rejectionReason;

        private FixedMatchRecognize(
                StreamExecMatch node,
                int[] partitionKeys,
                List<String> variableNames,
                List<RexNode> conditions,
                int[] measureVariables,
                int[] measureFields,
                boolean skipPastLastRow,
                String rejectionReason) {
            this.node = node;
            this.partitionKeys = partitionKeys.clone();
            this.variableNames = variableNames;
            this.conditions = conditions;
            this.measureVariables = measureVariables;
            this.measureFields = measureFields;
            this.skipPastLastRow = skipPastLastRow;
            this.rejectionReason = rejectionReason;
        }

        static FixedMatchRecognize rejected(StreamExecMatch node, String reason) {
            return new FixedMatchRecognize(
                    node, new int[0], List.of(), List.of(), new int[0], new int[0], false, reason);
        }
    }

    static final class ProcessingTimeMatchRecognize {
        final StreamExecCalc inputCalc;
        final StreamExecExchange exchange;
        final ExecEdge inputEdge;
        final List<RexNode> inputProjection;
        final RexNode inputCondition;
        final RowType inputType;
        final FixedMatchRecognize match;

        private ProcessingTimeMatchRecognize(
                StreamExecCalc inputCalc,
                StreamExecExchange exchange,
                ExecEdge inputEdge,
                List<RexNode> inputProjection,
                RexNode inputCondition,
                RowType inputType,
                FixedMatchRecognize match) {
            this.inputCalc = inputCalc;
            this.exchange = exchange;
            this.inputEdge = inputEdge;
            this.inputProjection = inputProjection;
            this.inputCondition = inputCondition;
            this.inputType = inputType;
            this.match = match;
        }
    }
}
