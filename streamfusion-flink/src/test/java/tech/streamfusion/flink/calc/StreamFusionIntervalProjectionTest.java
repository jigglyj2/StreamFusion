/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionIntervalProjectionTest {
    @Test
    void identityReferencesAcceptFlinksIntervalDeclarationNormalizationWithoutCasts() {
        var factory = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(factory);
        for (var source : List.of(
                DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3)),
                DataTypes.INTERVAL(DataTypes.DAY(4)),
                DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.SECOND(3)),
                DataTypes.INTERVAL(DataTypes.YEAR(4)),
                DataTypes.INTERVAL(DataTypes.MONTH()))) {
            var input = source.getLogicalType();
            var reference = rex.makeInputRef(factory.createFieldTypeFromLogicalType(input), 0);
            var normalized = StreamFusionExpressionTranslator.expressionLogicalType(reference);
            assertThat(normalized).isNotNull();
            assertThat(StreamFusionCalcTranslator.unsupportedReason(
                            RowType.of(input), RowType.of(normalized), List.of(reference), null))
                    .isNull();
            var expression = StreamFusionCalcTranslator.operatorExpression(reference, RowType.of(input), normalized);
            assertThat(expression.hasInputReference()).isTrue();
            assertThat(expression.getInputReference().getIndex()).isZero();
        }
    }

    @Test
    void rejectsCrossFamilyIntervalsAndUnrelatedPrecisionChanges() {
        var factory = new FlinkTypeFactory(getClass().getClassLoader(), RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(factory);
        for (var pair : List.of(
                List.of(DataTypes.INTERVAL(DataTypes.DAY()), DataTypes.INTERVAL(DataTypes.MONTH())),
                List.of(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6)),
                List.of(DataTypes.DECIMAL(12, 2), DataTypes.DECIMAL(12, 3)))) {
            var input = pair.get(0).getLogicalType();
            var output = pair.get(1).getLogicalType();
            var reference = rex.makeInputRef(factory.createFieldTypeFromLogicalType(input), 0);
            assertThat(StreamFusionCalcTranslator.unsupportedReason(
                            RowType.of(input), RowType.of(output), List.of(reference), null))
                    .contains("input and output types must match", "input=", "output=");
        }
    }
}
