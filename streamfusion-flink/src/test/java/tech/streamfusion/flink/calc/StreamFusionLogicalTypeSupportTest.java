/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.junit.jupiter.api.Test;

class StreamFusionLogicalTypeSupportTest {
    @Test
    void comparesCanonicalIntervalShapeWhileIgnoringOnlyTopLevelNullability() {
        YearMonthIntervalType nullableMonths = new YearMonthIntervalType(
                true, YearMonthIntervalType.YearMonthResolution.MONTH, YearMonthIntervalType.DEFAULT_PRECISION);
        LogicalType requiredMonths = nullableMonths.copy(false);
        YearMonthIntervalType yearsToMonths =
                new YearMonthIntervalType(true, YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH, 4);

        DayTimeIntervalType nullableSeconds =
                new DayTimeIntervalType(true, DayTimeIntervalType.DayTimeResolution.SECOND, 2, 3);
        LogicalType requiredSeconds = nullableSeconds.copy(false);
        DayTimeIntervalType nanos = new DayTimeIntervalType(true, DayTimeIntervalType.DayTimeResolution.SECOND, 2, 9);

        assertThat(StreamFusionLogicalTypeSupport.sameTypeIgnoringNullability(nullableMonths, requiredMonths))
                .isTrue();
        assertThat(StreamFusionLogicalTypeSupport.sameTypeIgnoringNullability(nullableSeconds, requiredSeconds))
                .isTrue();
        assertThat(StreamFusionLogicalTypeSupport.sameTypeIgnoringNullability(nullableMonths, yearsToMonths))
                .isFalse();
        assertThat(StreamFusionLogicalTypeSupport.sameTypeIgnoringNullability(nullableSeconds, nanos))
                .isFalse();
    }
}
