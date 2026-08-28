/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import tech.streamfusion.proto.plan.v1.CastKind;

class StreamFusionCastSupportTest {
    @ParameterizedTest
    @MethodSource("approvedCasts")
    void containsOnlyExplicitlyApprovedCastPairs(
            LogicalTypeRoot source, LogicalTypeRoot target, CastKind expectedKind) {
        assertThat(StreamFusionCastSupport.kind(source, target)).isEqualTo(expectedKind);
    }

    private static Stream<Arguments> approvedCasts() {
        return Stream.of(
                Arguments.of(LogicalTypeRoot.TINYINT, LogicalTypeRoot.SMALLINT, CastKind.CAST_KIND_TINYINT_TO_SMALLINT),
                Arguments.of(LogicalTypeRoot.TINYINT, LogicalTypeRoot.INTEGER, CastKind.CAST_KIND_TINYINT_TO_INTEGER),
                Arguments.of(LogicalTypeRoot.TINYINT, LogicalTypeRoot.BIGINT, CastKind.CAST_KIND_TINYINT_TO_BIGINT),
                Arguments.of(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, CastKind.CAST_KIND_SMALLINT_TO_INTEGER),
                Arguments.of(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.BIGINT, CastKind.CAST_KIND_SMALLINT_TO_BIGINT),
                Arguments.of(LogicalTypeRoot.INTEGER, LogicalTypeRoot.BIGINT, CastKind.CAST_KIND_INTEGER_TO_BIGINT),
                Arguments.of(LogicalTypeRoot.TINYINT, LogicalTypeRoot.FLOAT, CastKind.CAST_KIND_TINYINT_TO_FLOAT),
                Arguments.of(LogicalTypeRoot.TINYINT, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_TINYINT_TO_DOUBLE),
                Arguments.of(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.FLOAT, CastKind.CAST_KIND_SMALLINT_TO_FLOAT),
                Arguments.of(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_SMALLINT_TO_DOUBLE),
                Arguments.of(LogicalTypeRoot.INTEGER, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_INTEGER_TO_DOUBLE),
                Arguments.of(LogicalTypeRoot.FLOAT, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_FLOAT_TO_DOUBLE),
                Arguments.of(LogicalTypeRoot.DOUBLE, LogicalTypeRoot.FLOAT, CastKind.CAST_KIND_UNSPECIFIED),
                Arguments.of(LogicalTypeRoot.INTEGER, LogicalTypeRoot.FLOAT, CastKind.CAST_KIND_UNSPECIFIED),
                Arguments.of(LogicalTypeRoot.INTEGER, LogicalTypeRoot.SMALLINT, CastKind.CAST_KIND_INTEGER_TO_SMALLINT),
                Arguments.of(LogicalTypeRoot.INTEGER, LogicalTypeRoot.TINYINT, CastKind.CAST_KIND_INTEGER_TO_TINYINT),
                Arguments.of(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.TINYINT, CastKind.CAST_KIND_SMALLINT_TO_TINYINT),
                Arguments.of(LogicalTypeRoot.BIGINT, LogicalTypeRoot.TINYINT, CastKind.CAST_KIND_BIGINT_TO_TINYINT),
                Arguments.of(LogicalTypeRoot.BIGINT, LogicalTypeRoot.SMALLINT, CastKind.CAST_KIND_BIGINT_TO_SMALLINT),
                Arguments.of(LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, CastKind.CAST_KIND_BIGINT_TO_INTEGER),
                Arguments.of(LogicalTypeRoot.BIGINT, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_UNSPECIFIED));
    }
}
