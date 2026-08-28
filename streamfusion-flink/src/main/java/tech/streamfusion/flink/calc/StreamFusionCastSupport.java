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

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import tech.streamfusion.proto.plan.v1.CastKind;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.LogicalType;

/** Central approval matrix for native casts whose Flink parity has been proven. */
final class StreamFusionCastSupport {
    private static final Map<LogicalTypeRoot, Map<LogicalTypeRoot, CastKind>> APPROVED_CASTS = approvedCasts();

    private StreamFusionCastSupport() {}

    static CastKind kind(LogicalTypeRoot source, LogicalTypeRoot target) {
        return APPROVED_CASTS
                .getOrDefault(source, Collections.emptyMap())
                .getOrDefault(target, CastKind.CAST_KIND_UNSPECIFIED);
    }

    static LogicalType targetType(LogicalTypeRoot target, boolean nullable) {
        LogicalType.Builder type = LogicalType.newBuilder().setNullable(nullable);
        switch (target) {
            case TINYINT:
                return type.setTinyint(EmptyType.getDefaultInstance()).build();
            case SMALLINT:
                return type.setSmallint(EmptyType.getDefaultInstance()).build();
            case INTEGER:
                return type.setInteger(EmptyType.getDefaultInstance()).build();
            case BIGINT:
                return type.setBigint(EmptyType.getDefaultInstance()).build();
            case FLOAT:
                return type.setFloat(EmptyType.getDefaultInstance()).build();
            case DOUBLE:
                return type.setDouble(EmptyType.getDefaultInstance()).build();
            default:
                throw new IllegalArgumentException("Unsupported cast target " + target);
        }
    }

    private static Map<LogicalTypeRoot, Map<LogicalTypeRoot, CastKind>> approvedCasts() {
        EnumMap<LogicalTypeRoot, Map<LogicalTypeRoot, CastKind>> casts = new EnumMap<>(LogicalTypeRoot.class);
        approve(casts, LogicalTypeRoot.TINYINT, LogicalTypeRoot.SMALLINT, CastKind.CAST_KIND_TINYINT_TO_SMALLINT);
        approve(casts, LogicalTypeRoot.TINYINT, LogicalTypeRoot.INTEGER, CastKind.CAST_KIND_TINYINT_TO_INTEGER);
        approve(casts, LogicalTypeRoot.TINYINT, LogicalTypeRoot.BIGINT, CastKind.CAST_KIND_TINYINT_TO_BIGINT);
        approve(casts, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, CastKind.CAST_KIND_SMALLINT_TO_INTEGER);
        approve(casts, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.BIGINT, CastKind.CAST_KIND_SMALLINT_TO_BIGINT);
        approve(casts, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BIGINT, CastKind.CAST_KIND_INTEGER_TO_BIGINT);
        approve(casts, LogicalTypeRoot.TINYINT, LogicalTypeRoot.FLOAT, CastKind.CAST_KIND_TINYINT_TO_FLOAT);
        approve(casts, LogicalTypeRoot.TINYINT, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_TINYINT_TO_DOUBLE);
        approve(casts, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.FLOAT, CastKind.CAST_KIND_SMALLINT_TO_FLOAT);
        approve(casts, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_SMALLINT_TO_DOUBLE);
        approve(casts, LogicalTypeRoot.INTEGER, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_INTEGER_TO_DOUBLE);
        approve(casts, LogicalTypeRoot.FLOAT, LogicalTypeRoot.DOUBLE, CastKind.CAST_KIND_FLOAT_TO_DOUBLE);
        approve(casts, LogicalTypeRoot.INTEGER, LogicalTypeRoot.SMALLINT, CastKind.CAST_KIND_INTEGER_TO_SMALLINT);
        approve(casts, LogicalTypeRoot.INTEGER, LogicalTypeRoot.TINYINT, CastKind.CAST_KIND_INTEGER_TO_TINYINT);
        approve(casts, LogicalTypeRoot.SMALLINT, LogicalTypeRoot.TINYINT, CastKind.CAST_KIND_SMALLINT_TO_TINYINT);
        approve(casts, LogicalTypeRoot.BIGINT, LogicalTypeRoot.TINYINT, CastKind.CAST_KIND_BIGINT_TO_TINYINT);
        approve(casts, LogicalTypeRoot.BIGINT, LogicalTypeRoot.SMALLINT, CastKind.CAST_KIND_BIGINT_TO_SMALLINT);
        approve(casts, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, CastKind.CAST_KIND_BIGINT_TO_INTEGER);
        casts.replaceAll((ignored, targets) -> Collections.unmodifiableMap(targets));
        return Collections.unmodifiableMap(casts);
    }

    private static void approve(
            EnumMap<LogicalTypeRoot, Map<LogicalTypeRoot, CastKind>> casts,
            LogicalTypeRoot source,
            LogicalTypeRoot target,
            CastKind kind) {
        casts.computeIfAbsent(source, ignored -> new EnumMap<>(LogicalTypeRoot.class))
                .put(target, kind);
    }
}
