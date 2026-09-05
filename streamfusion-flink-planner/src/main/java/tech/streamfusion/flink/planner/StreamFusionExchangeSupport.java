/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.HashDistribution;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.KeepInputAsIsDistribution;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/** Exact support matrix for the native Arrow exchange boundary. */
final class StreamFusionExchangeSupport {
    private static final Set<LogicalTypeRoot> BOUNDARY_TYPES = EnumSet.of(
            LogicalTypeRoot.BOOLEAN,
            LogicalTypeRoot.TINYINT,
            LogicalTypeRoot.SMALLINT,
            LogicalTypeRoot.INTEGER,
            LogicalTypeRoot.BIGINT,
            LogicalTypeRoot.FLOAT,
            LogicalTypeRoot.DOUBLE,
            LogicalTypeRoot.CHAR,
            LogicalTypeRoot.VARCHAR,
            LogicalTypeRoot.BINARY,
            LogicalTypeRoot.VARBINARY,
            LogicalTypeRoot.DECIMAL,
            LogicalTypeRoot.DATE,
            LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
            LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
            LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            LogicalTypeRoot.INTERVAL_YEAR_MONTH,
            LogicalTypeRoot.INTERVAL_DAY_TIME,
            LogicalTypeRoot.ARRAY,
            LogicalTypeRoot.MAP,
            LogicalTypeRoot.MULTISET,
            LogicalTypeRoot.ROW);

    private StreamFusionExchangeSupport() {}

    static String unsupportedReason(RowType rowType, InputProperty.RequiredDistribution distribution) {
        if (distribution.getType() == InputProperty.DistributionType.KEEP_INPUT_AS_IS) {
            return unsupportedReason(rowType, ((KeepInputAsIsDistribution) distribution).getInputDistribution());
        }
        if (distribution.getType() != InputProperty.DistributionType.HASH
                && distribution.getType() != InputProperty.DistributionType.SINGLETON) {
            return "native exchange does not support " + distribution.getType() + " distribution";
        }
        for (int index = 0; index < rowType.getFieldCount(); index++) {
            String reason = unsupportedBoundaryType(rowType.getTypeAt(index));
            if (reason != null) {
                return "exchange field " + index + ": " + reason;
            }
        }
        if (distribution.getType() == InputProperty.DistributionType.HASH) {
            int[] keys = ((HashDistribution) distribution).getKeys();
            if (keys.length == 0) {
                return "native hash exchange requires at least one key";
            }
        }
        return null;
    }

    private static String unsupportedBoundaryType(LogicalType type) {
        while (type instanceof DistinctType) {
            type = ((DistinctType) type).getSourceType();
        }
        if (type instanceof StructuredType) {
            List<LogicalType> fields = LogicalTypeChecks.getFieldTypes(type);
            for (LogicalType field : fields) {
                String reason = unsupportedBoundaryType(field);
                if (reason != null) {
                    return reason;
                }
            }
            return null;
        }
        if (!BOUNDARY_TYPES.contains(type.getTypeRoot())) {
            return type.asSummaryString() + " has no Arrow exchange representation";
        }
        for (LogicalType child : type.getChildren()) {
            String reason = unsupportedBoundaryType(child);
            if (reason != null) {
                return reason;
            }
        }
        return null;
    }
}
