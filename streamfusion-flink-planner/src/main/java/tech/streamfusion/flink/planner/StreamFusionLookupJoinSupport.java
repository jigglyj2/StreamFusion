/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.util.Map;
import java.util.Set;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.arrow.CsvLookupSnapshotSource;

/** Semantic inspection of the original lookup node, without invoking its lookup function or file. */
final class StreamFusionLookupJoinSupport {
    private StreamFusionLookupJoinSupport() {}

    static String unsupportedReason(StreamExecLookupJoin node) {
        try {
            describe(node);
            return null;
        } catch (IllegalArgumentException unsupported) {
            return unsupported.getMessage();
        }
    }

    @SuppressWarnings("unchecked")
    static Shape describe(StreamExecLookupJoin node) {
        if (value(node, "joinType") != FlinkJoinType.INNER)
            reject("only synchronous inner equality lookup is implemented");
        if ((boolean) FlinkExecNodeAccess.field(node, StreamExecLookupJoin.class, "upsertMaterialize"))
            reject("upsert materialization requires Flink's keyed lookup state");
        for (String field : new String[] {
            "asyncLookupOptions",
            "retryOptions",
            "projectionOnTemporalTable",
            "filterOnTemporalTable",
            "preFilterCondition",
            "remainingJoinCondition"
        }) {
            if (value(node, field) != null) reject(field + " is not implemented in the native lookup stage");
        }
        if ((boolean) value(node, "preferCustomShuffle")) reject("custom lookup shuffle is not implemented");
        if (node.getInputEdges().size() != 1) reject("lookup requires one physical probe input");
        var temporal = node.getTemporalTableSourceSpec();
        if (temporal.getTableSourceSpec() != null) reject("only Flink's legacy CSV snapshot source is implemented");
        // Legacy sources are already resolved in this public specification. No planner context
        // or file I/O is needed to recover the original configured CsvTableSource.
        var table = temporal.getTemporalTable(null, null);
        if (!(table instanceof LegacyTableSourceTable)) reject("lookup source is not a legacy CSV table");
        var source = ((LegacyTableSourceTable<?>) table).tableSource();
        if (source.getClass() != CsvTableSource.class)
            reject("custom/non-CSV lookup source semantics are not implemented");
        var csv = (CsvTableSource) source;
        RowType input = (RowType) node.getInputEdges().get(0).getOutputType();
        RowType side = (RowType) csv.getProducedDataType().getLogicalType();
        RowType output = (RowType) node.getOutputType();
        var keys = (Map<Integer, FunctionCallUtil.FunctionParam>) value(node, "lookupKeys");
        if (keys.isEmpty()) reject("lookup requires nonempty equality keys");
        int[] sideKeys =
                keys.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
        int[] probeKeys = new int[sideKeys.length];
        Set<LogicalTypeRoot> supported = Set.of(
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.VARBINARY);
        for (int i = 0; i < sideKeys.length; i++) {
            var key = keys.get(sideKeys[i]);
            if (!(key instanceof FunctionCallUtil.FieldRef)) reject("constant lookup keys are not implemented");
            probeKeys[i] = ((FunctionCallUtil.FieldRef) key).index;
            if (probeKeys[i] < 0
                    || probeKeys[i] >= input.getFieldCount()
                    || sideKeys[i] < 0
                    || sideKeys[i] >= side.getFieldCount()) reject("lookup equality key is outside its payload");
            var left = input.getTypeAt(probeKeys[i]).getTypeRoot();
            var right = side.getTypeAt(sideKeys[i]).getTypeRoot();
            if (!supported.contains(left) || left != right)
                reject("lookup equality requires matching boolean, signed integer, VARCHAR or VARBINARY key types");
        }
        if (output.getFieldCount() != input.getFieldCount() + side.getFieldCount())
            reject("lookup output must contain the probe and snapshot payloads");
        for (int i = 0; i < output.getFieldCount(); i++) {
            var expected = i < input.getFieldCount() ? input.getTypeAt(i) : side.getTypeAt(i - input.getFieldCount());
            if (!expected.equals(output.getTypeAt(i))) reject("lookup output changes a payload type or nullability");
        }
        try {
            return new Shape(CsvLookupSnapshotSource.from(csv), side, probeKeys, sideKeys);
        } catch (Exception failure) {
            throw new IllegalArgumentException(
                    "lookup CSV source configuration cannot be represented: " + failure.getMessage(), failure);
        }
    }

    private static Object value(StreamExecLookupJoin node, String field) {
        return FlinkExecNodeAccess.field(node, CommonExecLookupJoin.class, field);
    }

    private static void reject(String reason) {
        throw new IllegalArgumentException("lookup: " + reason);
    }

    static final class Shape {
        final CsvLookupSnapshotSource source;
        final RowType sideType;
        final int[] probeKeys;
        final int[] sideKeys;

        Shape(CsvLookupSnapshotSource source, RowType sideType, int[] probeKeys, int[] sideKeys) {
            this.source = source;
            this.sideType = sideType;
            this.probeKeys = probeKeys;
            this.sideKeys = sideKeys;
        }
    }
}
