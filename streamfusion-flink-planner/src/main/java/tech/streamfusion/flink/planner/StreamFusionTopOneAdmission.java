/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecRank;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

/** FastTop1 subset with shared Arrow compute, batched state and full metric/recovery conformance. */
final class StreamFusionTopOneAdmission {
    private StreamFusionTopOneAdmission() {}

    static String unsupportedReason(StreamExecRank rank, ReadableConfig activeConfig) {
        var config = activeConfig == null ? new Configuration() : Configuration.fromMap(activeConfig.toMap());
        config.addAll(Configuration.fromMap(rank.getPersistedConfig().toMap()));
        if (rankType(rank) != RankType.ROW_NUMBER
                || !"APPEND_FAST".equals(rankStrategyName(rank))
                || !(rankRange(rank) instanceof ConstantRankRange)
                || ((ConstantRankRange) rankRange(rank)).getRankStart() != 1
                || ((ConstantRankRange) rankRange(rank)).getRankEnd() != 1)
            return "rank persistent admission: verified shared execution requires append-only ROW_NUMBER range [1,1]";
        if (rankPartitionKeys(rank).length == 0 || rankSortSpec(rank).getFieldSize() == 0)
            return "rank persistent admission: partitioned Top-1 with explicit ordering is verified; global rank and LIMIT remain gated";
        if (rankStateTtl(rank) != 0
                || !config.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).isZero())
            return "rank persistent admission: state TTL is not verified for shared Top-1";
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
                || config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED))
            return "rank persistent admission: shared Top-1 requires synchronous state and disabled mini-batching";
        var cache = ExecutionConfigOptions.TABLE_EXEC_RANK_TOPN_CACHE_SIZE;
        if (!config.get(cache).equals(cache.defaultValue()))
            return "rank persistent admission: custom table.exec.rank.topn-cache-size is not represented by native point state";
        var input = (RowType) rank.getInputEdges().get(0).getOutputType();
        for (int key : rankPartitionKeys(rank)) {
            var type = input.getTypeAt(key).getTypeRoot();
            if (type != LogicalTypeRoot.BIGINT && type != LogicalTypeRoot.INTEGER)
                return "rank persistent admission: partition key type " + type
                        + " lacks shared Top-1 recovery conformance";
        }
        for (var type : input.getChildren()) {
            switch (type.getTypeRoot()) {
                case BIGINT:
                case INTEGER:
                case VARCHAR:
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    if (((TimestampType) type).getPrecision() == 3
                            && ((TimestampType) type).getKind() != TimestampKind.PROCTIME) break;
                default:
                    return "rank persistent admission: input type " + type
                            + " lacks shared Top-1 conformance; verified payload/order types are BIGINT, INTEGER, VARCHAR and TIMESTAMP(3) without processing time";
            }
        }
        return null;
    }
}
