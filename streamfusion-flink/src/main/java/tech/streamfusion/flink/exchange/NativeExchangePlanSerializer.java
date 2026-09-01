/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.ExchangeDistribution;
import tech.streamfusion.proto.plan.v1.ExchangeMetadataColumns;
import tech.streamfusion.proto.plan.v1.ExchangeTransport;
import tech.streamfusion.proto.plan.v1.Field;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;
import tech.streamfusion.proto.plan.v1.Schema;

/** Serializes the complete Java-side native exchange control contract. */
public final class NativeExchangePlanSerializer {
    private NativeExchangePlanSerializer() {}

    public static byte[] hash(RowType rowType, int[] keys, int maxParallelism) {
        return hash(rowType, keys, maxParallelism, maxParallelism, true);
    }

    public static byte[] hash(
            RowType rowType, int[] keys, int maxParallelism, int parallelism, boolean preserveKeyGroups) {
        NativeExchangePlan.Builder plan = base(rowType)
                .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH)
                .setMaxParallelism(maxParallelism)
                .setParallelism(parallelism)
                .setPreserveKeyGroups(preserveKeyGroups);
        for (int key : keys) {
            plan.addKeyIndices(key);
        }
        return plan.build().toByteArray();
    }

    public static byte[] singleton(RowType rowType) {
        return base(rowType)
                .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_SINGLETON)
                .setMaxParallelism(1)
                .setParallelism(1)
                .setPreserveKeyGroups(false)
                .build()
                .toByteArray();
    }

    private static NativeExchangePlan.Builder base(RowType rowType) {
        RowType exchangeType = ArrowExchangeBatch.exchangeRowType(rowType);
        Schema.Builder schema = Schema.newBuilder();
        for (RowType.RowField field : exchangeType.getFields()) {
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        }
        int rowKindIndex = rowType.getFieldCount();
        return NativeExchangePlan.newBuilder()
                .setProtocolVersion(1)
                .setSchema(schema)
                .setTransport(ExchangeTransport.EXCHANGE_TRANSPORT_ARROW_IPC_STREAM)
                .setMetadataColumns(ExchangeMetadataColumns.newBuilder()
                        .setRowKindIndex(rowKindIndex)
                        .setStreamRecordTimestampIndex(rowKindIndex + 1));
    }
}
