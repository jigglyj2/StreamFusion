/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0.
 */
package tech.streamfusion.proto.plan.v1;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NativeExchangePlanTest {
    @Test
    void roundTripsFlinkDistributionAndOneSchemaHandshake() throws Exception {
        NativeExchangePlan plan = NativeExchangePlan.newBuilder()
                .setProtocolVersion(1)
                .setSchema(Schema.newBuilder()
                        .addFields(Field.newBuilder()
                                .setName("key")
                                .setType(LogicalType.newBuilder()
                                        .setNullable(false)
                                        .setInteger(EmptyType.getDefaultInstance())))
                        .addFields(Field.newBuilder()
                                .setName("__streamfusion_row_kind")
                                .setType(LogicalType.newBuilder()
                                        .setNullable(false)
                                        .setTinyint(EmptyType.getDefaultInstance()))))
                .setDistribution(ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH)
                .addKeyIndices(0)
                .setMaxParallelism(128)
                .setTransport(ExchangeTransport.EXCHANGE_TRANSPORT_ARROW_IPC_STREAM)
                .setMetadataColumns(ExchangeMetadataColumns.newBuilder().setRowKindIndex(1))
                .build();

        assertThat(NativeExchangePlan.parseFrom(plan.toByteArray())).isEqualTo(plan);
    }
}
