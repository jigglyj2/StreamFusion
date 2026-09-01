/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.ExchangeDistribution;
import tech.streamfusion.proto.plan.v1.NativeExchangePlan;

class NativeExchangePlanSerializerTest {
    @Test
    void recordsSchemaKeysAndStableFlinkEnvelopeIndices() throws Exception {
        RowType rowType = RowType.of(
                new org.apache.flink.table.types.logical.LogicalType[] {new IntType(), new VarCharType()},
                new String[] {"id", "name"});

        NativeExchangePlan plan =
                NativeExchangePlan.parseFrom(NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128));

        assertThat(plan.getDistribution()).isEqualTo(ExchangeDistribution.EXCHANGE_DISTRIBUTION_HASH);
        assertThat(plan.getKeyIndicesList()).containsExactly(0);
        assertThat(plan.getMaxParallelism()).isEqualTo(128);
        assertThat(plan.getParallelism()).isEqualTo(128);
        assertThat(plan.getPreserveKeyGroups()).isTrue();
        assertThat(plan.getSchema().getFieldsList())
                .extracting(tech.streamfusion.proto.plan.v1.Field::getName)
                .containsExactly("id", "name", ArrowExchangeBatch.ROW_KIND_COLUMN, ArrowExchangeBatch.TIMESTAMP_COLUMN);
        assertThat(plan.getMetadataColumns().getRowKindIndex()).isEqualTo(2);
        assertThat(plan.getMetadataColumns().getStreamRecordTimestampIndex()).isEqualTo(3);
    }

    @Test
    void recordsAlignedDestinationGatheringContract() throws Exception {
        RowType rowType = RowType.of(new IntType());

        NativeExchangePlan plan =
                NativeExchangePlan.parseFrom(NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128, 4, false));

        assertThat(plan.getParallelism()).isEqualTo(4);
        assertThat(plan.getPreserveKeyGroups()).isFalse();
    }

    @Test
    void recordsAnInputOnlyCanonicalRoutingKeyForComplexKeys() throws Exception {
        RowType rowType = RowType.of(new ArrayType(new IntType()));

        NativeExchangePlan plan =
                NativeExchangePlan.parseFrom(NativeExchangePlanSerializer.hash(rowType, new int[] {0}, 128));

        assertThat(plan.getMetadataColumns().hasRoutingKeyIndex()).isTrue();
        assertThat(plan.getMetadataColumns().getRoutingKeyIndex()).isEqualTo(3);
    }
}
