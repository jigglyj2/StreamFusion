/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;

/** A logical NOT NULL PROCTIME attribute is still a null slot in Flink's physical data plane. */
class ProcessingTimePhysicalSchemaTest {
    @Test
    void clockPlaceholderIsNullableInArrowAndProtobufButOrdinaryTimestampsKeepTheirConstraint() throws Exception {
        for (var kind : TimestampKind.values()) {
            var time = new LocalZonedTimestampType(false, kind, 3);
            var row = RowType.of(new org.apache.flink.table.types.logical.LogicalType[] {time}, new String[] {"pt"});
            boolean placeholder = kind == TimestampKind.PROCTIME;
            assertThat(time.isNullable()).isFalse();
            assertThat(FlinkLogicalTypeProto.serialize(time).getNullable()).isEqualTo(placeholder);
            assertThat(ArrowUtils.toArrowSchema(row).getFields().get(0).isNullable())
                    .isEqualTo(placeholder);
            if (placeholder)
                try (var allocator = new RootAllocator(1 << 20);
                        var batch = ArrowRowDataBatch.transpose(
                                java.util.List.of(GenericRowData.of((Object) null)), row, allocator)) {
                    assertThat(batch.rowView(0).isNullAt(0)).isTrue();
                }
        }
    }
}
