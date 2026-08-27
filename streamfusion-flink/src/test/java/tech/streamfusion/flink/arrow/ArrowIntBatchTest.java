/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

class ArrowIntBatchTest {
    @Test
    void transposesRowDataAndExposesReusableArrowBackedView() {
        List<RowData> rows = List.of(GenericRowData.of(11), GenericRowData.of(22), GenericRowData.of(33));

        try (ArrowIntBatch batch = ArrowIntBatch.fromRows(rows, 0, rows.size())) {
            RowData first = batch.rowView(0);
            RowData second = batch.rowView(1);

            assertThat(first).isSameAs(second);
            assertThat(second.getInt(0)).isEqualTo(22);
            assertThat(batch.toRebasedArray()).containsExactly(11, 22, 33);
        }
    }
}
