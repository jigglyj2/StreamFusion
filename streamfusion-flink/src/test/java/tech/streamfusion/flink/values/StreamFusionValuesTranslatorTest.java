/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.values;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class StreamFusionValuesTranslatorTest {
    @Test
    void acceptsFlinksOneRowZeroColumnSeedForSourceFreeExpressions() {
        RowType emptyOutput = RowType.of(new LogicalType[0]);

        assertThat(StreamFusionValuesTranslator.unsupportedReason(emptyOutput, List.of(List.of())))
                .isNull();
    }
}
