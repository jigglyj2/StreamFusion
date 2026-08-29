/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.union;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class UnionEmissionOrderTest {
    @Test
    void restoresInterleavedArrivalOrderAfterInputGroupedNativeOutput() {
        int[] nativeInputRows = {0, 1, 2, 3, 4};
        int[] arrivalIndexesByInputRow = {0, 2, 4, 1, 3};

        assertThat(UnionEmissionOrder.restore(nativeInputRows, arrivalIndexesByInputRow))
                .containsExactly(0, 3, 1, 4, 2);
    }

    @Test
    void rejectsCardinalityAndOrdinalContractViolations() {
        assertThatThrownBy(() -> UnionEmissionOrder.restore(new int[] {0}, new int[] {0, 1}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("exactly one row");
        assertThatThrownBy(() -> UnionEmissionOrder.restore(new int[] {1}, new int[] {0}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("input-row ordinal");
        assertThatThrownBy(() -> UnionEmissionOrder.restore(new int[] {0, 0}, new int[] {0, 1}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("arrival-order mapping");
    }
}
