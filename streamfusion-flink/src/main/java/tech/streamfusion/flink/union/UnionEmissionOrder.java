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

import java.util.Arrays;

/** Restores the cross-input arrival order after native UNION concatenates its input batches. */
final class UnionEmissionOrder {
    private UnionEmissionOrder() {}

    static int[] restore(int[] inputRows, int[] arrivalIndexesByInputRow) {
        if (inputRows.length != arrivalIndexesByInputRow.length) {
            throw new IllegalStateException("Native UNION ALL must emit exactly one row for every input row");
        }
        int[] outputIndexes = new int[inputRows.length];
        Arrays.fill(outputIndexes, -1);
        for (int outputIndex = 0; outputIndex < inputRows.length; outputIndex++) {
            int inputRow = inputRows[outputIndex];
            if (inputRow < 0 || inputRow >= arrivalIndexesByInputRow.length) {
                throw new IllegalStateException("Native UNION ALL returned invalid input-row ordinal " + inputRow);
            }
            int arrivalIndex = arrivalIndexesByInputRow[inputRow];
            if (arrivalIndex < 0 || arrivalIndex >= outputIndexes.length || outputIndexes[arrivalIndex] >= 0) {
                throw new IllegalStateException("Native UNION ALL returned an invalid arrival-order mapping");
            }
            outputIndexes[arrivalIndex] = outputIndex;
        }
        return outputIndexes;
    }
}
