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

import org.apache.arrow.vector.IntVector;
import org.apache.flink.table.data.columnar.vector.IntColumnVector;

/** A Flink column vector that reads an Arrow vector without materializing individual rows. */
final class ArrowIntColumnVector implements IntColumnVector {
    private final IntVector vector;

    ArrowIntColumnVector(IntVector vector) {
        this.vector = vector;
    }

    @Override
    public int getInt(int rowId) {
        return vector.get(rowId);
    }

    @Override
    public boolean isNullAt(int rowId) {
        return vector.isNull(rowId);
    }
}
