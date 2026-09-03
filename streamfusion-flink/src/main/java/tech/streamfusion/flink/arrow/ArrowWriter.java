/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.streamfusion.flink.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import tech.streamfusion.flink.arrow.writers.ArrowFieldWriter;

/**
 * Writer which serializes the Flink rows to Arrow format.
 *
 * @param <IN> Type of the row to write.
 */
@Internal
public final class ArrowWriter<IN> {

    /** Container that holds a set of vectors for the rows to be sent to the Python worker. */
    private final VectorSchemaRoot root;

    /**
     * An array of writers which are responsible for the serialization of each column of the rows.
     */
    private final ArrowFieldWriter<IN>[] fieldWriters;

    private final int batchCapacity;

    private int rowCount;
    private GenericRowData nullRow;

    public ArrowWriter(VectorSchemaRoot root, ArrowFieldWriter<IN>[] fieldWriters, int batchCapacity) {
        this.root = Preconditions.checkNotNull(root);
        this.fieldWriters = Preconditions.checkNotNull(fieldWriters);
        Preconditions.checkArgument(batchCapacity > 0, "Batch capacity must be positive");
        this.batchCapacity = batchCapacity;
        reset();
    }

    /** Gets the field writers. */
    public ArrowFieldWriter<IN>[] getFieldWriters() {
        return fieldWriters;
    }

    /** Writes the specified row which is serialized into Arrow format. */
    public void write(IN row) {
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, i);
        }
        rowCount++;
    }

    /** Writes a projected row without constructing a RowData wrapper. */
    public void write(IN row, int[] fieldOrdinals) {
        if (fieldOrdinals.length != fieldWriters.length) {
            throw new IllegalArgumentException("Arrow field projection does not match its schema");
        }
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, fieldOrdinals[i]);
        }
        rowCount++;
    }

    /** Writes flattened nested fields without allocating an intermediate projected RowData. */
    @SuppressWarnings("unchecked")
    public void write(IN row, int[][] fieldPaths, int[][] rowArities) {
        if (!(row instanceof RowData)
                || fieldPaths.length != fieldWriters.length
                || rowArities.length != fieldWriters.length) {
            throw new IllegalArgumentException("Nested Arrow field projection does not match its schema");
        }
        if (nullRow == null) {
            int maximumOrdinal = 0;
            for (int[] path : fieldPaths) {
                if (path.length == 0) {
                    throw new IllegalArgumentException("Nested Arrow field path must not be empty");
                }
                maximumOrdinal = Math.max(maximumOrdinal, path[path.length - 1]);
            }
            nullRow = new GenericRowData(maximumOrdinal + 1);
        }
        RowData input = (RowData) row;
        for (int output = 0; output < fieldWriters.length; output++) {
            int[] path = fieldPaths[output];
            int[] arities = rowArities[output];
            if (path.length == 0 || arities.length != path.length - 1) {
                throw new IllegalArgumentException("Nested Arrow field path has an incompatible arity contract");
            }
            RowData parent = input;
            boolean nullAncestor = false;
            for (int depth = 0; depth < path.length - 1; depth++) {
                if (parent.isNullAt(path[depth])) {
                    nullAncestor = true;
                    break;
                }
                parent = parent.getRow(path[depth], arities[depth]);
            }
            int ordinal = path[path.length - 1];
            fieldWriters[output].write((IN) (nullAncestor ? nullRow : parent), ordinal);
        }
        rowCount++;
    }

    /** Finishes the writing of the current row batch. */
    public void finish() {
        root.setRowCount(rowCount);
        for (ArrowFieldWriter<IN> fieldWriter : fieldWriters) {
            fieldWriter.finish();
        }
    }

    /** Resets the state of the writer to write the next batch of rows. */
    public void reset() {
        root.setRowCount(0);
        rowCount = 0;
        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            fieldWriter.reset(batchCapacity);
        }
    }
}
