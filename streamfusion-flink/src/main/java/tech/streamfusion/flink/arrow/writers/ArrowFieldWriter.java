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

package tech.streamfusion.flink.arrow.writers;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/**
 * Base class for arrow field writer which is used to convert a field to an Arrow format.
 *
 * @param <IN> Type of the row to write.
 */
@Internal
public abstract class ArrowFieldWriter<IN> {

    /** Container which is used to store the written sequence of values of a column. */
    private final ValueVector valueVector;

    /** Cached buffer views avoid Arrow's reference-count lookup on every field write. */
    private ArrowBuf dataBuffer;

    private ArrowBuf validityBuffer;
    private ArrowBuf offsetBuffer;

    /** The current count of elements written. */
    private int count = 0;

    /** Cached capacity avoids Arrow's checked growth path for every value. */
    private int valueCapacity;

    public ArrowFieldWriter(ValueVector valueVector) {
        this.valueVector = Preconditions.checkNotNull(valueVector);
        this.valueCapacity = valueVector.getValueCapacity();
        refreshBuffers();
    }

    /** Returns the underlying container which stores the sequence of values of a column. */
    public ValueVector getValueVector() {
        return valueVector;
    }

    /** Returns the current count of elements written. */
    public int getCount() {
        return count;
    }

    protected final ArrowBuf dataBuffer() {
        return dataBuffer;
    }

    protected final ArrowBuf offsetBuffer() {
        return offsetBuffer;
    }

    /** Clears the current validity bit; reset pre-initializes every in-capacity bit as valid. */
    protected final void writeNull() {
        org.apache.arrow.vector.BitVectorHelper.unsetBit(validityBuffer, count);
    }

    /** Sets the field value as the field at the specified ordinal of the specified row. */
    public abstract void doWrite(IN row, int ordinal);

    /** Writes the specified ordinal of the specified row. */
    public void write(IN row, int ordinal) {
        if (count >= valueCapacity && !(valueVector instanceof NullVector)) {
            growValueCapacity();
        }
        doWrite(row, ordinal);
        count += 1;
    }

    /** Finishes the writing of the current row batch. */
    public void finish() {
        valueVector.setValueCount(count);
    }

    /**
     * Resets logical writer state without clearing reusable data buffers.
     *
     * <p>Every writer overwrites the validity bit for each logical value. Initializing the tiny
     * validity bitmap to all-valid lets non-null fast paths write only their data while null paths
     * clear the corresponding bit.
     */
    public void reset(int batchCapacity) {
        valueVector.setValueCount(0);
        count = 0;
        valueCapacity = valueVector.getValueCapacity();
        resetOffsets();
        initializeValidity(valueCapacity);
    }

    private void growValueCapacity() {
        int previousCapacity = valueCapacity;
        do {
            valueVector.reAlloc();
            valueCapacity = valueVector.getValueCapacity();
        } while (count >= valueCapacity);
        refreshBuffers();
        initializeValidity(previousCapacity, valueCapacity);
        onVectorReallocated();
    }

    /** Refreshes writer-specific cached buffer views after Arrow grows a vector. */
    protected void onVectorReallocated() {}

    private void resetOffsets() {
        if (valueVector instanceof BaseVariableWidthVector) {
            BaseVariableWidthVector variableWidthVector = (BaseVariableWidthVector) valueVector;
            variableWidthVector.setLastSet(-1);
            variableWidthVector.getOffsetBuffer().setInt(0, 0);
        } else if (valueVector instanceof ListVector) {
            ListVector listVector = (ListVector) valueVector;
            listVector.setLastSet(-1);
            listVector.getOffsetBuffer().setInt(0, 0);
        }
    }

    private void initializeValidity(int values) {
        initializeValidity(0, values);
    }

    private void initializeValidity(int fromValue, int toValue) {
        if (toValue <= fromValue) {
            return;
        }
        ArrowBuf validity = validityBuffer;
        if (validity == null) {
            return;
        }
        int value = fromValue;
        while (value < toValue && (value & 7) != 0) {
            org.apache.arrow.vector.BitVectorHelper.setBit(validity, value++);
        }
        long firstByte = value / 8L;
        long byteCount = (toValue - value) / 8L;
        if (byteCount > 0) {
            validity.setOne(firstByte, byteCount);
            value += Math.toIntExact(byteCount * 8L);
        }
        while (value < toValue) {
            org.apache.arrow.vector.BitVectorHelper.setBit(validity, value++);
        }
    }

    private void refreshBuffers() {
        dataBuffer = null;
        validityBuffer = null;
        offsetBuffer = null;
        if (valueVector instanceof BaseFixedWidthVector) {
            BaseFixedWidthVector vector = (BaseFixedWidthVector) valueVector;
            dataBuffer = vector.getDataBuffer();
            validityBuffer = vector.getValidityBuffer();
        } else if (valueVector instanceof BaseVariableWidthVector) {
            BaseVariableWidthVector vector = (BaseVariableWidthVector) valueVector;
            dataBuffer = vector.getDataBuffer();
            validityBuffer = vector.getValidityBuffer();
            offsetBuffer = vector.getOffsetBuffer();
        } else if (valueVector instanceof ListVector) {
            ListVector vector = (ListVector) valueVector;
            validityBuffer = vector.getValidityBuffer();
            offsetBuffer = vector.getOffsetBuffer();
        } else if (valueVector instanceof StructVector) {
            validityBuffer = ((StructVector) valueVector).getValidityBuffer();
        }
    }
}
