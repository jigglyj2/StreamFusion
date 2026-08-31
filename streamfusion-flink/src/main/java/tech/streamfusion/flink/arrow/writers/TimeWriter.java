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

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

/** {@link ArrowFieldWriter} for Time. */
@Internal
public abstract class TimeWriter<T> extends ArrowFieldWriter<T> {

    public static TimeWriter<RowData> forRow(ValueVector valueVector) {
        return new TimeWriterForRow(valueVector);
    }

    public static TimeWriter<ArrayData> forArray(ValueVector valueVector) {
        return new TimeWriterForArray(valueVector);
    }

    // ------------------------------------------------------------------------------------------

    private TimeWriter(ValueVector valueVector) {
        super(valueVector);
        Preconditions.checkState(valueVector instanceof TimeSecVector
                || valueVector instanceof TimeMilliVector
                || valueVector instanceof TimeMicroVector
                || valueVector instanceof TimeNanoVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract int readTime(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        ValueVector valueVector = getValueVector();
        if (isNullAt(in, ordinal)) {
            writeNull();
        } else if (valueVector instanceof TimeSecVector) {
            dataBuffer().setInt((long) getCount() * Integer.BYTES, readTime(in, ordinal) / 1000);
        } else if (valueVector instanceof TimeMilliVector) {
            dataBuffer().setInt((long) getCount() * Integer.BYTES, readTime(in, ordinal));
        } else if (valueVector instanceof TimeMicroVector) {
            dataBuffer().setLong((long) getCount() * Long.BYTES, readTime(in, ordinal) * 1000L);
        } else {
            dataBuffer().setLong((long) getCount() * Long.BYTES, readTime(in, ordinal) * 1000000L);
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link TimeWriter} for {@link RowData} input. */
    public static final class TimeWriterForRow extends TimeWriter<RowData> {

        private TimeWriterForRow(ValueVector valueVector) {
            super(valueVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readTime(RowData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }

    /** {@link TimeWriter} for {@link ArrayData} input. */
    public static final class TimeWriterForArray extends TimeWriter<ArrayData> {

        private TimeWriterForArray(ValueVector valueVector) {
            super(valueVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readTime(ArrayData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }
}
