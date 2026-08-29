/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.changelog;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

/** Drops only UPDATE_BEFORE while Flink continues forwarding all control events. */
final class StreamFusionDropUpdateBeforeOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {
    @Override
    public void processElement(StreamRecord<RowData> element) {
        if (element.getValue().getRowKind() != RowKind.UPDATE_BEFORE) {
            output.collect(element);
        }
    }
}
