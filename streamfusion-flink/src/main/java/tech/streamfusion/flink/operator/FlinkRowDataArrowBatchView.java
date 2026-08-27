/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.operator;

import org.apache.flink.table.data.RowData;

/**
 * Lightweight boundary view presenting a batch of Flink rows to the native Arrow pipeline.
 *
 * <p>Creating the view must not copy row payloads. The native boundary may materialize Arrow
 * buffers once when the upstream connector is row-based; subsequent native operators exchange
 * those Arrow buffers directly. A columnar source can instead provide an implementation backed by
 * existing Arrow-compatible buffers.
 */
public interface FlinkRowDataArrowBatchView extends StreamFusionPlanImplementation {
    int rowCount();

    RowData rowData(int ordinal);
}
