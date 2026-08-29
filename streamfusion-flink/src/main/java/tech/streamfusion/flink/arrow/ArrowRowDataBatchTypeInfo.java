/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Type information for in-task Arrow batches between StreamFusion operators. */
public final class ArrowRowDataBatchTypeInfo extends TypeInformation<ArrowRowDataBatch> {
    public static final ArrowRowDataBatchTypeInfo INSTANCE = new ArrowRowDataBatchTypeInfo();

    private ArrowRowDataBatchTypeInfo() {}

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<ArrowRowDataBatch> getTypeClass() {
        return ArrowRowDataBatch.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<ArrowRowDataBatch> createSerializer(SerializerConfig config) {
        return ArrowRowDataBatchSerializer.INSTANCE;
    }

    @Override
    public boolean canEqual(Object object) {
        return object instanceof ArrowRowDataBatchTypeInfo;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof ArrowRowDataBatchTypeInfo;
    }

    @Override
    public int hashCode() {
        return ArrowRowDataBatchTypeInfo.class.hashCode();
    }

    @Override
    public String toString() {
        return "StreamFusionArrowBatch";
    }
}
