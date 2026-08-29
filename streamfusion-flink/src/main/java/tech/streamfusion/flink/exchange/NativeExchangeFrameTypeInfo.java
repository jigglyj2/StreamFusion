/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.exchange;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Flink type information that installs the stable native exchange frame serializer. */
public final class NativeExchangeFrameTypeInfo extends TypeInformation<NativeExchangeFrame> {
    public static final NativeExchangeFrameTypeInfo INSTANCE = new NativeExchangeFrameTypeInfo();

    private NativeExchangeFrameTypeInfo() {}

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
    public Class<NativeExchangeFrame> getTypeClass() {
        return NativeExchangeFrame.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<NativeExchangeFrame> createSerializer(SerializerConfig config) {
        return NativeExchangeFrameSerializer.INSTANCE;
    }

    @Override
    public boolean canEqual(Object object) {
        return object instanceof NativeExchangeFrameTypeInfo;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof NativeExchangeFrameTypeInfo;
    }

    @Override
    public int hashCode() {
        return NativeExchangeFrameTypeInfo.class.hashCode();
    }

    @Override
    public String toString() {
        return "StreamFusionArrowExchangeFrame";
    }
}
