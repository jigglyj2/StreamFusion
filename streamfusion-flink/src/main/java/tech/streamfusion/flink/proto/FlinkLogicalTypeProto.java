/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.proto;

import java.util.List;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import tech.streamfusion.proto.plan.v1.CollectionType;
import tech.streamfusion.proto.plan.v1.DecimalType;
import tech.streamfusion.proto.plan.v1.EmptyType;
import tech.streamfusion.proto.plan.v1.LengthType;
import tech.streamfusion.proto.plan.v1.LogicalType;
import tech.streamfusion.proto.plan.v1.MapType;
import tech.streamfusion.proto.plan.v1.PrecisionType;
import tech.streamfusion.proto.plan.v1.RowField;

/** Shared, versioned mapping from Flink logical types to the native plan contract. */
public final class FlinkLogicalTypeProto {
    private FlinkLogicalTypeProto() {}

    public static LogicalType serialize(org.apache.flink.table.types.logical.LogicalType flinkType) {
        flinkType = physicalType(flinkType);
        LogicalType.Builder type = LogicalType.newBuilder().setNullable(flinkType.isNullable());
        LogicalTypeRoot root = flinkType.getTypeRoot();
        switch (root) {
            case TINYINT:
                return type.setTinyint(EmptyType.getDefaultInstance()).build();
            case SMALLINT:
                return type.setSmallint(EmptyType.getDefaultInstance()).build();
            case INTEGER:
                return type.setInteger(EmptyType.getDefaultInstance()).build();
            case BIGINT:
                return type.setBigint(EmptyType.getDefaultInstance()).build();
            case FLOAT:
                return type.setFloat(EmptyType.getDefaultInstance()).build();
            case DOUBLE:
                return type.setDouble(EmptyType.getDefaultInstance()).build();
            case BOOLEAN:
                return type.setBoolean(EmptyType.getDefaultInstance()).build();
            case CHAR:
                return type.setFixedChar(LengthType.newBuilder()
                                .setLength(((org.apache.flink.table.types.logical.CharType) flinkType).getLength()))
                        .build();
            case VARCHAR:
                return type.setVarchar(EmptyType.getDefaultInstance()).build();
            case BINARY:
                return type.setFixedBinary(LengthType.newBuilder()
                                .setLength(((org.apache.flink.table.types.logical.BinaryType) flinkType).getLength()))
                        .build();
            case VARBINARY:
                return type.setBinary(EmptyType.getDefaultInstance()).build();
            case DATE:
                return type.setDate(EmptyType.getDefaultInstance()).build();
            case TIME_WITHOUT_TIME_ZONE:
                return type.setTime(PrecisionType.newBuilder()
                                .setPrecision(
                                        ((org.apache.flink.table.types.logical.TimeType) flinkType).getPrecision()))
                        .build();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return type.setTimestamp(PrecisionType.newBuilder()
                                .setPrecision(((org.apache.flink.table.types.logical.TimestampType) flinkType)
                                        .getPrecision()))
                        .build();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return type.setTimestampLtz(PrecisionType.newBuilder()
                                .setPrecision(((org.apache.flink.table.types.logical.LocalZonedTimestampType) flinkType)
                                        .getPrecision()))
                        .build();
            case DECIMAL:
                org.apache.flink.table.types.logical.DecimalType decimal =
                        (org.apache.flink.table.types.logical.DecimalType) flinkType;
                return type.setDecimal(DecimalType.newBuilder()
                                .setPrecision(decimal.getPrecision())
                                .setScale(decimal.getScale()))
                        .build();
            case INTERVAL_YEAR_MONTH:
                return type.setInteger(EmptyType.getDefaultInstance()).build();
            case INTERVAL_DAY_TIME:
                return type.setBigint(EmptyType.getDefaultInstance()).build();
            case ARRAY:
                org.apache.flink.table.types.logical.ArrayType array =
                        (org.apache.flink.table.types.logical.ArrayType) flinkType;
                return type.setArray(CollectionType.newBuilder().setElementType(serialize(array.getElementType())))
                        .build();
            case MAP:
                org.apache.flink.table.types.logical.MapType map =
                        (org.apache.flink.table.types.logical.MapType) flinkType;
                return type.setMap(MapType.newBuilder()
                                .setKeyType(serialize(map.getKeyType()))
                                .setValueType(serialize(map.getValueType())))
                        .build();
            case MULTISET:
                org.apache.flink.table.types.logical.MultisetType multiset =
                        (org.apache.flink.table.types.logical.MultisetType) flinkType;
                return type.setMap(MapType.newBuilder()
                                .setKeyType(serialize(multiset.getElementType()))
                                .setValueType(serialize(new org.apache.flink.table.types.logical.IntType(false))))
                        .build();
            case ROW:
                org.apache.flink.table.types.logical.RowType row =
                        (org.apache.flink.table.types.logical.RowType) flinkType;
                tech.streamfusion.proto.plan.v1.RowType.Builder rowType =
                        tech.streamfusion.proto.plan.v1.RowType.newBuilder();
                for (org.apache.flink.table.types.logical.RowType.RowField field : row.getFields()) {
                    rowType.addFields(
                            RowField.newBuilder().setName(field.getName()).setType(serialize(field.getType())));
                }
                return type.setRow(rowType).build();
            default:
                throw new IllegalArgumentException("Unsupported native type " + flinkType);
        }
    }

    private static org.apache.flink.table.types.logical.LogicalType physicalType(
            org.apache.flink.table.types.logical.LogicalType type) {
        boolean nullable = type.isNullable();
        while (type instanceof DistinctType) {
            type = ((DistinctType) type).getSourceType();
        }
        if (type instanceof StructuredType) {
            List<org.apache.flink.table.types.logical.LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(type);
            List<String> fieldNames = LogicalTypeChecks.getFieldNames(type);
            type = RowType.of(
                    fieldTypes.toArray(new org.apache.flink.table.types.logical.LogicalType[0]),
                    fieldNames.toArray(new String[0]));
        }
        return type.copy(nullable);
    }
}
