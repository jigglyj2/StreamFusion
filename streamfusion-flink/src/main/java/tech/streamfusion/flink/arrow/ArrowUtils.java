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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.streamfusion.flink.arrow.vectors.ArrowArrayColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowBigIntColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowBinaryColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowBooleanColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowDateColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowDecimalColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowDoubleColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowFloatColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowIntColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowMapColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowNullColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowRowColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowSmallIntColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowTimeColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowTimestampColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowTinyIntColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowVarBinaryColumnVector;
import tech.streamfusion.flink.arrow.vectors.ArrowVarCharColumnVector;
import tech.streamfusion.flink.arrow.writers.ArrayWriter;
import tech.streamfusion.flink.arrow.writers.ArrowFieldWriter;
import tech.streamfusion.flink.arrow.writers.BigIntWriter;
import tech.streamfusion.flink.arrow.writers.BinaryWriter;
import tech.streamfusion.flink.arrow.writers.BooleanWriter;
import tech.streamfusion.flink.arrow.writers.DateWriter;
import tech.streamfusion.flink.arrow.writers.DecimalWriter;
import tech.streamfusion.flink.arrow.writers.DoubleWriter;
import tech.streamfusion.flink.arrow.writers.FloatWriter;
import tech.streamfusion.flink.arrow.writers.IntWriter;
import tech.streamfusion.flink.arrow.writers.MapWriter;
import tech.streamfusion.flink.arrow.writers.NullWriter;
import tech.streamfusion.flink.arrow.writers.RowWriter;
import tech.streamfusion.flink.arrow.writers.SmallIntWriter;
import tech.streamfusion.flink.arrow.writers.TimeWriter;
import tech.streamfusion.flink.arrow.writers.TimestampWriter;
import tech.streamfusion.flink.arrow.writers.TinyIntWriter;
import tech.streamfusion.flink.arrow.writers.VarBinaryWriter;
import tech.streamfusion.flink.arrow.writers.VarCharWriter;

/** Utilities for Arrow. */
@Internal
public final class ArrowUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowUtils.class);

    public static void checkArrowUsable() {
        // Arrow requires the property io.netty.tryReflectionSetAccessible to
        // be set to true for JDK >= 9. Please refer to ARROW-5412 for more details.
        if (System.getProperty("io.netty.tryReflectionSetAccessible") == null) {
            System.setProperty("io.netty.tryReflectionSetAccessible", "true");
        } else if (!io.netty.util.internal.PlatformDependent.hasDirectBufferNoCleanerConstructor()) {
            throw new RuntimeException("Arrow depends on "
                    + "DirectByteBuffer.<init>(long, int) which is not available. Please set the "
                    + "system property 'io.netty.tryReflectionSetAccessible' to 'true'.");
        }
    }

    /** Returns the Arrow schema of the specified type. */
    public static Schema toArrowSchema(RowType rowType) {
        Collection<Field> fields = rowType.getFields().stream()
                .map(f -> ArrowUtils.toArrowField(f.getName(), f.getType()))
                .collect(Collectors.toCollection(ArrayList::new));
        return new Schema(fields);
    }

    private static Field toArrowField(String fieldName, LogicalType logicalType) {
        LogicalType physicalType = physicalType(logicalType);
        FieldType fieldType = new FieldType(
                logicalType.isNullable(), physicalType.accept(LogicalTypeToArrowTypeConverter.INSTANCE), null);
        List<Field> children = null;
        if (physicalType instanceof ArrayType) {
            children = Collections.singletonList(toArrowField("element", ((ArrayType) physicalType).getElementType()));
        } else if (physicalType instanceof RowType) {
            RowType rowType = (RowType) physicalType;
            children = new ArrayList<>(rowType.getFieldCount());
            for (RowType.RowField field : rowType.getFields()) {
                children.add(toArrowField(field.getName(), field.getType()));
            }
        } else if (physicalType instanceof MapType || physicalType instanceof MultisetType) {
            LogicalType keyType = mapKeyType(physicalType);
            LogicalType valueType = mapValueType(physicalType);
            Preconditions.checkArgument(!keyType.isNullable(), "Map and multiset key types should be non-nullable");
            children = Collections.singletonList(new Field(
                    "items",
                    new FieldType(false, ArrowType.Struct.INSTANCE, null),
                    Arrays.asList(toArrowField("key", keyType), toArrowField("value", valueType))));
        }
        return new Field(fieldName, fieldType, children);
    }

    private static LogicalType physicalType(LogicalType type) {
        while (type instanceof DistinctType) {
            type = ((DistinctType) type).getSourceType();
        }
        if (type instanceof StructuredType) {
            List<LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(type);
            List<String> fieldNames = LogicalTypeChecks.getFieldNames(type);
            return RowType.of(fieldTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));
        }
        return type;
    }

    private static LogicalType mapKeyType(LogicalType type) {
        return type instanceof MapType ? ((MapType) type).getKeyType() : ((MultisetType) type).getElementType();
    }

    private static LogicalType mapValueType(LogicalType type) {
        return type instanceof MapType ? ((MapType) type).getValueType() : new IntType(false);
    }

    /** Creates an {@link ArrowWriter} for the specified {@link VectorSchemaRoot}. */
    public static ArrowWriter<RowData> createRowDataArrowWriter(VectorSchemaRoot root, RowType rowType) {
        return createRowDataArrowWriter(root, rowType, 1024);
    }

    /** Creates a row writer whose vectors are sized once for the source-edge batch. */
    public static ArrowWriter<RowData> createRowDataArrowWriter(
            VectorSchemaRoot root, RowType rowType, int batchCapacity) {
        ArrowFieldWriter<RowData>[] fieldWriters =
                new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            setInitialCapacity(vector, batchCapacity);
            vector.allocateNew();
            fieldWriters[i] = createArrowFieldWriterForRow(vector, rowType.getTypeAt(i));
        }

        return new ArrowWriter<>(root, fieldWriters, batchCapacity);
    }

    private static void setInitialCapacity(FieldVector vector, int batchCapacity) {
        if (vector instanceof org.apache.arrow.vector.BaseVariableWidthVector) {
            ((org.apache.arrow.vector.BaseVariableWidthVector) vector).setInitialCapacity(batchCapacity, 32.0);
        } else {
            vector.setInitialCapacity(batchCapacity);
        }
        for (FieldVector child : vector.getChildrenFromFields()) {
            setInitialCapacity(child, batchCapacity);
        }
    }

    private static ArrowFieldWriter<RowData> createArrowFieldWriterForRow(ValueVector vector, LogicalType fieldType) {
        fieldType = physicalType(fieldType);
        if (vector instanceof TinyIntVector) {
            return TinyIntWriter.forRow((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return SmallIntWriter.forRow((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return IntWriter.forRow((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return BigIntWriter.forRow((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return BooleanWriter.forRow((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return FloatWriter.forRow((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return DoubleWriter.forRow((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return VarCharWriter.forRow((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return BinaryWriter.forRow((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return VarBinaryWriter.forRow((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return DecimalWriter.forRow(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return DateWriter.forRow((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return TimeWriter.forRow(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            int precision;
            if (fieldType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) fieldType).getPrecision();
            } else {
                precision = ((TimestampType) fieldType).getPrecision();
            }
            return TimestampWriter.forRow(vector, precision);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            LogicalType keyType = mapKeyType(fieldType);
            LogicalType valueType = mapValueType(fieldType);
            StructVector structVector = (StructVector) mapVector.getDataVector();
            return MapWriter.forRow(
                    mapVector,
                    createArrowFieldWriterForArray(structVector.getChild(MapVector.KEY_NAME), keyType),
                    createArrowFieldWriterForArray(structVector.getChild(MapVector.VALUE_NAME), valueType));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            LogicalType elementType = ((ArrayType) fieldType).getElementType();
            return ArrayWriter.forRow(
                    listVector, createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
        } else if (vector instanceof StructVector) {
            RowType rowType = (RowType) fieldType;
            ArrowFieldWriter<RowData>[] fieldsWriters = new ArrowFieldWriter[rowType.getFieldCount()];
            for (int i = 0; i < fieldsWriters.length; i++) {
                fieldsWriters[i] =
                        createArrowFieldWriterForRow(((StructVector) vector).getVectorById(i), rowType.getTypeAt(i));
            }
            return RowWriter.forRow((StructVector) vector, fieldsWriters);
        } else if (vector instanceof NullVector) {
            return new NullWriter<>((NullVector) vector);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type %s.", fieldType));
        }
    }

    private static ArrowFieldWriter<ArrayData> createArrowFieldWriterForArray(
            ValueVector vector, LogicalType fieldType) {
        fieldType = physicalType(fieldType);
        if (vector instanceof TinyIntVector) {
            return TinyIntWriter.forArray((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return SmallIntWriter.forArray((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return IntWriter.forArray((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return BigIntWriter.forArray((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return BooleanWriter.forArray((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return FloatWriter.forArray((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return DoubleWriter.forArray((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return VarCharWriter.forArray((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return BinaryWriter.forArray((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return VarBinaryWriter.forArray((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            DecimalVector decimalVector = (DecimalVector) vector;
            return DecimalWriter.forArray(decimalVector, getPrecision(decimalVector), decimalVector.getScale());
        } else if (vector instanceof DateDayVector) {
            return DateWriter.forArray((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return TimeWriter.forArray(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            int precision;
            if (fieldType instanceof LocalZonedTimestampType) {
                precision = ((LocalZonedTimestampType) fieldType).getPrecision();
            } else {
                precision = ((TimestampType) fieldType).getPrecision();
            }
            return TimestampWriter.forArray(vector, precision);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            LogicalType keyType = mapKeyType(fieldType);
            LogicalType valueType = mapValueType(fieldType);
            StructVector structVector = (StructVector) mapVector.getDataVector();
            return MapWriter.forArray(
                    mapVector,
                    createArrowFieldWriterForArray(structVector.getChild(MapVector.KEY_NAME), keyType),
                    createArrowFieldWriterForArray(structVector.getChild(MapVector.VALUE_NAME), valueType));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            LogicalType elementType = ((ArrayType) fieldType).getElementType();
            return ArrayWriter.forArray(
                    listVector, createArrowFieldWriterForArray(listVector.getDataVector(), elementType));
        } else if (vector instanceof StructVector) {
            RowType rowType = (RowType) fieldType;
            ArrowFieldWriter<RowData>[] fieldsWriters = new ArrowFieldWriter[rowType.getFieldCount()];
            for (int i = 0; i < fieldsWriters.length; i++) {
                fieldsWriters[i] =
                        createArrowFieldWriterForRow(((StructVector) vector).getVectorById(i), rowType.getTypeAt(i));
            }
            return RowWriter.forArray((StructVector) vector, fieldsWriters);
        } else if (vector instanceof NullVector) {
            return new NullWriter<>((NullVector) vector);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type %s.", fieldType));
        }
    }

    /** Creates an {@link ArrowReader} for the specified {@link VectorSchemaRoot}. */
    public static ArrowReader createArrowReader(VectorSchemaRoot root, RowType rowType) {
        List<ColumnVector> columnVectors = new ArrayList<>();
        List<FieldVector> fieldVectors = root.getFieldVectors();
        for (int i = 0; i < fieldVectors.size(); i++) {
            columnVectors.add(createColumnVector(fieldVectors.get(i), rowType.getTypeAt(i)));
        }

        return new ArrowReader(columnVectors.toArray(new ColumnVector[0]));
    }

    public static ColumnVector createColumnVector(ValueVector vector, LogicalType fieldType) {
        fieldType = physicalType(fieldType);
        if (vector instanceof TinyIntVector) {
            return new ArrowTinyIntColumnVector((TinyIntVector) vector);
        } else if (vector instanceof SmallIntVector) {
            return new ArrowSmallIntColumnVector((SmallIntVector) vector);
        } else if (vector instanceof IntVector) {
            return new ArrowIntColumnVector((IntVector) vector);
        } else if (vector instanceof BigIntVector) {
            return new ArrowBigIntColumnVector((BigIntVector) vector);
        } else if (vector instanceof BitVector) {
            return new ArrowBooleanColumnVector((BitVector) vector);
        } else if (vector instanceof Float4Vector) {
            return new ArrowFloatColumnVector((Float4Vector) vector);
        } else if (vector instanceof Float8Vector) {
            return new ArrowDoubleColumnVector((Float8Vector) vector);
        } else if (vector instanceof VarCharVector) {
            return new ArrowVarCharColumnVector((VarCharVector) vector);
        } else if (vector instanceof FixedSizeBinaryVector) {
            return new ArrowBinaryColumnVector((FixedSizeBinaryVector) vector);
        } else if (vector instanceof VarBinaryVector) {
            return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
        } else if (vector instanceof DecimalVector) {
            return new ArrowDecimalColumnVector((DecimalVector) vector);
        } else if (vector instanceof DateDayVector) {
            return new ArrowDateColumnVector((DateDayVector) vector);
        } else if (vector instanceof TimeSecVector
                || vector instanceof TimeMilliVector
                || vector instanceof TimeMicroVector
                || vector instanceof TimeNanoVector) {
            return new ArrowTimeColumnVector(vector);
        } else if (vector instanceof TimeStampVector
                && ((ArrowType.Timestamp) vector.getField().getType()).getTimezone() == null) {
            return new ArrowTimestampColumnVector(vector);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            LogicalType keyType = mapKeyType(fieldType);
            LogicalType valueType = mapValueType(fieldType);
            StructVector structVector = (StructVector) mapVector.getDataVector();
            return new ArrowMapColumnVector(
                    mapVector,
                    createColumnVector(structVector.getChild(MapVector.KEY_NAME), keyType),
                    createColumnVector(structVector.getChild(MapVector.VALUE_NAME), valueType));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            return new ArrowArrayColumnVector(
                    listVector,
                    createColumnVector(listVector.getDataVector(), ((ArrayType) fieldType).getElementType()));
        } else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector) vector;
            ColumnVector[] fieldColumns = new ColumnVector[structVector.size()];
            for (int i = 0; i < fieldColumns.length; ++i) {
                fieldColumns[i] = createColumnVector(structVector.getVectorById(i), ((RowType) fieldType).getTypeAt(i));
            }
            return new ArrowRowColumnVector(structVector, fieldColumns);
        } else if (vector instanceof NullVector) {
            return ArrowNullColumnVector.INSTANCE;
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type %s.", fieldType));
        }
    }

    private static class LogicalTypeToArrowTypeConverter extends LogicalTypeDefaultVisitor<ArrowType> {

        private static final LogicalTypeToArrowTypeConverter INSTANCE = new LogicalTypeToArrowTypeConverter();

        @Override
        public ArrowType visit(TinyIntType tinyIntType) {
            return new ArrowType.Int(8, true);
        }

        @Override
        public ArrowType visit(SmallIntType smallIntType) {
            return new ArrowType.Int(2 * 8, true);
        }

        @Override
        public ArrowType visit(IntType intType) {
            return new ArrowType.Int(4 * 8, true);
        }

        @Override
        public ArrowType visit(BigIntType bigIntType) {
            return new ArrowType.Int(8 * 8, true);
        }

        @Override
        public ArrowType visit(BooleanType booleanType) {
            return ArrowType.Bool.INSTANCE;
        }

        @Override
        public ArrowType visit(FloatType floatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }

        @Override
        public ArrowType visit(DoubleType doubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }

        @Override
        public ArrowType visit(CharType varCharType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(VarCharType varCharType) {
            return ArrowType.Utf8.INSTANCE;
        }

        @Override
        public ArrowType visit(BinaryType varCharType) {
            return new ArrowType.FixedSizeBinary(varCharType.getLength());
        }

        @Override
        public ArrowType visit(VarBinaryType varCharType) {
            return ArrowType.Binary.INSTANCE;
        }

        @Override
        public ArrowType visit(DecimalType decimalType) {
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale());
        }

        @Override
        public ArrowType visit(DateType dateType) {
            return new ArrowType.Date(DateUnit.DAY);
        }

        @Override
        public ArrowType visit(YearMonthIntervalType intervalType) {
            // Flink's internal representation is a signed month count.
            return new ArrowType.Int(32, true);
        }

        @Override
        public ArrowType visit(DayTimeIntervalType intervalType) {
            // Flink's internal representation is a signed millisecond count.
            return new ArrowType.Int(64, true);
        }

        @Override
        public ArrowType visit(TimeType timeType) {
            if (timeType.getPrecision() == 0) {
                return new ArrowType.Time(TimeUnit.SECOND, 32);
            } else if (timeType.getPrecision() >= 1 && timeType.getPrecision() <= 3) {
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            } else if (timeType.getPrecision() >= 4 && timeType.getPrecision() <= 6) {
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            } else {
                return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
            }
        }

        @Override
        public ArrowType visit(LocalZonedTimestampType localZonedTimestampType) {
            if (localZonedTimestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 1 && localZonedTimestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (localZonedTimestampType.getPrecision() >= 4 && localZonedTimestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(TimestampType timestampType) {
            if (timestampType.getPrecision() == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (timestampType.getPrecision() >= 1 && timestampType.getPrecision() <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (timestampType.getPrecision() >= 4 && timestampType.getPrecision() <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        }

        @Override
        public ArrowType visit(ArrayType arrayType) {
            return ArrowType.List.INSTANCE;
        }

        @Override
        public ArrowType visit(RowType rowType) {
            return ArrowType.Struct.INSTANCE;
        }

        @Override
        public ArrowType visit(MapType mapType) {
            return new ArrowType.Map(false);
        }

        @Override
        public ArrowType visit(MultisetType multisetType) {
            return new ArrowType.Map(false);
        }

        @Override
        public ArrowType visit(NullType nullType) {
            return ArrowType.Null.INSTANCE;
        }

        @Override
        protected ArrowType defaultMethod(LogicalType logicalType) {
            if (logicalType instanceof LegacyTypeInformationType) {
                Class<?> typeClass = ((LegacyTypeInformationType) logicalType)
                        .getTypeInformation()
                        .getTypeClass();
                if (typeClass == BigDecimal.class) {
                    // Because we can't get precision and scale from legacy BIG_DEC_TYPE_INFO,
                    // we set the precision and scale to default value compatible with python.
                    return new ArrowType.Decimal(38, 18);
                }
            }
            throw new UnsupportedOperationException(String.format(
                    "Python vectorized UDF doesn't support logical type %s currently.", logicalType.asSummaryString()));
        }
    }

    private static int getPrecision(DecimalVector decimalVector) {
        int precision = -1;
        try {
            java.lang.reflect.Field precisionField = decimalVector.getClass().getDeclaredField("precision");
            precisionField.setAccessible(true);
            precision = (int) precisionField.get(decimalVector);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // should not happen, ignore
        }
        return precision;
    }
}
