/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.arrow;

import java.io.IOException;
import java.io.Serializable;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.format.RowCsvInputFormat;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

/**
 * Source boundary for Flink's legacy CSV lookup snapshot. Keep file listing, splitting and all CSV
 * parsing in Flink, and copy each source row directly into its owning Arrow batch. The task must drain
 * this finite source during operator open, before accepting probe records, just like CsvLookupFunction.
 */
@SuppressWarnings("deprecation")
public final class CsvLookupSnapshotSource implements Serializable {
    private static final long serialVersionUID = 1L;
    private final RowCsvInputFormat format;
    private final DataType dataType;
    private final DataType externalType;

    private CsvLookupSnapshotSource(RowCsvInputFormat format, DataType dataType, DataType externalType) {
        this.format = format;
        this.dataType = dataType;
        this.externalType = externalType;
    }

    /** Extracts the configured reader through the public source API; does not access the file. */
    public static CsvLookupSnapshotSource from(CsvTableSource source) throws Exception {
        if (source.getClass() != CsvTableSource.class) {
            throw new IllegalArgumentException(
                    "Custom CSV lookup implementations require separate semantic verification");
        }
        try (FormatCapture environment = new FormatCapture()) {
            source.getDataStream(environment);
            if (environment.format == null)
                throw new IllegalArgumentException("CSV source did not provide its input format");
            DataType dataType = source.getProducedDataType();
            DataType externalType = DataTypeUtils.transform(dataType, TypeTransformations.timeToSqlTypes());
            DataStructureConverters.getConverter(externalType);
            return new CsvLookupSnapshotSource(environment.format, dataType, externalType);
        }
    }

    public RowType rowType() {
        return (RowType) dataType.getLogicalType();
    }

    /** Each task/recovery gets a fresh reader. The caller supplies its Flink-accounted allocator. */
    public Cursor open(BufferAllocator allocator, ClassLoader classLoader, int batchRows) throws Exception {
        if (batchRows <= 0) throw new IllegalArgumentException("CSV snapshot batch size must be positive");
        RowCsvInputFormat reader = InstantiationUtil.clone(format, classLoader);
        return new Cursor(reader, allocator, dataType, externalType, classLoader, batchRows);
    }

    /** Uses an existing public extension point, without reflecting into Flink's private CSV config. */
    private static final class FormatCapture extends StreamExecutionEnvironment {
        private RowCsvInputFormat format;

        @Override
        public <T> DataStreamSource<T> createInput(InputFormat<T, ?> input, TypeInformation<T> type) {
            if (format != null || input.getClass() != RowCsvInputFormat.class) {
                throw new IllegalArgumentException("Expected exactly one Flink RowCsvInputFormat");
            }
            format = (RowCsvInputFormat) input;
            // Construct only the isolated source description so CsvTableSource can name it normally.
            // No job, input split or file read is created by this call.
            return super.createInput(input, type);
        }
    }

    /** Owns only the active Flink reader. Returned Arrow batches have independent buffer ownership. */
    public static final class Cursor implements AutoCloseable {
        private final RowCsvInputFormat reader;
        private final BufferAllocator allocator;
        private final RowType rowType;
        private final DataStructureConverter<Object, Object> converter;
        private final FileInputSplit[] splits;
        private final int batchRows;
        private int splitIndex;
        private Row reuse;
        private boolean splitOpen;
        private boolean closed;

        private Cursor(
                RowCsvInputFormat reader,
                BufferAllocator allocator,
                DataType dataType,
                DataType externalType,
                ClassLoader classLoader,
                int batchRows)
                throws IOException {
            this.reader = reader;
            this.allocator = allocator;
            this.rowType = (RowType) dataType.getLogicalType();
            // Legacy CSV returns java.sql temporal objects even for modern logical declarations.
            // Use the reader's actual external conversion classes, retaining the declared Arrow schema.
            this.converter = DataStructureConverters.getConverter(externalType);
            this.converter.open(classLoader);
            this.batchRows = batchRows;
            // Match CsvLookupFunction.open exactly: every task reads all splits with minNumSplits=1.
            this.splits = reader.createInputSplits(1);
        }

        /** Returns null at EOF. Closing this cursor never invalidates already returned batches. */
        public ArrowRowDataBatch nextBatch() throws IOException {
            if (closed) throw new IllegalStateException("CSV lookup snapshot is closed");
            if (!splitOpen && splitIndex == splits.length) return null;
            VectorSchemaRoot root = VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
            try {
                ArrowWriter<RowData> writer = ArrowUtils.createRowDataArrowWriter(root, rowType, batchRows);
                int count = 0;
                while (count < batchRows) {
                    if (!splitOpen) {
                        if (splitIndex == splits.length) break;
                        // Set first so failed opens also close any partially initialized file reader.
                        splitOpen = true;
                        reader.open(splits[splitIndex++]);
                        reuse = new Row(rowType.getFieldCount());
                    }
                    Row row = reader.nextRecord(reuse);
                    if (row == null) {
                        closeSplit();
                        continue;
                    }
                    writer.write((RowData) converter.toInternal(row));
                    count++;
                }
                if (count == 0) {
                    root.close();
                    return null;
                }
                writer.finish();
                return ArrowRowDataBatch.wrap(root, rowType, allocator);
            } catch (IOException | RuntimeException | Error failure) {
                try {
                    root.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                try {
                    close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }

        private void closeSplit() throws IOException {
            if (splitOpen) {
                splitOpen = false;
                reader.close();
                reuse = null;
            }
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                closeSplit();
            }
        }
    }
}
