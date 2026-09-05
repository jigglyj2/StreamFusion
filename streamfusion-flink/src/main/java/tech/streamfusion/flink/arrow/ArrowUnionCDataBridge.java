/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.types.logical.RowType;
import tech.streamfusion.nativebridge.NativeExecutionContext;
import tech.streamfusion.nativebridge.NativeMemoryManager;
import tech.streamfusion.nativebridge.NativeUnionBridge;

/** Ownership-safe Arrow C Data transfer for one native multi-input UNION ALL batch. */
public final class ArrowUnionCDataBridge {
    private ArrowUnionCDataBridge() {}

    public static NativeCalcResult executeWithSelection(
            byte[] serializedPlan,
            List<ArrowRowDataBatch> inputs,
            RowType outputType,
            BufferAllocator allocator,
            NativeMemoryManager memoryManager) {
        return executeWithSelection(
                inputs,
                outputType,
                allocator,
                (arrays, schemas, outputArray, outputSchema) -> NativeUnionBridge.executeArrow(
                        serializedPlan, arrays, schemas, outputArray, outputSchema, memoryManager));
    }

    public static NativeCalcResult executeWithSelection(
            NativeExecutionContext context,
            List<ArrowRowDataBatch> inputs,
            RowType outputType,
            BufferAllocator allocator) {
        return executeWithSelection(
                inputs,
                outputType,
                allocator,
                (arrays, schemas, outputArray, outputSchema) ->
                        NativeUnionBridge.executeArrow(context, arrays, schemas, outputArray, outputSchema));
    }

    private static NativeCalcResult executeWithSelection(
            List<ArrowRowDataBatch> inputs,
            RowType outputType,
            BufferAllocator allocator,
            NativeUnionInvocation invocation) {
        if (inputs.size() < 2) {
            throw new IllegalArgumentException("Native UNION ALL requires at least two inputs");
        }
        List<ArrowArray> inputArrays = new ArrayList<>(inputs.size());
        List<ArrowSchema> inputSchemas = new ArrayList<>(inputs.size());
        try (ArrowArray outputArray = ArrowArray.allocateNew(allocator);
                ArrowSchema outputSchema = ArrowSchema.allocateNew(allocator);
                CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
            long[] arrayAddresses = new long[inputs.size()];
            long[] schemaAddresses = new long[inputs.size()];
            for (int index = 0; index < inputs.size(); index++) {
                BufferAllocator inputAllocator = inputs.get(index).allocator();
                ArrowArray inputArray = ArrowArray.allocateNew(inputAllocator);
                ArrowSchema inputSchema = ArrowSchema.allocateNew(inputAllocator);
                inputArrays.add(inputArray);
                inputSchemas.add(inputSchema);
                Data.exportVectorSchemaRoot(inputAllocator, inputs.get(index).root(), null, inputArray, inputSchema);
                arrayAddresses[index] = inputArray.memoryAddress();
                schemaAddresses[index] = inputSchema.memoryAddress();
            }
            long rowCount = invocation.execute(
                    arrayAddresses, schemaAddresses, outputArray.memoryAddress(), outputSchema.memoryAddress());
            if (rowCount < 0 || rowCount > Integer.MAX_VALUE) {
                throw new IllegalStateException("Native UNION ALL returned invalid row count " + rowCount);
            }
            VectorSchemaRoot output = Data.importVectorSchemaRoot(allocator, outputArray, outputSchema, dictionaries);
            output.setRowCount((int) rowCount);
            return removeSelection(output, outputType, allocator);
        } finally {
            inputArrays.forEach(ArrowArray::close);
            inputSchemas.forEach(ArrowSchema::close);
        }
    }

    private static NativeCalcResult removeSelection(
            VectorSchemaRoot output, RowType outputType, BufferAllocator allocator) {
        int ordinalIndex = output.getFieldVectors().size() - 1;
        FieldVector ordinalVector = output.getVector(ordinalIndex);
        if (!(ordinalVector instanceof IntVector)) {
            output.close();
            throw new IllegalStateException("Native UNION ALL did not return its INT input-row ordinal");
        }
        int[] inputRows = new int[output.getRowCount()];
        IntVector ordinals = (IntVector) ordinalVector;
        for (int index = 0; index < inputRows.length; index++) {
            inputRows[index] = ordinals.get(index);
        }
        VectorSchemaRoot visibleOutput = output.removeVector(ordinalIndex);
        ordinalVector.close();
        return new NativeCalcResult(ArrowRowDataBatch.wrap(visibleOutput, outputType, allocator), inputRows);
    }

    @FunctionalInterface
    private interface NativeUnionInvocation {
        long execute(long[] inputArrays, long[] inputSchemas, long outputArray, long outputSchema);
    }
}
