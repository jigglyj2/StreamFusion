/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Prevents a StreamFusion internal operator from silently becoming row-shaped again. */
class StreamFusionArrowArchitectureTest {
    @Test
    void aggregateAndWindowRuntimeDoNotLinkIsolatedPlannerClasses() throws IOException {
        for (String family : List.of("aggregate", "window", "over")) {
            try (var sources = Files.walk(Path.of("src/main/java/tech/streamfusion/flink", family))) {
                for (Path source : sources.filter(path -> path.toString().endsWith(".java"))
                        .collect(Collectors.toList())) {
                    assertThat(Files.readString(source))
                            .as("%s must receive a runtime/protobuf contract", source)
                            .doesNotContain("org.apache.calcite.", "org.apache.flink.table.planner.");
                }
            }
        }
    }

    @Test
    void multiInputRuntimeKeepsControlInFlinkAndPayloadInOneSharedNativeTree() throws Exception {
        String operator = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/operator/StreamFusionArrowNativeRegionOperator.java"));
        assertThat(operator)
                .contains(
                        "MultipleInputStreamOperator<ArrowRowDataBatch>",
                        "ArrowNativePlanDispatcher",
                        "NativeRegionControlTree",
                        "NativeRegionControlScheduler",
                        "memory.executionContext().controlCapabilities()",
                        "metricTree.bindGauges(",
                        "memory.executionContext().gaugeSchema()",
                        "getProcessingTimeService()::getCurrentProcessingTime",
                        "dispatcher.control(request, this::emitOutput)",
                        "dispatcher.processFrame(",
                        "StreamFusionNativeMetricTree.forRegion(")
                .doesNotContain(
                        "ArrowExchangeInputCDataBridge",
                        "NativeUnionBridge",
                        "NativeCalcBridge",
                        "rowView(",
                        ".transpose(",
                        "OperatorCase");
        assertThat(operator)
                .contains("NativeRegionStateLifecycle", "stateLifecycle.initialize(", "stateLifecycle.writeSnapshot(");
        String lifecycle = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/state/NativeRegionStateLifecycle.java"));
        assertThat(lifecycle)
                .contains("createWithState(", "NativeRegionStateParticipant", "registerNativeStateParticipant")
                .doesNotContain("NativeDeduplicateBridge", "NativeRegularJoinBridge", "executeArrow", ".transpose(");
        String controls = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/operator/NativeRegionControlTree.java"));
        assertThat(controls)
                .contains("IndexedCombinedWatermarkStatus", "NativePhysicalPlan.children(", "listener.inputWatermark(")
                .doesNotContain("OperatorCase.CALC", "OperatorCase.UNION", "NativeCalcBridge", "executeArrow");
        String scheduling = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/operator/NativeRegionControlScheduler.java"));
        assertThat(scheduling)
                .contains("NativeControlCapabilities.parseFrom", "tree.endInput(", "pending.values()")
                .doesNotContain("OperatorCase", "getGroupAggregate", "getDeduplicate", "rowView(", ".transpose(");
        String gauges =
                Files.readString(Path.of("src/main/java/tech/streamfusion/flink/metrics/NativeStageGauges.java"));
        assertThat(gauges)
                .contains("NativeGaugeSchema.parseFrom", "volatile long[]", "group.gauge(")
                .doesNotContain("OperatorCase", "getGroupAggregate", "bundleSize", "gaugeSnapshot(", "rowView(");
        String physical =
                Files.readString(Path.of("src/main/java/tech/streamfusion/flink/proto/NativePhysicalPlan.java"));
        assertThat(physical).contains("getAllFields()", "instanceof Operator").doesNotContain("OperatorCase");
    }

    @Test
    void runtimeArrivalsUseOneArityIndependentDispatcherAndNeverTransposeOrBufferRows() throws Exception {
        String dispatcher =
                Files.readString(Path.of("src/main/java/tech/streamfusion/flink/arrow/ArrowNativePlanDispatcher.java"));
        assertThat(dispatcher)
                .contains("bridge.executeStream(inputs)", "ArrowRowDataBatch.empty(", "stream.next()", "finally")
                .doesNotContain("NativeUnionBridge", "NativeCalcBridge", "OperatorCase", "rowView(", ".transpose(");
        String operator = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/operator/StreamFusionArrowNativeOperator.java"));
        assertThat(operator)
                .contains("ArrowNativePlanDispatcher", "nativeExecution.process(0, input, this::emitOutput)")
                .doesNotContain("NativeOutputStream", "nextWithSelection(", "executeArrowStream(");
        assertThat(operator)
                .contains(
                        "NativeRegionControlScheduler",
                        "controls.beforeCheckpoint(",
                        "controls.endInput(",
                        "nativeExecution.control(request, this::emitOutput)",
                        "metricTree.bindGauges(")
                .doesNotContain("OperatorCase", "NativeLocalGroupAggregateBridge", "getLocalGroupAggregate(");
    }

    @Test
    void singleAndMultiInputStreamsShareThePlanEdgeWithoutOperatorDispatch() throws Exception {
        String edge =
                Files.readString(Path.of("src/main/java/tech/streamfusion/flink/arrow/ArrowNativePlanBridge.java"));
        assertThat(edge)
                .contains(
                        "context.executeArrowStream(",
                        "ArrowCDataBridge.NativeOutputStream",
                        "context.requiresInputEnvelope()")
                .doesNotContain("context.hasStateBindings()")
                .doesNotContain("NativeUnionBridge", "NativeCalcBridge", "rowView(", "ArrowRowDataBatch.transpose(");
        String single = Files.readString(Path.of("src/main/java/tech/streamfusion/flink/arrow/ArrowCDataBridge.java"));
        assertThat(single)
                .contains("streamExecution.executeStream(")
                .doesNotContain("NativeCalcBridge.executeArrowStream(");
        String nativeEdge = Files.readString(Path.of("../streamfusion-native/src/jni_bridge/plan_stream.rs"));
        assertThat(nativeEdge)
                .contains("export_plan_stream(", "context.start_control(batches, events)?", "context.start(batches)?")
                .doesNotContain("concat_batches", "collect(plan", "operator::Operator::");
        String exporter = Files.readString(Path.of("../streamfusion-native/src/jni_bridge/common.rs"));
        assertThat(exporter).contains("c_stream::export(").doesNotContain("FFI_ArrowArrayStream::new(");
        String importer = exporter.substring(
                exporter.indexOf("unsafe fn import_input("), exporter.indexOf("fn input_ordinal_end("));
        assertThat(importer)
                .contains("reservation.try_grow(", "import_record_batch_with_schema(", "prepare_input(")
                .doesNotContain("descriptor_bytes(");
        assertThat(importer.indexOf("reservation.try_grow("))
                .isLessThan(importer.indexOf("import_record_batch_with_schema("));
        String stream = Files.readString(Path.of("../streamfusion-native/src/memory_pool/c_stream.rs"));
        assertThat(stream)
                .contains("super::c_data::array(", "super::c_data::schema(", "reader.next()")
                .doesNotContain("operator::Operator::", "jni::", "transpose", "concat_batches");
    }

    @Test
    void plannerCollectsFragmentsWithoutCalcCorrelateCombinationDrivers() throws IOException {
        Path planner = Path.of("../streamfusion-flink-planner/src/main/java/tech/streamfusion/flink/planner");
        String region = Files.readString(planner.resolve("StreamFusionStatelessRegion.java"));
        assertThat(region)
                .contains("instanceof StreamFusionNativePlanNode", ".nativePlanFragment(planner)")
                .doesNotContain(
                        "instanceof StreamFusionExecCalc",
                        "instanceof StreamFusionExecExpand",
                        "instanceof StreamFusionExecArrayUnnest");
        for (String name : List.of(
                "StreamFusionExecCalc",
                "StreamFusionBatchExecCalc",
                "StreamFusionExecArrayUnnest",
                "StreamFusionBatchExecArrayUnnest",
                "StreamFusionExecReplicateRows",
                "StreamFusionExecDeduplicate",
                "StreamFusionExecRegularJoin",
                "StreamFusionExecUnion",
                "StreamFusionBatchExecUnion")) {
            assertThat(Files.readString(planner.resolve(name + ".java")))
                    .contains("nativePlanFragment", "StreamFusionStatelessRegion.translate(this, planner)")
                    .doesNotContain(
                            "translateCalcArrayUnnestCalcChains", "fusedUnnests", "fusedReplicate", "boundaryCalcs");
        }
        for (String name : List.of("StreamFusionCalcTranslator", "StreamFusionCalcPlan")) {
            assertThat(Files.readString(Path.of("src/main/java/tech/streamfusion/flink/calc/" + name + ".java")))
                    .doesNotContain(
                            "createFusedCalcUnnest",
                            "createFusedReplicateRows",
                            "translateCalcArrayUnnestCalcChains",
                            "translateReplicateRowsChain");
        }
        assertThat(region)
                .contains("ownsNativeKeyedState", "translateKeyedInputs")
                .doesNotContain("StreamFusionNativeRegionOwner", "translateNativeRegion(");
        assertThat(Files.readString(planner.resolve("StreamFusionExecCalc.java")))
                .doesNotContain("StreamFusionExecRegularJoin", "translateWithOutputCalcs");
        assertThat(Files.readString(planner.resolve("StreamFusionExecRegularJoin.java")))
                .contains("StreamFusionNativePlanNode", "ownsNativeKeyedState")
                .doesNotContain("StreamFusionExecCalc", "translateWithOutputCalcs", "translateWithNativeOutput");
    }

    @Test
    void regularJoinUsesPullBasedArrowStreamWithoutAnIntermediateRowPath() throws IOException {
        String operator = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/join/StreamFusionArrowRegularJoinOperator.java"));
        String stream = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/arrow/ArrowRegularJoinOutputStream.java"));
        assertThat(operator)
                .contains("ArrowRegularJoinOutputStream", "stream.next()")
                .doesNotContain("ArrowRegularJoinCDataBridge.execute(", "ArrowRowDataBatch.transpose(");
        assertThat(stream)
                .containsOnlyOnce("NativeRegularJoinBridge.processStream(")
                .contains("stream.getNext(array)", "input.transportRoot()")
                .doesNotContain("rowView(", "ArrowRowDataBatch.transpose(");
        String nativeBridge = Files.readString(Path.of("../streamfusion-native/src/jni_bridge/regular_join_stream.rs"));
        String nativeRegion =
                Files.readString(Path.of("../streamfusion-native/src/planner/operators/regular_join/region.rs"));
        assertThat(nativeBridge)
                .contains("region.start(side, input)")
                .doesNotContain("next_streaming_batch", "begin_streaming_batch");
        assertThat(nativeRegion)
                .contains("RegularJoinFactory", "context.bind_persistent", "self.context.start(inputs)")
                .doesNotContain(
                        "RuntimeEnvBuilder",
                        "Builder::new_current_thread",
                        "FusedCalcPipeline",
                        "block_on(collect",
                        "physical_plan::collect",
                        "try_collect");
    }

    @Test
    void persistentFusionUsesSharedLoweringAndExecutionInsteadOfOperatorPairDrivers() throws IOException {
        String context = Files.readString(Path.of("../streamfusion-native/src/execution_context.rs"));
        String driver = Files.readString(Path.of("../streamfusion-native/src/execution_context/stream.rs"));
        String planner = Files.readString(Path.of("../streamfusion-native/src/planner/mod.rs"));
        String dedup = Files.readString(Path.of("../streamfusion-native/src/planner/operators/deduplicate/region.rs"));
        assertThat(context).contains("create_plan_with_memory", "&self.persistent", "Some(self.memory_pool.clone())");
        assertThat(planner).contains("factory.build(operator, children)", "resources.memory.clone()");
        assertThat(driver)
                .contains("plan.execute(0, self.task_context())")
                .doesNotContain("RegularJoin", "Deduplicate", "proto::operator");
        assertThat(dedup)
                .contains("context.bind_persistent", "execute_single_batch")
                .doesNotContain("RuntimeEnvBuilder", "DeduplicateRegion", "Operator::Calc");
    }

    @Test
    void filterAdmissionKeepsTheCachedDataFusionKernelAndArrowStream() throws IOException {
        String calc = Files.readString(Path.of("../streamfusion-native/src/planner/operators/calc.rs"));
        String filter = Files.readString(Path.of("../streamfusion-native/src/planner/operators/managed_filter.rs"));
        String stream =
                Files.readString(Path.of("../streamfusion-native/src/planner/operators/managed_filter/stream.rs"));
        assertThat(calc).contains("ManagedFilterExec::wrap", "ProjectionExec::try_new");
        assertThat(filter)
                .contains("filter: FilterExec", "self.filter.execute(partition, context)", "self.filter.metrics()");
        assertThat(stream)
                .contains(
                        "RecordBatchStream",
                        "native filter gather workspace",
                        "datafusion_batch_registered(batch, memory, registry)");
        assertThat(filter + stream).doesNotContain("jni::", "RowData", "filter_record_batch(", "create_plan(");
    }

    @Test
    void rowDataExistsOnlyAtTheTwoExplicitRuntimeBoundaries() throws IOException {
        Path sources = Path.of("src/main/java/tech/streamfusion/flink");
        try (var files = Files.walk(sources)) {
            List<Path> javaFiles =
                    files.filter(path -> path.toString().endsWith(".java")).collect(Collectors.toList());
            for (Path file : javaFiles) {
                String source = Files.readString(file);
                String name = file.getFileName().toString();
                if (!name.equals("RowDataToArrowBatchOperator.java")) {
                    assertThat(source)
                            .as("RowData input is forbidden in %s", file)
                            .doesNotContain("OneInputStreamOperator<RowData,");
                }
                if (!name.equals("ArrowBatchToRowDataOperator.java")) {
                    assertThat(source)
                            .as("RowData output is forbidden in %s", file)
                            .doesNotContain("AbstractStreamOperator<RowData>");
                }
                assertThat(source)
                        .as("RowData multi-input is forbidden in %s", file)
                        .doesNotContain("MultipleInputStreamOperator<RowData>");
                if (!name.equals("ArrowRowDataBatch.java")) {
                    assertThat(source)
                            .as("operator-local RowData transpose is forbidden in %s", file)
                            .doesNotContain("ArrowRowDataBatch.transpose(");
                }
            }
        }
    }

    @Test
    void networkExchangeUsesArrowIpcRatherThanAProprietaryRowEncoding() throws IOException {
        String frame =
                Files.readString(Path.of("src/main/java/tech/streamfusion/flink/exchange/NativeExchangeFrame.java"));
        String serializer = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/exchange/NativeExchangeFrameSerializer.java"));
        String plan = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/exchange/NativeExchangePlanSerializer.java"));

        assertThat(frame).contains("Arrow IPC frame").doesNotContain("RowData");
        assertThat(serializer).contains("Arrow IPC frame").doesNotContain("RowDataSerializer");
        assertThat(plan).contains("EXCHANGE_TRANSPORT_ARROW_IPC_STREAM");
    }

    @Test
    void topNIsOneArrowBatchCallWithoutJavaStateOrRowAlgorithms() throws IOException {
        String operator = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/topn/StreamFusionArrowTopNOperator.java"));
        String bridge =
                Files.readString(Path.of("src/main/java/tech/streamfusion/flink/arrow/ArrowTopNCDataBridge.java"));

        assertThat(operator)
                .contains("OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>")
                .doesNotContain("GeneratedRecordComparator")
                .doesNotContain("GeneratedRecordEqualiser")
                .doesNotContain("ArrowRowDataBatch.transpose(");
        assertThat(bridge)
                .containsOnlyOnce("NativeTopNBridge.process(")
                .doesNotContain("loadGroups")
                .doesNotContain("commitGroups")
                .doesNotContain("ArrowRowDataBatch.transpose(");
    }

    @Test
    void overAggregationIsOneArrowBatchCallWithoutJavaStateOrRowAlgorithms() throws IOException {
        String operator = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/over/StreamFusionArrowOverAggregateOperator.java"));
        String bridge = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/arrow/ArrowOverAggregateCDataBridge.java"));

        assertThat(operator)
                .contains("OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>")
                .doesNotContain("GeneratedRecordComparator")
                .doesNotContain("GeneratedRecordEqualiser")
                .doesNotContain("ArrowRowDataBatch.transpose(");
        assertThat(bridge)
                .containsOnlyOnce("NativeOverAggregateBridge.process(")
                .doesNotContain("loadGroups")
                .doesNotContain("commitGroups")
                .doesNotContain("ArrowRowDataBatch.transpose(");
    }

    @Test
    void temporalSortKeepsStateSortingAndTimerDrainingBehindArrowBatchCalls() throws IOException {
        String operator = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/sort/StreamFusionArrowTemporalSortOperator.java"));
        String bridge = Files.readString(
                Path.of("src/main/java/tech/streamfusion/flink/arrow/ArrowTemporalSortCDataBridge.java"));

        assertThat(operator)
                .contains("OneInputStreamOperator<ArrowRowDataBatch, ArrowRowDataBatch>")
                .doesNotContain("GeneratedRecordComparator")
                .doesNotContain("GeneratedRecordEqualiser")
                .doesNotContain("ArrowRowDataBatch.transpose(");
        assertThat(bridge)
                .containsOnlyOnce("NativeTemporalSortBridge.process(")
                .doesNotContain("loadGroups")
                .doesNotContain("commitGroups")
                .doesNotContain("ArrowRowDataBatch.transpose(");
    }
}
