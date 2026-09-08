/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.NativeRegionInputReference;
import tech.streamfusion.proto.plan.v1.NativeRegionPlan;

class NativeRegionPlanComposerTest {
    private static byte[] fixture() throws Exception {
        for (Path root = Path.of("").toAbsolutePath(); root != null; root = root.getParent()) {
            var path = root.resolve("streamfusion-proto/src/test/resources/native-region-v1.pb");
            if (Files.exists(path)) return Files.readAllBytes(path);
        }
        throw new IllegalStateException("Shared native region wire fixture is missing");
    }

    @Test
    void composesTheSameContractAsRustAndPreservesPhysicalIdentity() throws Exception {
        byte[] golden = fixture();
        var plan = NativeRegionPlan.parseFrom(golden);
        var fragments = new ArrayList<byte[]>();
        var inputs = new ArrayList<List<NativeRegionInputReference>>();
        for (var stage : plan.getStagesList()) {
            fragments.add(NativePlan.newBuilder()
                    .setProtocolVersion(3)
                    .setRoot(stage.getOperator())
                    .build()
                    .toByteArray());
            inputs.add(stage.getInputsList());
        }
        var encoded = NativeRegionPlanComposer.compose(1, fragments, inputs, plan.getOutputStageIdsList());
        assertThat(NativeRegionPlan.parseFrom(encoded)).isEqualTo(plan);
        assertThat(plan.getStagesCount()).isEqualTo(3);
        assertThat(plan.getStages(1).getInputs(0).getStageId()).isEqualTo(plan.getOutputStageIds(0));
        // Java protobuf treats the mismatched field wire type as unknown. Composition still
        // rejects it because a region cannot provide the required NativePlan root.
        assertThat(NativePlan.parseFrom(golden).hasRoot()).isFalse();
        assertThatThrownBy(() -> NativeRegionPlanComposer.compose(
                        1, List.of(golden), List.of(plan.getStages(0).getInputsList()), plan.getOutputStageIdsList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fragment protocol");
    }

    @Test
    void rejectsAmbiguousReferencesIdentitiesAndMalformedFragments() throws Exception {
        var plan = NativeRegionPlan.parseFrom(fixture());
        List<Consumer<NativeRegionPlan.Builder>> mutations = List.of(
                p -> p.setProtocolVersion(2),
                p -> p.setInputCount(0),
                p -> p.setInputCount(-1),
                p -> p.setInputCount(2),
                p -> p.getStagesBuilder(0).clearOperator(),
                p -> p.getStagesBuilder(0).getOperatorBuilder().setPlanNodeId(0),
                p -> p.getStagesBuilder(0).getOperatorBuilder().setPlanNodeId(-1),
                p -> p.getStagesBuilder(1).getOperatorBuilder().setPlanNodeId(4294967301L),
                p -> {
                    for (var stage : p.getStagesBuilderList())
                        stage.getOperatorBuilder().setMetricUid("duplicate");
                },
                p -> p.getStagesBuilder(0).getInputsBuilder(0).setStageId(4294967303L),
                p -> p.getStagesBuilder(1).getInputsBuilder(0).setStageId(4294967302L),
                p -> p.getStagesBuilder(1).getInputsBuilder(0).setExternalInput(0),
                p -> p.getStagesBuilder(0).getInputsBuilder(0).clearSource(),
                p -> p.getStagesBuilder(0).clearInputs(),
                p -> p.clearOutputStageIds().addOutputStageIds(4294967301L),
                p -> p.clearOutputStageIds().addOutputStageIds(4294967303L).addOutputStageIds(4294967303L),
                p -> p.clearOutputStageIds().addOutputStageIds(42),
                p -> p.clearOutputStageIds(),
                p -> p.getStagesBuilder(0)
                        .getOperatorBuilder()
                        .getCalcBuilder()
                        .getInputBuilder()
                        .setPlanNodeId(42),
                p -> p.getStagesBuilder(0)
                        .getOperatorBuilder()
                        .getCalcBuilder()
                        .getInputBuilder()
                        .getInputBuilder()
                        .setInputIndex(1),
                p -> p.getStagesBuilder(0)
                        .getOperatorBuilder()
                        .getCalcBuilder()
                        .setInput(plan.getStages(1).getOperator()));
        for (var mutation : mutations) {
            var candidate = plan.toBuilder();
            mutation.accept(candidate);
            assertThatThrownBy(() -> NativeRegionPlanComposer.validate(candidate.build()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void preservesDistinctExternalChannelsEvenForEqualFragments() throws Exception {
        var plan = NativeRegionPlan.parseFrom(fixture()).toBuilder().setInputCount(2);
        plan.getStagesBuilder(1).getInputsBuilder(0).setExternalInput(1);
        NativeRegionPlanComposer.validate(plan.build());
        assertThat(plan.getStages(0).getInputs(0).getExternalInput()).isZero();
        assertThat(plan.getStages(1).getInputs(0).getExternalInput()).isOne();
    }
}
