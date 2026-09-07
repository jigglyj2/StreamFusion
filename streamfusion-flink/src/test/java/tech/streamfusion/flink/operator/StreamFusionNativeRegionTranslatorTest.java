/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.List;
import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.RegularJoin;

class StreamFusionNativeRegionTranslatorTest {
    @Test
    void bindsEveryProtocolShapeByInputSlotIncludingRepeatedAndReorderedChildren() throws Exception {
        int tested = 0;
        for (FieldDescriptor payload : Operator.getDescriptor().getFields()) {
            if (payload.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || payload.getName().equals("input")) {
                continue;
            }
            Message.Builder node = Operator.newBuilder().newBuilderForField(payload);
            List<FieldDescriptor> childFields = node.getDescriptorForType().getFields().stream()
                    .filter(field -> field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                            && field.getMessageType().equals(Operator.getDescriptor()))
                    .collect(java.util.stream.Collectors.toList());
            int count = childFields.stream()
                    .mapToInt(field -> field.isRepeated() ? 3 : 1)
                    .sum();
            int slot = count - 1;
            for (FieldDescriptor field : childFields) {
                for (int index = 0; index < (field.isRepeated() ? 3 : 1); index++) {
                    Operator input = Operator.newBuilder()
                            .setInput(Input.newBuilder().setInputIndex(slot--))
                            .build();
                    if (field.isRepeated()) {
                        node.addRepeatedField(field, input);
                    } else {
                        node.setField(field, input);
                    }
                }
            }
            Operator fragment =
                    Operator.newBuilder().setField(payload, node.build()).build();
            java.util.ArrayList<byte[]> inputs = new java.util.ArrayList<>();
            for (int index = 0; index < count; index++) {
                inputs.add(plan(FIRST.toBuilder().setPlanNodeId(100 + index).build()));
            }
            String metricName = "original Flink " + payload.getName() + "[59]";
            byte[] identified = StreamFusionNativeRegionTranslator.identifyStage(plan(fragment), 59, metricName);
            NativePlan result =
                    NativePlan.parseFrom(StreamFusionNativeRegionTranslator.composeWithInputs(identified, inputs));
            assertThat(result.getRoot().getPlanNodeId()).isEqualTo((1L << 32) | 59);
            assertThat(result.getRoot().getMetricName()).isEqualTo(metricName);
            // Control/metric traversal must stop at these direct physical children, not recurse
            // into their synthetic input leaves or count schema/expression messages as stages.
            assertThat(tech.streamfusion.flink.proto.NativePhysicalPlan.children(result.getRoot()))
                    .hasSize(count)
                    .allSatisfy(child -> assertThat(child.hasCalc()).isTrue());
            Message composed = (Message) result.getRoot().getField(payload);
            slot = count - 1;
            for (FieldDescriptor field : childFields) {
                for (int index = 0; index < (field.isRepeated() ? 3 : 1); index++) {
                    Operator child = (Operator)
                            (field.isRepeated() ? composed.getRepeatedField(field, index) : composed.getField(field));
                    assertThat(child)
                            .isEqualTo(NativePlan.parseFrom(inputs.get(slot--)).getRoot());
                }
            }
            tested++;
        }
        assertThat(tested).isGreaterThan(20);
    }

    @Test
    void rejectsUnboundDuplicateAndNonFragmentInputs() {
        Operator duplicate = Operator.newBuilder()
                .setRegularJoin(RegularJoin.newBuilder().setLeftInput(EDGE).setRightInput(EDGE))
                .build();
        assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.composeWithInputs(
                        plan(duplicate), List.of(plan(FIRST), plan(FIRST))))
                .hasMessageContaining("unique and contiguous");
        assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.composeWithInputs(plan(FIRST), List.of()))
                .hasMessageContaining("arity");
        Operator nested =
                Operator.newBuilder().setCalc(Calc.newBuilder().setInput(FIRST)).build();
        assertThatThrownBy(
                        () -> StreamFusionNativeRegionTranslator.composeWithInputs(plan(nested), List.of(plan(FIRST))))
                .hasMessageContaining("external Input slots");
    }

    @Test
    void physicalIdentitiesSurviveCompositionAndRegionGrowth() throws Exception {
        byte[] first = StreamFusionNativeRegionTranslator.identifyStage(plan(FIRST), 27, "Calc[27]", "27_calc");
        byte[] last = StreamFusionNativeRegionTranslator.identifyStage(plan(FIRST), 43, "Calc(select=[résultat])");
        NativePlan single = NativePlan.parseFrom(StreamFusionNativeRegionTranslator.compose(List.of(first)));
        NativePlan grown = NativePlan.parseFrom(StreamFusionNativeRegionTranslator.compose(List.of(first, last)));
        assertThat(single.getRoot().getPlanNodeId()).isEqualTo((1L << 32) | 27);
        assertThat(grown.getRoot().getPlanNodeId()).isEqualTo((1L << 32) | 43);
        assertThat(grown.getRoot().getMetricName()).isEqualTo("Calc(select=[résultat])");
        assertThat(grown.getRoot().getCalc().getInput().getMetricName()).isEqualTo("Calc[27]");
        assertThat(grown.getRoot().hasMetricUid()).isFalse();
        assertThat(grown.getRoot().getCalc().getInput().getMetricUid()).isEqualTo("27_calc");
        assertThat(NativePlan.parseFrom(StreamFusionNativeRegionTranslator.identifyStage(plan(FIRST), 1, "Calc", ""))
                        .getRoot()
                        .hasMetricUid())
                .isTrue();
        assertThat(grown.getRoot().getCalc().getInput()).isEqualTo(single.getRoot());
        assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.identifyStage(plan(FIRST), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void composesAboveAnySubtreeAndPreservesTheHighestRequiredProtocol() throws Exception {
        Operator join = Operator.newBuilder()
                .setRegularJoin(RegularJoin.newBuilder()
                        .setLeftInput(EDGE)
                        .setRightInput(Operator.newBuilder()
                                .setInput(Input.newBuilder().setInputIndex(1))))
                .build();
        byte[] tail = NativePlan.newBuilder()
                .setProtocolVersion(2)
                .setRoot(Operator.newBuilder()
                        .setCalc(Calc.newBuilder().setInput(EDGE).setPreserveInputEnvelope(true)))
                .build()
                .toByteArray();
        NativePlan result =
                NativePlan.parseFrom(StreamFusionNativeRegionTranslator.composeAbove(plan(join), List.of(tail)));
        assertThat(result.getProtocolVersion()).isEqualTo(2);
        assertThat(result.getRoot().getCalc().getInput()).isEqualTo(join);
        assertThat(result.getRoot().getCalc().getPreserveInputEnvelope()).isTrue();
        Operator expand = Operator.newBuilder()
                .setExpand(tech.streamfusion.proto.plan.v1.Expand.newBuilder().setInput(EDGE))
                .build();
        assertThat(NativePlan.parseFrom(
                                StreamFusionNativeRegionTranslator.composeAbove(plan(join), List.of(plan(expand))))
                        .getProtocolVersion())
                .isEqualTo(2);
        assertThat(StreamFusionNativeRegionTranslator.composeAbove(plan(join), List.of()))
                .isEqualTo(plan(join));
        byte[] unknown = NativePlan.newBuilder()
                .setProtocolVersion(4)
                .setRoot(FIRST)
                .build()
                .toByteArray();
        assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.composeAbove(unknown, List.of(tail)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("protocol");
    }

    @Test
    void preservesStageRecordPolicyAndProtocolAcrossGenericComposition() throws Exception {
        byte[] stage = NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(FIRST.toBuilder().setClearRecordTimestamps(true))
                .build()
                .toByteArray();
        for (byte[] composed : List.of(
                StreamFusionNativeRegionTranslator.composeWithInputs(stage, List.of(plan(EDGE))),
                StreamFusionNativeRegionTranslator.composeAbove(plan(EDGE), List.of(stage)))) {
            var decoded = NativePlan.parseFrom(composed);
            assertThat(decoded.getProtocolVersion()).isEqualTo(3);
            assertThat(decoded.getRoot().getClearRecordTimestamps()).isTrue();
            assertThat(decoded.getRoot().getCalc().getInput()).isEqualTo(EDGE);
        }
        byte[] invalid = NativePlan.parseFrom(stage).toBuilder()
                .setProtocolVersion(2)
                .build()
                .toByteArray();
        assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.composeWithInputs(invalid, List.of(plan(EDGE))))
                .hasMessageContaining("timestamp policy requires");
    }

    private static final Operator EDGE =
            Operator.newBuilder().setInput(Input.newBuilder()).build();
    private static final Operator FIRST =
            Operator.newBuilder().setCalc(Calc.newBuilder().setInput(EDGE)).build();

    @Test
    void connectsEveryUnaryProtocolNodeWithoutAnOperatorCombinationAllowlist() throws Exception {
        int tested = 0;
        for (FieldDescriptor payload : Operator.getDescriptor().getFields()) {
            if (payload.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
                continue;
            }
            Message.Builder node = Operator.newBuilder().newBuilderForField(payload);
            List<FieldDescriptor> children = node.getDescriptorForType().getFields().stream()
                    .filter(field -> field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                            && field.getMessageType().equals(Operator.getDescriptor()))
                    .collect(java.util.stream.Collectors.toList());
            if (children.size() != 1 || children.get(0).isRepeated()) {
                continue;
            }
            FieldDescriptor child = children.get(0);
            Operator stage = Operator.newBuilder()
                    .setField(payload, node.setField(child, EDGE).build())
                    .build();
            NativePlan result =
                    NativePlan.parseFrom(StreamFusionNativeRegionTranslator.compose(List.of(plan(FIRST), plan(stage))));
            Message composed = (Message) result.getRoot().getField(payload);
            assertThat(composed.getField(child)).as(payload.getName()).isEqualTo(FIRST);
            tested++;
        }
        assertThat(tested).isGreaterThan(15);
    }

    @Test
    void unaryComposerRejectsMissingAndMultiplePhysicalEdges() {
        Operator missing = Operator.newBuilder().setCalc(Calc.newBuilder()).build();
        Operator binary = Operator.newBuilder()
                .setRegularJoin(RegularJoin.newBuilder().setLeftInput(EDGE).setRightInput(EDGE))
                .build();
        for (Operator node : List.of(missing, binary)) {
            assertThatThrownBy(() -> StreamFusionNativeRegionTranslator.compose(List.of(plan(node))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("exactly one physical child");
        }
    }

    private static byte[] plan(Operator root) {
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(root)
                .build()
                .toByteArray();
    }
}
