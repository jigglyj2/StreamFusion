/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.nativebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import tech.streamfusion.proto.plan.v1.Calc;
import tech.streamfusion.proto.plan.v1.Input;
import tech.streamfusion.proto.plan.v1.LookupJoin;
import tech.streamfusion.proto.plan.v1.LookupJoinKind;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;
import tech.streamfusion.proto.plan.v1.Union;

class NativePlanNodeIdentityTest {
    @Test
    void windowJoinChildrenReceiveStableDistinctStageIdentities() throws Exception {
        NativePlan plan = NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder()
                        .setClearRecordTimestamps(true)
                        .setWindowJoin(tech.streamfusion.proto.plan.v1.WindowJoin.newBuilder()
                                .setJoinType(tech.streamfusion.proto.plan.v1.RegularJoinType.REGULAR_JOIN_TYPE_INNER)
                                .setLeftInput(Operator.newBuilder()
                                        .setInput(Input.newBuilder().setInputIndex(0)))
                                .setRightInput(Operator.newBuilder()
                                        .setInput(Input.newBuilder().setInputIndex(1)))))
                .build();
        NativePlan identified = NativePlan.parseFrom(NativePlanNodeIdentity.assign(plan.toByteArray()));
        assertThat(identified.getRoot().getPlanNodeId()).isEqualTo(1);
        assertThat(identified.getRoot().getWindowJoin().getLeftInput().getPlanNodeId())
                .isEqualTo(2);
        assertThat(identified.getRoot().getWindowJoin().getRightInput().getPlanNodeId())
                .isEqualTo(3);
        assertThat(NativePlanNodeIdentity.assign(identified.toByteArray())).containsExactly(identified.toByteArray());
    }

    @Test
    void lookupSnapshotIdentityStaysStableWhileProbeChildrenAreAssigned() throws Exception {
        NativePlan original = NativePlan.newBuilder()
                .setProtocolVersion(3)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(41)
                        .setMetricName("LookupJoin[41]")
                        .setLookupJoin(LookupJoin.newBuilder()
                                .setKind(LookupJoinKind.LOOKUP_JOIN_KIND_INNER)
                                .addProbeKeys(2)
                                .addSideKeys(0)
                                .setInput(Operator.newBuilder()
                                        .setCalc(Calc.newBuilder()
                                                .setPreserveInputEnvelope(true)
                                                .setInput(
                                                        Operator.newBuilder().setInput(Input.getDefaultInstance()))))))
                .build();
        NativePlan identified = NativePlan.parseFrom(NativePlanNodeIdentity.assign(original.toByteArray()));
        assertThat(identified.getRoot().getPlanNodeId()).isEqualTo(41);
        assertThat(identified.getRoot().getMetricName()).isEqualTo("LookupJoin[41]");
        LookupJoin lookup = identified.getRoot().getLookupJoin();
        assertThat(lookup.getKind()).isEqualTo(LookupJoinKind.LOOKUP_JOIN_KIND_INNER);
        assertThat(lookup.getProbeKeysList()).containsExactly(2);
        assertThat(lookup.getSideKeysList()).containsExactly(0);
        assertThat(lookup.getInput().getPlanNodeId()).isEqualTo(1);
        assertThat(lookup.getInput().getCalc().getInput().getPlanNodeId()).isEqualTo(2);
        assertThat(NativePlanNodeIdentity.assign(identified.toByteArray())).containsExactly(identified.toByteArray());
    }

    @Test
    void reservesDescendantIdsBeforeAssigningMissingParentIds() throws Exception {
        NativePlan original = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder()
                        .setMetricName("Calc[original-37]")
                        .setCalc(Calc.newBuilder()
                                .setInput(Operator.newBuilder().setPlanNodeId(1).setInput(Input.getDefaultInstance()))))
                .build();
        NativePlan identified = NativePlan.parseFrom(NativePlanNodeIdentity.assign(original.toByteArray()));
        assertThat(identified.getRoot().getPlanNodeId()).isEqualTo(2);
        assertThat(identified.getRoot().getMetricName()).isEqualTo("Calc[original-37]");
        assertThat(identified.getRoot().getCalc().getInput().getPlanNodeId()).isEqualTo(1);
    }

    @Test
    void rejectsDuplicatePlannerIds() {
        NativePlan original = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(1)
                        .setCalc(Calc.newBuilder()
                                .setInput(Operator.newBuilder().setPlanNodeId(1).setInput(Input.getDefaultInstance()))))
                .build();
        assertThatThrownBy(() -> NativePlanNodeIdentity.assign(original.toByteArray()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate physical node id");
    }

    @Test
    void assignsStablePreOrderIdsWithoutReplacingPlannerIds() throws Exception {
        Operator input =
                Operator.newBuilder().setInput(Input.getDefaultInstance()).build();
        NativePlan original = NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(41)
                        .setCalc(Calc.newBuilder().setInput(input)))
                .build();

        NativePlan identified = NativePlan.parseFrom(NativePlanNodeIdentity.assign(original.toByteArray()));

        assertThat(identified.getRoot().getPlanNodeId()).isEqualTo(41);
        assertThat(identified.getRoot().getCalc().getInput().getPlanNodeId()).isEqualTo(1);
        assertThat(NativePlanNodeIdentity.rootId(identified.toByteArray())).isEqualTo(41);
        assertThat(NativePlanNodeIdentity.assign(original.toByteArray()))
                .containsExactly(NativePlanNodeIdentity.assign(original.toByteArray()));
    }

    @Test
    void reservesPhysicalIdsAndWalksRepeatedChildrenWithoutFamilyDispatch() throws Exception {
        long physicalId = (1L << 32) | 59;
        NativePlan original = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder()
                        .setPlanNodeId(physicalId)
                        .setUnion(Union.newBuilder()
                                .addInputs(Operator.newBuilder().setInput(Input.getDefaultInstance()))
                                .addInputs(Operator.newBuilder()
                                        .setCalc(Calc.newBuilder()
                                                .setInput(Operator.newBuilder()
                                                        .setPlanNodeId(1)
                                                        .setInput(Input.getDefaultInstance()))))))
                .build();
        NativePlan identified = NativePlan.parseFrom(NativePlanNodeIdentity.assign(original.toByteArray()));
        assertThat(identified.getRoot().getPlanNodeId()).isEqualTo(physicalId);
        assertThat(identified.getRoot().getUnion().getInputs(0).getPlanNodeId()).isEqualTo(2);
        assertThat(identified.getRoot().getUnion().getInputs(1).getPlanNodeId()).isEqualTo(3);
        assertThat(identified
                        .getRoot()
                        .getUnion()
                        .getInputs(1)
                        .getCalc()
                        .getInput()
                        .getPlanNodeId())
                .isEqualTo(1);
        assertThat(NativePlanNodeIdentity.assign(identified.toByteArray())).containsExactly(identified.toByteArray());
    }

    @Test
    void doesNotManufactureAbsentChildrenOrOverflowAtLargestSignedPhysicalId() throws Exception {
        NativePlan original = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder().setPlanNodeId(Long.MAX_VALUE).setCalc(Calc.getDefaultInstance()))
                .build();
        NativePlan identified = NativePlan.parseFrom(NativePlanNodeIdentity.assign(original.toByteArray()));
        assertThat(identified).isEqualTo(original);
        assertThat(identified.getRoot().getCalc().hasInput()).isFalse();
    }

    @Test
    void rejectsIdsOutsideSignedJavaRange() {
        NativePlan original = NativePlan.newBuilder()
                .setRoot(Operator.newBuilder().setPlanNodeId(-1).setInput(Input.getDefaultInstance()))
                .build();
        assertThatThrownBy(() -> NativePlanNodeIdentity.assign(original.toByteArray()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid or duplicate physical node id");
    }
}
