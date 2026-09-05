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

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.concurrent.atomic.AtomicLong;
import tech.streamfusion.proto.plan.v1.NativePlan;
import tech.streamfusion.proto.plan.v1.Operator;

/** Assigns stable pre-order identities to the native physical-plan tree before JNI. */
final class NativePlanNodeIdentity {
    private NativePlanNodeIdentity() {}

    static byte[] assign(byte[] serializedPlan) {
        try {
            NativePlan.Builder plan = NativePlan.parseFrom(serializedPlan).toBuilder();
            if (!plan.hasRoot()) {
                return serializedPlan;
            }
            assign(plan.getRootBuilder(), new AtomicLong(1));
            return plan.build().toByteArray();
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Invalid StreamFusion native plan", failure);
        }
    }

    private static void assign(Operator.Builder operator, AtomicLong nextId) {
        if (operator.getPlanNodeId() == 0) {
            operator.setPlanNodeId(nextId.getAndIncrement());
        } else {
            nextId.accumulateAndGet(operator.getPlanNodeId() + 1, Math::max);
        }
        switch (operator.getOperatorCase()) {
            case BOUNDED_SORT:
                assign(operator.getBoundedSortBuilder().getInputBuilder(), nextId);
                break;
            case TEMPORAL_SORT:
                assign(operator.getTemporalSortBuilder().getInputBuilder(), nextId);
                break;
            case OVER_AGGREGATE:
                assign(operator.getOverAggregateBuilder().getInputBuilder(), nextId);
                break;
            case TOP_N:
                assign(operator.getTopNBuilder().getInputBuilder(), nextId);
                break;
            case DEDUPLICATE:
                assign(operator.getDeduplicateBuilder().getInputBuilder(), nextId);
                break;
            case CHANGELOG_NORMALIZE:
                assign(operator.getChangelogNormalizeBuilder().getInputBuilder(), nextId);
                break;
            case GROUP_AGGREGATE:
                assign(operator.getGroupAggregateBuilder().getInputBuilder(), nextId);
                break;
            case LOCAL_GROUP_AGGREGATE:
                assign(operator.getLocalGroupAggregateBuilder().getInputBuilder(), nextId);
                break;
            case GLOBAL_GROUP_AGGREGATE:
                assign(operator.getGlobalGroupAggregateBuilder().getInputBuilder(), nextId);
                break;
            case INCREMENTAL_GROUP_AGGREGATE:
                assign(operator.getIncrementalGroupAggregateBuilder().getInputBuilder(), nextId);
                break;
            case WINDOW_AGGREGATE:
                assign(operator.getWindowAggregateBuilder().getInputBuilder(), nextId);
                break;
            case WINDOW_DEDUPLICATE:
                assign(operator.getWindowDeduplicateBuilder().getInputBuilder(), nextId);
                break;
            case WINDOW_RANK:
                assign(operator.getWindowRankBuilder().getInputBuilder(), nextId);
                break;
            case WINDOW_TABLE_FUNCTION:
                assign(operator.getWindowTableFunctionBuilder().getInputBuilder(), nextId);
                break;
            case UNION:
                for (int index = 0; index < operator.getUnion().getInputsCount(); index++) {
                    assign(operator.getUnionBuilder().getInputsBuilder(index), nextId);
                }
                break;
            case EXPAND:
                assign(operator.getExpandBuilder().getInputBuilder(), nextId);
                break;
            case CALC:
                assign(operator.getCalcBuilder().getInputBuilder(), nextId);
                break;
            case ARRAY_UNNEST:
                assign(operator.getArrayUnnestBuilder().getInputBuilder(), nextId);
                break;
            case REPLICATE_ROWS:
                assign(operator.getReplicateRowsBuilder().getInputBuilder(), nextId);
                break;
            default:
                // Stateful multi-input operators receive their Arrow inputs at the persistent
                // handle boundary and therefore have no nested Operator children in the plan.
        }
    }
}
