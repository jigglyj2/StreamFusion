/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.planner;

import static tech.streamfusion.flink.planner.FlinkExecNodeAccess.*;
import static tech.streamfusion.flink.planner.StreamFusionGroupAggregateSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionJoinSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionOverSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionProcessingTimeShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankShapes.*;
import static tech.streamfusion.flink.planner.StreamFusionRankSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionRuntimeClasses.*;
import static tech.streamfusion.flink.planner.StreamFusionStatelessSupport.*;
import static tech.streamfusion.flink.planner.StreamFusionWindowAggregateSupport.*;

import org.apache.flink.table.planner.plan.logical.LogicalWindow;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSort;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecSortWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGlobalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecGroupWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecIncrementalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecMiniBatchAssigner;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.types.logical.RowType;

/** StreamFusion AggregateShapes for native physical planning. */
final class StreamFusionAggregateShapes {
    static TwoPhaseWindowAggregate twoPhaseWindowAggregate(StreamExecGlobalWindowAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> exchange = global.getInputEdges().get(0).getSource();
        if (!(exchange instanceof StreamExecExchange)
                || exchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> local = exchange.getInputEdges().get(0).getSource();
        if (!(local instanceof StreamExecLocalWindowAggregate)
                || local.getInputEdges().size() != 1) {
            return null;
        }
        return new TwoPhaseWindowAggregate(
                global,
                (StreamExecExchange) exchange,
                (StreamExecLocalWindowAggregate) local,
                local.getInputEdges().get(0));
    }

    static TwoPhaseGroupAggregate twoPhaseGroupAggregate(StreamExecGlobalGroupAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> exchange = global.getInputEdges().get(0).getSource();
        if (!(exchange instanceof StreamExecExchange)
                || exchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> local = exchange.getInputEdges().get(0).getSource();
        if (!(local instanceof StreamExecLocalGroupAggregate)
                || local.getInputEdges().size() != 1) {
            return null;
        }
        return new TwoPhaseGroupAggregate(
                global,
                (StreamExecExchange) exchange,
                (StreamExecLocalGroupAggregate) local,
                local.getInputEdges().get(0));
    }

    static BatchGroupAggregatePair batchGroupAggregatePair(ExecNode<?> global) {
        if (!isBatchAggregate(global)
                || !batchAggregateBoolean(global, "isMerge")
                || !batchAggregateBoolean(global, "isFinal")
                || global.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge edge = global.getInputEdges().get(0);
        ExecNode<?> cursor = edge.getSource();
        while ((cursor instanceof BatchExecExchange || cursor instanceof BatchExecSort)
                && cursor.getInputEdges().size() == 1) {
            edge = cursor.getInputEdges().get(0);
            cursor = edge.getSource();
        }
        if (!isBatchAggregate(cursor)
                || batchAggregateBoolean(cursor, "isFinal")
                || batchAggregateBoolean(cursor, "isMerge")
                || cursor.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge inputEdge = cursor.getInputEdges().get(0);
        ExecNode<?> input = inputEdge.getSource();
        while ((input instanceof BatchExecExchange || input instanceof BatchExecSort)
                && input.getInputEdges().size() == 1) {
            inputEdge = input.getInputEdges().get(0);
            input = inputEdge.getSource();
        }
        return new BatchGroupAggregatePair((ExecNodeBase<?>) global, (ExecNodeBase<?>) cursor, inputEdge);
    }

    static BatchWindowAggregatePair batchWindowAggregatePair(ExecNode<?> global) {
        if (!isBatchWindowAggregate(global)
                || !batchWindowBoolean(global, "isMerge")
                || !batchWindowBoolean(global, "isFinal")
                || global.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge edge = global.getInputEdges().get(0);
        ExecNode<?> cursor = edge.getSource();
        while ((cursor instanceof BatchExecExchange || cursor instanceof BatchExecSort)
                && cursor.getInputEdges().size() == 1) {
            edge = cursor.getInputEdges().get(0);
            cursor = edge.getSource();
        }
        if (!isBatchWindowAggregate(cursor)
                || batchWindowBoolean(cursor, "isFinal")
                || batchWindowBoolean(cursor, "isMerge")
                || cursor.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge inputEdge = cursor.getInputEdges().get(0);
        ExecNode<?> input = inputEdge.getSource();
        while ((input instanceof BatchExecExchange || input instanceof BatchExecSort)
                && input.getInputEdges().size() == 1) {
            inputEdge = input.getInputEdges().get(0);
            input = inputEdge.getSource();
        }
        return new BatchWindowAggregatePair((ExecNodeBase<?>) global, (ExecNodeBase<?>) cursor, inputEdge);
    }

    static ExecEdge batchOnePhaseWindowInputEdge(ExecNode<?> aggregate) {
        if (!isBatchWindowAggregate(aggregate)
                || !batchWindowBoolean(aggregate, "isFinal")
                || batchWindowBoolean(aggregate, "isMerge")
                || aggregate.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge edge = aggregate.getInputEdges().get(0);
        ExecNode<?> input = edge.getSource();
        if ((input instanceof BatchExecExchange || input instanceof StreamFusionBatchExecExchange)
                && input.getInputEdges().size() == 1) {
            ExecEdge exchangeInput = input.getInputEdges().get(0);
            ExecNode<?> exchangeChild = exchangeInput.getSource();
            if (exchangeChild instanceof BatchExecSort || exchangeChild instanceof StreamFusionBatchExecBoundedSort) {
                edge = exchangeInput;
                input = exchangeChild;
            }
        }
        while ((input instanceof BatchExecSort || input instanceof StreamFusionBatchExecBoundedSort)
                && input.getInputEdges().size() == 1) {
            edge = input.getInputEdges().get(0);
            input = edge.getSource();
        }
        return edge;
    }

    static boolean isBatchWindowAggregate(ExecNode<?> node) {
        return node instanceof BatchExecHashWindowAggregate || node instanceof BatchExecSortWindowAggregate;
    }

    static boolean isBatchAggregate(ExecNode<?> node) {
        return node instanceof BatchExecHashAggregate || node instanceof BatchExecSortAggregate;
    }

    static int[] batchGrouping(ExecNode<?> node) {
        if (node instanceof BatchExecHashAggregate) {
            return grouping((BatchExecHashAggregate) node);
        }
        return grouping((BatchExecSortAggregate) node);
    }

    static int[] batchAuxiliaryGrouping(ExecNode<?> node) {
        if (node instanceof BatchExecHashAggregate) {
            return auxiliaryGrouping((BatchExecHashAggregate) node);
        }
        return auxiliaryGrouping((BatchExecSortAggregate) node);
    }

    static org.apache.calcite.rel.core.AggregateCall[] batchAggregateCalls(ExecNode<?> node) {
        if (node instanceof BatchExecHashAggregate) {
            return aggregateCalls((BatchExecHashAggregate) node);
        }
        return aggregateCalls((BatchExecSortAggregate) node);
    }

    static RowType batchAggregateInputType(ExecNode<?> node) {
        if (node instanceof BatchExecHashAggregate) {
            return aggregateInputType((BatchExecHashAggregate) node);
        }
        return aggregateInputType((BatchExecSortAggregate) node);
    }

    static boolean batchAggregateBoolean(ExecNode<?> node, String fieldName) {
        if (node instanceof BatchExecHashAggregate) {
            return batchAggregateBooleanField((BatchExecHashAggregate) node, fieldName);
        }
        return batchAggregateBooleanField((BatchExecSortAggregate) node, fieldName);
    }

    static IncrementalGroupAggregate incrementalGroupAggregate(StreamExecGlobalGroupAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> finalExchange = global.getInputEdges().get(0).getSource();
        if (!(finalExchange instanceof StreamExecExchange)
                || finalExchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> incremental = finalExchange.getInputEdges().get(0).getSource();
        if (!(incremental instanceof StreamExecIncrementalGroupAggregate)
                || incremental.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> partialExchange = incremental.getInputEdges().get(0).getSource();
        if (!(partialExchange instanceof StreamExecExchange)
                || partialExchange.getInputEdges().size() != 1) {
            return null;
        }
        ExecNode<?> local = partialExchange.getInputEdges().get(0).getSource();
        if (!(local instanceof StreamExecLocalGroupAggregate)
                || local.getInputEdges().size() != 1) {
            return null;
        }
        ExecEdge originalInput = local.getInputEdges().get(0);
        return new IncrementalGroupAggregate(
                global,
                (StreamExecExchange) finalExchange,
                (StreamExecIncrementalGroupAggregate) incremental,
                (StreamExecExchange) partialExchange,
                (StreamExecLocalGroupAggregate) local,
                originalInput);
    }

    static boolean hasIncrementalGroupAggregateChain(StreamExecGlobalGroupAggregate global) {
        if (global.getInputEdges().size() != 1) {
            return false;
        }
        ExecNode<?> finalExchange = global.getInputEdges().get(0).getSource();
        return finalExchange instanceof StreamExecExchange
                && finalExchange.getInputEdges().size() == 1
                && finalExchange.getInputEdges().get(0).getSource() instanceof StreamExecIncrementalGroupAggregate;
    }

    static org.apache.flink.table.planner.plan.trait.MiniBatchInterval miniBatchInterval(
            StreamExecMiniBatchAssigner assigner) {
        return (org.apache.flink.table.planner.plan.trait.MiniBatchInterval)
                field(assigner, StreamExecMiniBatchAssigner.class, "miniBatchInterval");
    }

    static LegacyGroupWindowAggregate legacyGroupWindowAggregate(StreamExecGroupWindowAggregate node) {
        int[] grouping = ((int[]) field(node, StreamExecGroupWindowAggregate.class, "grouping")).clone();
        org.apache.calcite.rel.core.AggregateCall[] aggregateCalls = ((org.apache.calcite.rel.core.AggregateCall[])
                        field(node, StreamExecGroupWindowAggregate.class, "aggCalls"))
                .clone();
        LogicalWindow logicalWindow = (LogicalWindow) field(node, StreamExecGroupWindowAggregate.class, "window");
        NamedWindowProperty[] properties = ((NamedWindowProperty[])
                        field(node, StreamExecGroupWindowAggregate.class, "namedWindowProperties"))
                .clone();
        boolean needRetraction = (boolean) field(node, StreamExecGroupWindowAggregate.class, "needRetraction");
        return new LegacyGroupWindowAggregate(
                node, grouping, aggregateCalls, logicalWindow, properties, needRetraction);
    }

    static int[] localWindowGrouping(StreamExecLocalWindowAggregate aggregate) {
        return ((int[]) field(aggregate, StreamExecLocalWindowAggregate.class, "grouping")).clone();
    }

    static org.apache.calcite.rel.core.AggregateCall[] localWindowAggregateCalls(
            StreamExecLocalWindowAggregate aggregate) {
        return ((org.apache.calcite.rel.core.AggregateCall[])
                        field(aggregate, StreamExecLocalWindowAggregate.class, "aggCalls"))
                .clone();
    }

    static WindowingStrategy localWindowing(StreamExecLocalWindowAggregate aggregate) {
        return (WindowingStrategy) field(aggregate, StreamExecLocalWindowAggregate.class, "windowing");
    }

    static boolean localWindowNeedRetraction(StreamExecLocalWindowAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecLocalWindowAggregate.class, "needRetraction");
    }

    static NamedWindowProperty[] globalWindowProperties(StreamExecGlobalWindowAggregate aggregate) {
        return ((NamedWindowProperty[])
                        field(aggregate, StreamExecGlobalWindowAggregate.class, "namedWindowProperties"))
                .clone();
    }

    static boolean globalWindowNeedRetraction(StreamExecGlobalWindowAggregate aggregate) {
        return (boolean) field(aggregate, StreamExecGlobalWindowAggregate.class, "needRetraction");
    }

    static final class TwoPhaseWindowAggregate {
        final StreamExecGlobalWindowAggregate global;
        final StreamExecExchange exchange;
        final StreamExecLocalWindowAggregate local;
        final ExecEdge inputEdge;

        TwoPhaseWindowAggregate(
                StreamExecGlobalWindowAggregate global,
                StreamExecExchange exchange,
                StreamExecLocalWindowAggregate local,
                ExecEdge inputEdge) {
            this.global = global;
            this.exchange = exchange;
            this.local = local;
            this.inputEdge = inputEdge;
        }
    }

    static final class LegacyGroupWindowAggregate {
        final StreamExecGroupWindowAggregate node;
        final int[] grouping;
        final org.apache.calcite.rel.core.AggregateCall[] aggregateCalls;
        final LogicalWindow window;
        final NamedWindowProperty[] properties;
        final boolean needRetraction;

        LegacyGroupWindowAggregate(
                StreamExecGroupWindowAggregate node,
                int[] grouping,
                org.apache.calcite.rel.core.AggregateCall[] aggregateCalls,
                LogicalWindow window,
                NamedWindowProperty[] properties,
                boolean needRetraction) {
            this.node = node;
            this.grouping = grouping;
            this.aggregateCalls = aggregateCalls;
            this.window = window;
            this.properties = properties;
            this.needRetraction = needRetraction;
        }
    }

    static final class TwoPhaseGroupAggregate {
        final StreamExecGlobalGroupAggregate global;
        final StreamExecExchange exchange;
        final StreamExecLocalGroupAggregate local;
        final ExecEdge inputEdge;

        TwoPhaseGroupAggregate(
                StreamExecGlobalGroupAggregate global,
                StreamExecExchange exchange,
                StreamExecLocalGroupAggregate local,
                ExecEdge inputEdge) {
            this.global = global;
            this.exchange = exchange;
            this.local = local;
            this.inputEdge = inputEdge;
        }
    }

    static final class BatchGroupAggregatePair {
        final ExecNodeBase<?> global;
        final ExecNodeBase<?> local;
        final ExecEdge inputEdge;

        BatchGroupAggregatePair(ExecNodeBase<?> global, ExecNodeBase<?> local, ExecEdge inputEdge) {
            this.global = global;
            this.local = local;
            this.inputEdge = inputEdge;
        }
    }

    static final class BatchWindowAggregatePair {
        final ExecNodeBase<?> global;
        final ExecNodeBase<?> local;
        final ExecEdge inputEdge;

        BatchWindowAggregatePair(ExecNodeBase<?> global, ExecNodeBase<?> local, ExecEdge inputEdge) {
            this.global = global;
            this.local = local;
            this.inputEdge = inputEdge;
        }
    }

    static final class IncrementalGroupAggregate {
        final StreamExecGlobalGroupAggregate global;
        final StreamExecExchange finalExchange;
        final StreamExecIncrementalGroupAggregate incremental;
        final StreamExecExchange partialExchange;
        final StreamExecLocalGroupAggregate local;
        final ExecEdge inputEdge;

        IncrementalGroupAggregate(
                StreamExecGlobalGroupAggregate global,
                StreamExecExchange finalExchange,
                StreamExecIncrementalGroupAggregate incremental,
                StreamExecExchange partialExchange,
                StreamExecLocalGroupAggregate local,
                ExecEdge inputEdge) {
            this.global = global;
            this.finalExchange = finalExchange;
            this.incremental = incremental;
            this.partialExchange = partialExchange;
            this.local = local;
            this.inputEdge = inputEdge;
        }
    }
}
