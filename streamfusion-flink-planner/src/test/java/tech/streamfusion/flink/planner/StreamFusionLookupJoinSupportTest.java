/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.TemporalTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLookupJoin;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@SuppressWarnings("deprecation")
class StreamFusionLookupJoinSupportTest {
    @TempDir
    Path directory;

    private static final RowType INPUT = RowType.of(new BigIntType());
    private static final RowType OUTPUT =
            RowType.of(new BigIntType(), new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));

    @Test
    void ordinarySelectionBuildsADistinctNativeNodeWithoutOpeningTheSnapshot() {
        var original = lookup(FlinkJoinType.INNER, false, null, Map.of(0, new FunctionCallUtil.FieldRef(0)));
        assertThat(StreamFusionLookupJoinSupport.unsupportedReason(original)).isNull();
        var graph = new ExecNodeGraph(List.of(original));
        var selected = new StreamFusionExecGraphProcessor().process(graph, null);
        assertThat(selected).as(StreamFusionPlanningDiagnostics.explain()).isNotSameAs(graph);
        assertThat(selected.getRootNodes().get(0).getInputEdges().get(0).getSource())
                .isInstanceOf(StreamFusionExecLookupJoin.class);
        assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: yes");
    }

    @Test
    void unsupportedLookupSemanticsKeepTheCompleteOriginalGraph() {
        var cases = Map.of(
                "synchronous inner",
                        lookup(FlinkJoinType.LEFT, false, null, Map.of(0, new FunctionCallUtil.FieldRef(0))),
                "upsert materialization",
                        lookup(FlinkJoinType.INNER, true, null, Map.of(0, new FunctionCallUtil.FieldRef(0))),
                "projectionOnTemporalTable",
                        lookup(FlinkJoinType.INNER, false, List.of(), Map.of(0, new FunctionCallUtil.FieldRef(0))),
                "outside its payload",
                        lookup(FlinkJoinType.INNER, false, null, Map.of(0, new FunctionCallUtil.FieldRef(3))),
                "nonempty equality", lookup(FlinkJoinType.INNER, false, null, Map.of()));
        for (var entry : cases.entrySet()) {
            var original = entry.getValue();
            var edge = original.getInputEdges().get(0);
            var graph = new ExecNodeGraph(List.of(original));
            assertThat(new StreamFusionExecGraphProcessor().process(graph, null))
                    .isSameAs(graph);
            assertThat(original.getInputEdges()).containsExactly(edge);
            assertThat(StreamFusionPlanningDiagnostics.explain()).contains("Accelerated: no", entry.getKey());
        }
    }

    @Test
    void selectedLookupAndKeyedStageAreRejectedBeforeGraphCommitEvenWithOneExit() {
        var original = lookup(FlinkJoinType.INNER, false, null, Map.of(0, new FunctionCallUtil.FieldRef(0)));
        var lookup = new StreamFusionExecLookupJoin(
                new Configuration(), InputProperty.DEFAULT, OUTPUT, StreamFusionLookupJoinSupport.describe(original));
        lookup.setInputEdges(original.getInputEdges());
        // The ownership declaration is the production contract. No duplicated operator-family list.
        var keyed = new KeyedStage();
        keyed.setInputEdges(
                List.of(ExecEdge.builder().source(lookup).target(keyed).build()));
        assertThatThrownBy(() -> StreamFusionSharedNativeRegion.install(new ExecNodeGraph(List.of(keyed)), null))
                .hasMessageContaining("lookup task-open sources", "keyed state initialization");
    }

    private StreamExecLookupJoin lookup(
            FlinkJoinType kind,
            boolean materialize,
            List<org.apache.calcite.rex.RexNode> projection,
            Map<Integer, FunctionCallUtil.FunctionParam> keys) {
        var config = new Configuration();
        var csv = CsvTableSource.builder()
                .path(directory.resolve("absent.csv").toString())
                .field("key", DataTypes.BIGINT())
                .field("value", DataTypes.STRING())
                .build();
        var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        var side = (RowType) csv.getProducedDataType().getLogicalType();
        var temporal = new LegacyTableSourceTable<>(
                null,
                ObjectIdentifier.of("catalog", "db", "side"),
                types.buildRelNodeRowType(side),
                FlinkStatistic.UNKNOWN(),
                csv,
                true,
                org.apache.flink.table.catalog.CatalogTable.newBuilder()
                        .schema(org.apache.flink.table.api.Schema.newBuilder()
                                .column("key", DataTypes.BIGINT())
                                .column("value", DataTypes.STRING())
                                .build())
                        .options(Map.of())
                        .build());
        var node = new StreamExecLookupJoin(
                config,
                kind,
                null,
                null,
                new TemporalTableSourceSpec(temporal),
                keys,
                projection,
                null,
                false,
                materialize,
                null,
                null,
                ChangelogMode.all(),
                null,
                InputProperty.DEFAULT,
                OUTPUT,
                "lookup",
                false);
        var source = new StreamExecTableSourceScan(config, null, INPUT, "source");
        source.setInputEdges(List.of());
        node.setInputEdges(
                List.of(ExecEdge.builder().source(source).target(node).build()));
        return node;
    }

    private static final class KeyedStage
            extends org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase<org.apache.flink.table.data.RowData>
            implements StreamFusionNativePlanNode,
                    org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode<
                            org.apache.flink.table.data.RowData> {
        KeyedStage() {
            super(
                    org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext.newNodeId(),
                    new org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext("test-keyed_1"),
                    new Configuration(),
                    List.of(InputProperty.DEFAULT),
                    OUTPUT,
                    "keyed");
        }

        public boolean ownsNativeKeyedState() {
            return true;
        }

        public StreamFusionNativeNodeMetadata nativeMetadata() {
            throw new AssertionError("preflight only");
        }

        public byte[] nativePlanFragment(org.apache.flink.table.planner.delegation.PlannerBase planner) {
            throw new AssertionError("preflight only");
        }

        protected org.apache.flink.api.dag.Transformation<org.apache.flink.table.data.RowData> translateToPlanInternal(
                org.apache.flink.table.planner.delegation.PlannerBase planner,
                org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig config) {
            throw new AssertionError("preflight only");
        }
    }
}
