/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.planner;

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.ConstantRankRange;
import org.apache.flink.table.runtime.operators.rank.FastTop1Function;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import tech.streamfusion.flink.StreamFusionPlannerFactory;
import tech.streamfusion.flink.proto.FlinkLogicalTypeProto;
import tech.streamfusion.proto.plan.v1.*;

/** Original Flink SQL selects FastTop1Function; native plans use the same input/output types. */
final class TopOneFlinkFixture {
    final OneInputTransformation<?, ?> stage;
    final RowType input;
    final RowType output;
    final boolean ascending;
    final boolean rankNumber;
    final boolean before;

    @SuppressWarnings("unchecked")
    TopOneFlinkFixture(boolean ascending, boolean rankNumber, boolean before) throws Exception {
        this.ascending = ascending;
        this.rankNumber = rankNumber;
        this.before = before;
        String factory = System.getProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        String processor = System.getProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY);
        System.clearProperty(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY);
        try {
            var env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            var tables = StreamTableEnvironment.create(env);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_STATE_ENABLED, false);
            tables.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
            var source = env.fromCollection(
                    List.of(Row.of(1L, 1L, LocalDateTime.of(2026, 1, 1, 0, 0), "label")),
                    Types.ROW_NAMED(
                            new String[] {"k", "score", "ts", "label"},
                            Types.LONG,
                            Types.LONG,
                            Types.LOCAL_DATE_TIME,
                            Types.STRING));
            tables.createTemporaryView(
                    "top_one_input",
                    tables.fromDataStream(
                            source,
                            org.apache.flink.table.api.Schema.newBuilder()
                                    .column("k", org.apache.flink.table.api.DataTypes.BIGINT())
                                    .column("score", org.apache.flink.table.api.DataTypes.BIGINT())
                                    .column("ts", org.apache.flink.table.api.DataTypes.TIMESTAMP(3))
                                    .column("label", org.apache.flink.table.api.DataTypes.STRING())
                                    .build()));
            String sql = "SELECT k,score,ts,label" + (rankNumber ? ",rn" : "")
                    + " FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY k ORDER BY score "
                    + (ascending ? "ASC" : "DESC")
                    + " NULLS LAST,ts ASC NULLS FIRST) rn FROM top_one_input) WHERE rn<=1";
            var stream = tables.toChangelogStream(tables.sqlQuery(sql));
            stage = find(stream.getTransformation());
            if (stage == null) throw new AssertionError("Flink did not select Rank");
            input = ((InternalTypeInfo<RowData>) stage.getInputs().get(0).getOutputType()).toRowType();
            var fields = new java.util.ArrayList<>(input.getFields());
            if (rankNumber)
                fields.add(new RowType.RowField("rn", new org.apache.flink.table.types.logical.BigIntType(false)));
            output = new RowType(fields);
        } finally {
            restore(StreamFusionPlannerFactory.FACTORY_CLASS_PROPERTY, factory);
            restore(StreamFusionPlannerFactory.EXEC_GRAPH_PROCESSOR_PROPERTY, processor);
        }
    }

    @SuppressWarnings("unchecked")
    KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> oracle(boolean rocks) throws Exception {
        var original = ((KeyedProcessOperator<RowData, RowData, RowData>) stage.getOperator()).getUserFunction();
        if (!(original instanceof FastTop1Function)) throw new AssertionError("Flink did not select FastTop1Function");
        // SQL may replace rank 1 with a constant or suppress UPDATE_BEFORE for an upsert sink.
        // Reuse its generated comparator/selector with explicit physical function flags.
        var function = new FastTop1Function(
                (StateTtlConfig) FlinkExecNodeAccess.field(original, AbstractTopNFunction.class, "ttlConfig"),
                InternalTypeInfo.of(input),
                (GeneratedRecordComparator)
                        FlinkExecNodeAccess.field(original, AbstractTopNFunction.class, "generatedSortKeyComparator"),
                (RowDataKeySelector) FlinkExecNodeAccess.field(original, AbstractTopNFunction.class, "sortKeySelector"),
                RankType.ROW_NUMBER,
                new ConstantRankRange(1, 1),
                before,
                rankNumber,
                (long) FlinkExecNodeAccess.field(original, FastTop1Function.class, "cacheSize"));
        var operator = new KeyedProcessOperator<>(function);
        function.setKeyContext(operator);
        var harness = new KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>(
                operator, (KeySelector<RowData, RowData>) stage.getStateKeySelector(), (InternalTypeInfo<RowData>)
                        stage.getStateKeyType());
        harness.setStateBackend(
                rocks
                        ? new org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend(true)
                        : new org.apache.flink.runtime.state.hashmap.HashMapStateBackend());
        harness.setup(new RowDataSerializer(output));
        harness.open();
        return harness;
    }

    byte[] plan() {
        var node = TopN.newBuilder()
                .setInput(Operator.newBuilder().setInput(Input.newBuilder()))
                .setInputSchema(schema(input))
                .setOutputSchema(schema(output))
                .addPartitionKeyIndices(0)
                .addSortKeyIndices(1)
                .addSortAscending(ascending)
                .addSortNullsLast(true)
                .addSortKeyIndices(2)
                .addSortAscending(true)
                .addSortNullsLast(false)
                .setRankStart(1)
                .setRankEnd(1)
                .setOutputRankNumber(rankNumber)
                .setGenerateUpdateBefore(before)
                .setStrategy(TopNStrategy.TOP_N_STRATEGY_APPEND_FAST)
                .setRankType(TopNRankType.TOP_N_RANK_TYPE_ROW_NUMBER);
        return NativePlan.newBuilder()
                .setProtocolVersion(1)
                .setRoot(Operator.newBuilder().setTopN(node))
                .build()
                .toByteArray();
    }

    private static Schema schema(RowType type) {
        var schema = Schema.newBuilder();
        for (var field : type.getFields())
            schema.addFields(Field.newBuilder()
                    .setName(field.getName())
                    .setType(FlinkLogicalTypeProto.serialize(field.getType())));
        return schema.build();
    }

    private static OneInputTransformation<?, ?> find(Transformation<?> node) {
        if (node instanceof OneInputTransformation<?, ?> && node.getName().contains("Rank"))
            return (OneInputTransformation<?, ?>) node;
        for (var input : node.getInputs()) {
            var found = find(input);
            if (found != null) return found;
        }
        return null;
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
