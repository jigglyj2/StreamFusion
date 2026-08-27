/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.planner;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import tech.streamfusion.flink.operator.ArrowToRowDataSinkTransposeOperator;
import tech.streamfusion.flink.operator.DummyStreamFusionOperator;
import tech.streamfusion.flink.operator.RowDataToArrowSourceTransposeOperator;
import tech.streamfusion.flink.planner.StreamFusionPlanNode.Role;

class StreamFusionPlanAnalyzerTest {
    private final StreamFusionPlanAnalyzer analyzer = new StreamFusionPlanAnalyzer();

    @Test
    void acceptsOnlyWhenEveryNodeHasStreamFusionCoverage() {
        StreamFusionPlanNode source =
                StreamFusionPlanNode.supported("KafkaSource", Role.SOURCE, new RowDataToArrowSourceTransposeOperator());
        StreamFusionPlanNode calc = StreamFusionPlanNode.supported(
                "StreamExecCalc", Role.INTERNAL, new DummyStreamFusionOperator("StreamFusionDummyCalc"), source);
        StreamFusionPlanNode sink =
                StreamFusionPlanNode.supported("KafkaSink", Role.SINK, new ArrowToRowDataSinkTransposeOperator(), calc);

        StreamFusionPlanDecision decision = analyzer.analyze(sink);

        assertThat(decision.acceleratesWholePlan()).isTrue();
        assertThat(decision.explain())
                .contains("Accelerated: yes")
                .contains("every internal node has a StreamFusion operator");
    }

    @Test
    void rejectsWholePlanAndExplainsEveryUnsupportedOperator() {
        StreamFusionPlanNode source =
                StreamFusionPlanNode.supported("KafkaSource", Role.SOURCE, new RowDataToArrowSourceTransposeOperator());
        StreamFusionPlanNode calc = StreamFusionPlanNode.unsupported(
                "StreamExecCalc", Role.INTERNAL, "scalar function JSON_VALUE is not implemented", source);
        StreamFusionPlanNode aggregate = StreamFusionPlanNode.unsupported(
                "StreamExecGroupAggregate", Role.INTERNAL, "retractable SUM is not implemented", calc);
        StreamFusionPlanNode sink = StreamFusionPlanNode.supported(
                "KafkaSink", Role.SINK, new ArrowToRowDataSinkTransposeOperator(), aggregate);

        StreamFusionPlanDecision decision = analyzer.analyze(sink);

        assertThat(decision.acceleratesWholePlan()).isFalse();
        assertThat(decision.rejections())
                .extracting(StreamFusionPlanDecision.Rejection::flinkOperator)
                .containsExactly("StreamExecGroupAggregate", "StreamExecCalc");
        assertThat(decision.explain())
                .contains("the entire plan will use Flink")
                .contains("StreamExecCalc [INTERNAL]: scalar function JSON_VALUE is not implemented")
                .contains("StreamExecGroupAggregate [INTERNAL]: retractable SUM is not implemented");
    }

    @Test
    void rejectsUncoveredSourceOrSinkBoundary() {
        StreamFusionPlanNode source = StreamFusionPlanNode.unsupported(
                "CustomSource", Role.SOURCE, "no StreamFusion source or RowData-to-Arrow transpose");
        StreamFusionPlanNode internal = StreamFusionPlanNode.supported(
                "StreamExecCalc", Role.INTERNAL, new DummyStreamFusionOperator("StreamFusionDummyCalc"), source);
        StreamFusionPlanNode sink = StreamFusionPlanNode.unsupported(
                "CustomSink", Role.SINK, "no StreamFusion sink or Arrow-to-RowData transpose", internal);

        StreamFusionPlanDecision decision = analyzer.analyze(sink);

        assertThat(decision.acceleratesWholePlan()).isFalse();
        assertThat(decision.explain())
                .contains("CustomSource [SOURCE]: no StreamFusion source or RowData-to-Arrow transpose")
                .contains("CustomSink [SINK]: no StreamFusion sink or Arrow-to-RowData transpose");
    }
}
