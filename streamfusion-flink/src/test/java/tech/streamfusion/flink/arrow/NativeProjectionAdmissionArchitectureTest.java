/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class NativeProjectionAdmissionArchitectureTest {
    @Test
    void scalarAdmissionKeepsDataFusionProjectionAndDirectArrayHandoff() throws Exception {
        Path expressions = Path.of("../streamfusion-native/src/planner/expressions");
        String calc = Files.readString(Path.of("../streamfusion-native/src/planner/operators/calc.rs"));
        assertThat(calc).contains("managed_expression::projection(", "ProjectionExec::try_new(expressions, child)");
        String materialize = Files.readString(expressions.resolve("managed_expression/materialize.rs"));
        assertThat(materialize)
                .contains(
                        "literal.value()",
                        "value.to_array_of_size(rows)",
                        "value @ ColumnarValue::Array(_) => Ok(value)",
                        "datafusion_array_registered(");
        assertThat(materialize).doesNotContain("jni", "RowData", "concat_batches", "RecordBatch::try_new");
        String expand = Files.readString(Path.of("../streamfusion-native/src/planner/operators/expand.rs"));
        assertThat(expand)
                .contains("managed_scalar::install(", "Arc::clone(&self.projections)")
                .doesNotContain("Arc::new(self.projections.clone())");
        String expandStream =
                Files.readString(Path.of("../streamfusion-native/src/planner/operators/expand/stream.rs"));
        assertThat(expandStream)
                .contains("managed_expression::evaluate_projection(")
                .doesNotContain("descriptor_allowance(&self.batch)")
                .doesNotContain("downcast_ref::<Literal>", "literal.value().size()");
    }
}
