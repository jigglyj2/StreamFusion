/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.flink.calc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionKind;
import org.junit.jupiter.api.Test;

class BuiltinScalarCoverageTest {
    @Test
    void everyPublicFlinkScalarHasAnAccelerationDecision() throws Exception {
        Set<String> actual = new TreeSet<>();
        for (Field field : BuiltInFunctionDefinitions.class.getFields()) {
            if (field.getType() != BuiltInFunctionDefinition.class) {
                continue;
            }
            BuiltInFunctionDefinition function = (BuiltInFunctionDefinition) field.get(null);
            if (function.getKind() == FunctionKind.SCALAR && !function.isInternal()) {
                actual.add(field.getName());
            }
        }

        InputStream resource = getClass().getResourceAsStream("/flink-2.3-scalar-decisions.txt");
        assertThat(resource).isNotNull();
        Set<String> decisions;
        Set<String> categories = new HashSet<>(Arrays.asList("NATIVE", "FALLBACK", "REWRITE", "NON_CALC"));
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8))) {
            decisions = reader.lines()
                    .map(String::trim)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .peek(line -> assertThat(categories).contains(line.split(" ", 2)[0]))
                    .map(line -> line.split(" ", 2)[1])
                    .collect(Collectors.toCollection(TreeSet::new));
        }

        assertThat(decisions).hasSameSizeAs(actual);
        assertThat(decisions).isEqualTo(actual);
    }
}
