/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.streamfusion.proto.plan.v1.NativeStateBindings;

class NativeStateResourcesTest {
    @Test
    void preservesStateAssignmentsAndRestoredClockWhileBindingAllFlinkSpillDirectories(@TempDir Path root)
            throws Exception {
        var binding = NativeStateResources.memory(2, 16, 0, 15).toBuilder()
                .setRestoredWatermark(1234)
                .build();
        var first = root.resolve("first/../first");
        var second = root.resolve("second");
        var decoded =
                NativeStateBindings.parseFrom(NativeStateResources.serialize(List.of(binding), List.of(first, second)));
        assertThat(decoded.getProtocolVersion()).isEqualTo(4);
        assertThat(decoded.getBindingsList()).containsExactly(binding);
        assertThat(decoded.getSpillDirectoriesList())
                .containsExactly(first.normalize().toString(), second.toString());
        var legacy = NativeStateBindings.parseFrom(NativeStateResources.serialize(List.of(binding)));
        assertThat(legacy.getProtocolVersion()).isEqualTo(3);
        assertThat(legacy.getSpillDirectoriesList()).isEmpty();
        assertThatThrownBy(() -> NativeStateResources.serialize(List.of(binding), List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }
}
