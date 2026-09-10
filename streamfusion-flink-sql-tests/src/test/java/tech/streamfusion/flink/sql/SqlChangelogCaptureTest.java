/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.flink.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

class SqlChangelogCaptureTest {
    private static final DataType STRINGS =
            DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING()), DataTypes.FIELD("b", DataTypes.STRING()));

    @Test
    void distinguishesNullAndStringAndFieldBoundariesThatShareDisplayText() throws Exception {
        assertThat(encode(Row.of(null, "x"))).isNotEqualTo(encode(Row.of("null", "x")));
        assertThat(encode(Row.of("a, b", "c"))).isNotEqualTo(encode(Row.of("a", "b, c")));
    }

    @Test
    void preservesEveryChangelogTransitionAndItsOrder() throws Exception {
        Row before = Row.ofKind(RowKind.UPDATE_BEFORE, "a", "old");
        Row after = Row.ofKind(RowKind.UPDATE_AFTER, "a", "new");
        assertThat(encode(before, after)).isNotEqualTo(encode(after, before));
        assertThat(encode(Row.of("a", "new"))).isNotEqualTo(encode(after));
        assertThat(encode(before, after, after)).isNotEqualTo(encode(before, after));
        assertThatThrownBy(() -> SqlChangelogCapture.encode(
                        STRINGS, List.of(before, after).iterator(), SqlChangelogCapture.Order.UNORDERED_INSERTS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unorderedInsertComparisonIsExplicitAndKeepsDuplicates() throws Exception {
        Row a = Row.of("a", "1");
        Row b = Row.of("b", "2");
        assertThat(encode(a, b)).isNotEqualTo(encode(b, a));
        assertThat(unordered(a, b)).isEqualTo(unordered(b, a));
        assertThat(unordered(a, b)).isNotEqualTo(unordered(a, b, b));
    }

    @Test
    void usesLogicalTypesForNestedBinaryAndTimestampValues() throws Exception {
        DataType type = DataTypes.ROW(
                DataTypes.FIELD("nested", DataTypes.ARRAY(DataTypes.BYTES())),
                DataTypes.FIELD("time", DataTypes.TIMESTAMP(9)));
        var time = java.time.LocalDateTime.of(2026, 9, 10, 1, 2, 3, 123456789);
        Row a = Row.of(new byte[][] {{0, 1}, null, {2}}, time);
        Row b = Row.of(new byte[][] {{0, 1}, null, {2}}, time.plusNanos(1));
        byte[] bytes = SqlChangelogCapture.encode(type, List.of(a).iterator(), SqlChangelogCapture.Order.CHANGELOG);
        assertThat(bytes)
                .isEqualTo(
                        SqlChangelogCapture.encode(type, List.of(a).iterator(), SqlChangelogCapture.Order.CHANGELOG));
        assertThat(bytes)
                .isNotEqualTo(
                        SqlChangelogCapture.encode(type, List.of(b).iterator(), SqlChangelogCapture.Order.CHANGELOG));
    }

    private static byte[] encode(Row... rows) throws Exception {
        return SqlChangelogCapture.encode(STRINGS, List.of(rows).iterator(), SqlChangelogCapture.Order.CHANGELOG);
    }

    private static byte[] unordered(Row... rows) throws Exception {
        return SqlChangelogCapture.encode(
                STRINGS, List.of(rows).iterator(), SqlChangelogCapture.Order.UNORDERED_INSERTS);
    }
}
