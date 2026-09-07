/*
 * Copyright 2026 StreamFusion Authors
 * Licensed under the Apache License, Version 2.0
 */
package tech.streamfusion.flink.proto;

import com.google.protobuf.Message;
import java.util.ArrayList;
import java.util.List;
import tech.streamfusion.proto.plan.v1.Operator;

/** Physical edges in the versioned protobuf, shared by control and metric tree traversal. */
public final class NativePhysicalPlan {
    private NativePhysicalPlan() {}

    public static List<Operator> children(Operator operator) {
        List<Operator> result = new ArrayList<>();
        collectChildren(operator, result);
        return result;
    }

    private static void collectChildren(Message message, List<Operator> result) {
        for (Object value : message.getAllFields().values()) {
            if (value instanceof Operator) result.add((Operator) value);
            else if (value instanceof Message) collectChildren((Message) value, result);
            else if (value instanceof Iterable<?>) {
                for (Object item : (Iterable<?>) value) {
                    if (item instanceof Operator) result.add((Operator) item);
                    else if (item instanceof Message) collectChildren((Message) item, result);
                }
            }
        }
    }
}
