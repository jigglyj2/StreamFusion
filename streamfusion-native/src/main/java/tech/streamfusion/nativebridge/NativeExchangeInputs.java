/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable task-lifetime exchange schemas, bound once per native input port. */
final class NativeExchangeInputs {
    private final NativeExecutionContext context;
    private final Map<Integer, byte[]> plans = new HashMap<>();

    NativeExchangeInputs(NativeExecutionContext context) {
        this.context = context;
    }

    synchronized void prepare(int port, byte[] plan) {
        long handle = context.handle();
        Objects.requireNonNull(plan, "exchange plan");
        byte[] existing = plans.get(port);
        if (existing != null) {
            if (!Arrays.equals(existing, plan))
                throw new IllegalArgumentException("Native exchange plan changed for input port " + port);
            return;
        }
        if (edgeVersion() != 1) throw new IllegalStateException("Unsupported native exchange input edge version");
        byte[] owned = plan.clone();
        prepare(handle, port, owned);
        plans.put(port, owned);
    }

    static native int edgeVersion();

    private static native void prepare(long handle, int port, byte[] plan);
}
