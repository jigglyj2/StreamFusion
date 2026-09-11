/* Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0. */
package tech.streamfusion.nativebridge;

import java.util.ArrayList;
import java.util.List;

/** Binds native output routes once; the context retains routers after these setup handles close. */
public final class NativeExchangeOutputs {
    private NativeExchangeOutputs() {}

    public static void bind(
            NativeExecutionContext context,
            int[] ports,
            List<byte[]> plans,
            boolean[] arrow,
            NativeMemoryManager memory) {
        if (!context.hasOwnedOutputEnvelope() || ports.length != plans.size())
            throw new IllegalArgumentException("Exchange outputs require owned envelopes and matching bindings");
        if (NativeRegionStream.edgeVersion() != 4)
            throw new IllegalStateException("Unsupported native framed output edge version");
        var routers = new ArrayList<NativeExchangeRouter>();
        try {
            long[] handles = new long[ports.length];
            for (int i = 0; i < ports.length; i++) {
                var router = new NativeExchangeRouter(plans.get(i), memory);
                routers.add(router);
                handles[i] = router.handle();
            }
            int[] flags = new int[arrow.length];
            for (int i = 0; i < arrow.length; i++) flags[i] = arrow[i] ? 1 : 0;
            bindNative(context.handle(), ports, handles, flags);
        } finally {
            for (int i = routers.size() - 1; i >= 0; i--) routers.get(i).close();
        }
    }

    private static native void bindNative(long context, int[] ports, long[] routers, int[] arrow);
}
