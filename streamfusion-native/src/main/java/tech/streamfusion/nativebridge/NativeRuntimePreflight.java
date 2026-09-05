/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package tech.streamfusion.nativebridge;

/** Planner-time verification that the packaged native runtime can be loaded on this host. */
public final class NativeRuntimePreflight {
    private NativeRuntimePreflight() {}

    public static void verify() {
        NativeLibraryLoader.load();
    }
}
