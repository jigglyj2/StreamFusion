/*
 * Copyright 2026 StreamFusion Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.github.nexmark.flink.source;

import com.github.nexmark.flink.generator.GeneratorConfig;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

/** Corrects the boundedness advertised by the finite local Nexmark generator. */
final class StreamFusionBoundedNexmarkSource extends NexmarkSource {
    private final GeneratorConfig config;

    StreamFusionBoundedNexmarkSource(GeneratorConfig config, TypeInformation<RowData> outputType) {
        super(config, outputType);
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, NexmarkSourceSplit> createReader(SourceReaderContext context) {
        return new StreamFusionDeterministicNexmarkSourceReader(context, config);
    }

    @Override
    public SplitEnumerator<NexmarkSourceSplit, Collection<NexmarkSourceSplit>> createEnumerator(
            SplitEnumeratorContext<NexmarkSourceSplit> context) {
        return new BoundedSplitEnumerator(context, getSplits(context.currentParallelism()));
    }

    @Override
    public SplitEnumerator<NexmarkSourceSplit, Collection<NexmarkSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<NexmarkSourceSplit> context, Collection<NexmarkSourceSplit> splits) {
        return new BoundedSplitEnumerator(context, splits);
    }

    /** Assigns one finite split per reader and explicitly closes split discovery. */
    private static final class BoundedSplitEnumerator
            implements SplitEnumerator<NexmarkSourceSplit, Collection<NexmarkSourceSplit>> {
        private final SplitEnumeratorContext<NexmarkSourceSplit> context;
        private final LinkedList<NexmarkSourceSplit> remainingSplits;

        private BoundedSplitEnumerator(
                SplitEnumeratorContext<NexmarkSourceSplit> context, Collection<NexmarkSourceSplit> splits) {
            this.context = context;
            this.remainingSplits = new LinkedList<>(splits);
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            if (!context.registeredReaders().containsKey(subtaskId)) {
                return;
            }
            NexmarkSourceSplit split = remainingSplits.pollFirst();
            Preconditions.checkState(split != null, "No Nexmark split remains for subtask %s", subtaskId);
            context.assignSplit(split, subtaskId);
            context.signalNoMoreSplits(subtaskId);
        }

        @Override
        public void addSplitsBack(List<NexmarkSourceSplit> splits, int subtaskId) {
            remainingSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Collection<NexmarkSourceSplit> snapshotState(long checkpointId) {
            return new ArrayList<>(remainingSplits);
        }

        @Override
        public void close() {}
    }
}
