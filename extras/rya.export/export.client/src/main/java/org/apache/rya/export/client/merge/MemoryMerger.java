/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.export.client.merge;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.Merger;
import org.apache.rya.export.api.StatementMerger;
import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.apache.rya.export.api.metadata.ParentMetadataExistsException;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.ContainsStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.future.RepSynchRunnable;
import org.apache.rya.export.client.merge.future.RepSynchRunnableFactory;
import org.apache.rya.export.client.merge.future.RepSynchRunnableFactory.StatementListener;

/**
 * An in memory {@link Merger}.  Merges {@link RyaStatement}s from a parent
 * to a child.  The statements merged will be any that have a timestamp after
 * the provided time.  If there are any conflicting statements, the provided
 * {@link StatementMerger} will merge the statements and produce the desired
 * {@link RyaStatement}.
 */
public class MemoryMerger implements Merger {
    private static final Logger LOG = Logger.getLogger(MemoryMerger.class);

    private final RyaStatementStore sourceStore;
    private final RyaStatementStore destStore;
    private final String ryaInstanceName;
    private final Long timeOffset;

    private final RepSynchRunnableFactory factory;
    private final ExecutorService repSynchExecutor;

    private RepSynchRunnable repSynchTask;
    private Future<?> future;

    /**
     * Creates a new {@link MemoryMerger} to merge the statements from the parent to a child.
     *
     * @param parentStore - The source store, where the statements are coming from. (not null)
     * @param childStore - The destination store, where the statements are going. (not null)
     * @param ryaInstanceName - The Rya instance to merge. (not null)
     * @param timeOffset - The offset from a potential NTP server. (not null)
     */
    public MemoryMerger(final RyaStatementStore parentStore, final RyaStatementStore childStore,
            final String ryaInstanceName, final Long timeOffset) {
        sourceStore = checkNotNull(parentStore);
        destStore = checkNotNull(childStore);
        this.ryaInstanceName = checkNotNull(ryaInstanceName);
        this.timeOffset = checkNotNull(timeOffset);

        factory = RepSynchRunnableFactory.getInstance();
        repSynchExecutor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void runJob() {
        final Optional<MergeParentMetadata> metadata = sourceStore.getParentMetadata();

        //check the source for a parent metadata
        if(metadata.isPresent()) {
            LOG.info("Importing statements...");
            final MergeParentMetadata parentMetadata = metadata.get();
            if(parentMetadata.getRyaInstanceName().equals(ryaInstanceName)) {
                try {
                    importStatements(parentMetadata);
                } catch (AddStatementException | ContainsStatementException | RemoveStatementException | FetchStatementException e) {
                    LOG.error("Failed to import statements.", e);
                }
            }
        } else {
            final Optional<MergeParentMetadata> synchMetadata = destStore.getParentMetadata();
            //if the metadata exists, the destination store is going to be synched from the source store if the source is the original parent
            if(synchMetadata.isPresent() &&
                    synchMetadata.get().getRyaInstanceName().equals(sourceStore.getRyaInstanceName())) {
                LOG.info("Synching child store...");
                synchExistingStore();

            } else {
                try {
                    LOG.info("Replicating store...");
                    replicate();
                } catch (final ParentMetadataExistsException | FetchStatementException e) {
                    LOG.error("Failed to replicate store.", e);
                }
            }
        }
    }

    /**
     * Exports all statements after the provided timestamp.
     * @throws ParentMetadataExistsException -
     * @throws FetchStatementException
     */
    private void replicate() throws ParentMetadataExistsException, FetchStatementException {
        LOG.info("Creating parent metadata in the child.");
        //setup parent metadata repo in the child
        final MergeParentMetadata metadata = new MergeParentMetadata.Builder()
            .setRyaInstanceName(ryaInstanceName)
            .setTimestamp(new Date())
            .setParentTimeOffset(timeOffset)
            .build();
        destStore.setParentMetadata(metadata);

        repSynchTask = factory.getReplication(sourceStore, destStore);
        future = repSynchExecutor.submit(repSynchTask);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to replicate Rya instance.");
        }
    }

    private void importStatements(final MergeParentMetadata metadata) throws AddStatementException, ContainsStatementException, RemoveStatementException, FetchStatementException {
        LOG.info("Importing statements.");
        repSynchTask = factory.getImport(sourceStore, destStore, metadata, timeOffset);
        future = repSynchExecutor.submit(repSynchTask);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to replicate Rya instance.");
        }
    }

    /**
     * Synchronizes the source store with the destination store.
     */
    private void synchExistingStore() {
        repSynchTask = factory.getSynchronize(sourceStore, destStore, timeOffset);
        future = repSynchExecutor.submit(repSynchTask);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed to replicate Rya instance.");
        }
    }

    /**
     * Cancels the current running rep/synch job.
     */
    public void cancel() {
        if(repSynchTask != null) {
            repSynchTask.cancel();
            future.cancel(true);
        }
    }

    /**
     * @param listener - The listener added to be notified when a statement has been rep/synched. (not null)
     */
    public void addStatementListener(final StatementListener listener) {
        requireNonNull(listener);
        factory.addStatementListener(listener);
    }

    /**
     * @param listener - The listener to remove. (not null)
     */
    public void removeStatementListener(final StatementListener listener) {
        requireNonNull(listener);
        factory.removeStatementListener(listener);
    }
}
