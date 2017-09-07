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
package org.apache.rya.periodic.notification.pruner;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.periodic.notification.api.BinPruner;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link BinPruner} that deletes old, already processed
 * Periodic Query results from Fluo and the PCJ table to which the Fluo results
 * are exported.
 *
 */
public class PeriodicQueryPruner implements BinPruner, Runnable {

    private static final Logger log = LoggerFactory.getLogger(PeriodicQueryPruner.class);
    private final FluoClient client;
    private final AccumuloBinPruner accPruner;
    private final FluoBinPruner fluoPruner;
    private final BlockingQueue<NodeBin> bins;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int threadNumber;

    public PeriodicQueryPruner(final FluoBinPruner fluoPruner, final AccumuloBinPruner accPruner, final FluoClient client, final BlockingQueue<NodeBin> bins, final int threadNumber) {
        this.fluoPruner = Objects.requireNonNull(fluoPruner);
        this.accPruner = Objects.requireNonNull(accPruner);
        this.client = Objects.requireNonNull(client);
        this.bins = Objects.requireNonNull(bins);
        this.threadNumber = threadNumber;
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                pruneBindingSetBin(bins.take());
            }
        } catch (final InterruptedException e) {
            log.warn("Thread {} is unable to prune the next message.", threadNumber);
            throw new RuntimeException(e);
        }
    }

    /**
     * Prunes BindingSet bins from the Rya Fluo Application in addition to the BindingSet
     * bins created in the PCJ tables associated with the give query id.
     * @param id - QueryResult Id for the Rya Fluo application
     * @param bin - bin id for bins to be deleted
     */
    @Override
    public void pruneBindingSetBin(final NodeBin nodeBin) {
        final String pcjId = nodeBin.getNodeId();
        final long bin = nodeBin.getBin();
        try(Snapshot sx = client.newSnapshot()) {
            final String queryId = NodeType.generateNewIdForType(NodeType.QUERY, pcjId);
            final Set<String> fluoIds = getNodeIdsFromResultId(sx, queryId);
            accPruner.pruneBindingSetBin(nodeBin);
            for(final String fluoId: fluoIds) {
                fluoPruner.pruneBindingSetBin(new NodeBin(fluoId, bin));
            }
        } catch (final Exception e) {
            log.warn("Could not successfully initialize PeriodicQueryBinPruner.", e);
        }
    }


    public void shutdown() {
        closed.set(true);
    }

    private Set<String> getNodeIdsFromResultId(final SnapshotBase sx, final String id) {
        final Set<String> ids = new HashSet<>();
        PeriodicQueryUtil.getPeriodicQueryNodeAncestorIds(sx, id, ids);
        return ids;
    }


}
