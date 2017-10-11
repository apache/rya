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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.periodic.notification.api.BinPruner;
import org.apache.rya.periodic.notification.api.NodeBin;

import com.google.common.base.Preconditions;

/**
 * Implementation of {@link BinPruner} that deletes old, already processed
 * Periodic Query results from Fluo and the PCJ table to which the Fluo results
 * are exported.
 *
 */
public class PeriodicQueryPruner implements BinPruner, Runnable {

    private static final Logger log = Logger.getLogger(PeriodicQueryPruner.class);
    private FluoClient client;
    private AccumuloBinPruner accPruner;
    private FluoBinPruner fluoPruner;
    private BlockingQueue<NodeBin> bins;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private int threadNumber;

    public PeriodicQueryPruner(FluoBinPruner fluoPruner, AccumuloBinPruner accPruner, FluoClient client, BlockingQueue<NodeBin> bins, int threadNumber) {
        this.fluoPruner = Preconditions.checkNotNull(fluoPruner);
        this.accPruner = Preconditions.checkNotNull(accPruner);
        this.client = Preconditions.checkNotNull(client);
        this.bins = Preconditions.checkNotNull(bins);
        this.threadNumber = threadNumber;
    }
    
    @Override
    public void run() {
        try {
            while (!closed.get()) {
                pruneBindingSetBin(bins.take());
            }
        } catch (InterruptedException e) {
            log.trace("Thread " + threadNumber + " is unable to prune the next message.");
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
    public void pruneBindingSetBin(NodeBin nodeBin) {
        String pcjId = nodeBin.getNodeId();
        long bin = nodeBin.getBin();
        try(Snapshot sx = client.newSnapshot()) {
            String queryId = NodeType.generateNewIdForType(NodeType.QUERY, pcjId);
            Set<String> fluoIds = getNodeIdsFromResultId(sx, queryId);
            accPruner.pruneBindingSetBin(nodeBin);
            for(String fluoId: fluoIds) {
                fluoPruner.pruneBindingSetBin(new NodeBin(fluoId, bin));
            }
        } catch (Exception e) {
            log.trace("Could not successfully initialize PeriodicQueryBinPruner.");
        }
    }
    
    
    public void shutdown() {
        closed.set(true);
    }

    private Set<String> getNodeIdsFromResultId(SnapshotBase sx, String id) {
        Set<String> ids = new HashSet<>();
        PeriodicQueryUtil.getPeriodicQueryNodeAncestorIds(sx, id, ids);
        return ids;
    }


}
