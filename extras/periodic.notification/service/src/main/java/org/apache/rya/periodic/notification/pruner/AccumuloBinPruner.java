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

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.periodic.notification.api.BinPruner;
import org.apache.rya.periodic.notification.api.NodeBin;

import com.google.common.base.Preconditions;

/**
 * Deletes BindingSets from time bins in the indicated PCJ table
 */
public class AccumuloBinPruner implements BinPruner {

    private static final Logger log = Logger.getLogger(AccumuloBinPruner.class);
    private PeriodicQueryResultStorage periodicStorage;

    public AccumuloBinPruner(PeriodicQueryResultStorage periodicStorage) {
        Preconditions.checkNotNull(periodicStorage);
        this.periodicStorage = periodicStorage;
    }

    /**
     * This method deletes all BindingSets in the indicated bin from the PCJ
     * table indicated by the id. It is assumed that all BindingSet entries for
     * the corresponding bin are written to the PCJ table so that the bin Id
     * occurs first.
     * 
     * @param id
     *            - pcj table id
     * @param bin
     *            - temporal bin the BindingSets are contained in
     */
    @Override
    public void pruneBindingSetBin(NodeBin nodeBin) {
        Preconditions.checkNotNull(nodeBin);
        String id = nodeBin.getNodeId();
        long bin = nodeBin.getBin();
        try {
            periodicStorage.deletePeriodicQueryResults(id, bin);
        } catch (PeriodicQueryStorageException e) {
            log.trace("Unable to delete results from Peroidic Table: " + id + " for bin: " + bin);
            throw new RuntimeException(e);
        }
    }

}
