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
package org.apache.rya.indexing.pcj.fluo.app.observers;

import static java.util.Objects.requireNonNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.PERIODIC_QUERY_PREFIX;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.PeriodicQueryUpdater;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;

/**
 * This Observer is responsible for assigning Periodic Bin Ids to BindingSets.
 * This class delegates to the {@link BindingSetUpdater} process method, which
 * uses the {@link PeriodicQueryUpdater} to extract the time stamp from the BindingSet.
 * The PeriodicQueryUpdater creates one instance of the given BindingSet for each bin
 * that the time stamp is assigned to by the updater, and these BindingSets are written
 * to the parent node of the given PeriodicQueryMetadata node.
 *
 */
public class PeriodicQueryObserver extends BindingSetUpdater {

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.PERIODIC_QUERY_BINDING_SET, NotificationType.STRONG);
    }

    @Override
    public Observation parseObservation(final TransactionBase tx, final Bytes row) throws Exception {
        requireNonNull(tx);
        requireNonNull(row);

        // Read the Join metadata.
        final String periodicBinNodeId = BindingSetRow.makeFromShardedRow(Bytes.of(PERIODIC_QUERY_PREFIX), row).getNodeId();
        final PeriodicQueryMetadata periodicBinMetadata = queryDao.readPeriodicQueryMetadata(tx, periodicBinNodeId);

        // Read the Visibility Binding Set from the Value.
        final Bytes valueBytes = tx.get(row, FluoQueryColumns.PERIODIC_QUERY_BINDING_SET);
        final VisibilityBindingSet periodicBinBindingSet = BS_SERDE.deserialize(valueBytes);

        // Figure out which node needs to handle the new metadata.
        final String parentNodeId = periodicBinMetadata.getParentNodeId();

        return new Observation(periodicBinNodeId, periodicBinBindingSet, parentNodeId);
    }


}
