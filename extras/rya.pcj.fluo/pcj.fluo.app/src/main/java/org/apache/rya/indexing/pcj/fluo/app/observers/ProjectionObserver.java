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
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.PROJECTION_PREFIX;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.ProjectionMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;

/**
 * Performs incremental result exporting to the configured destinations.
 */
public class ProjectionObserver extends BindingSetUpdater {
    private static final Logger log = Logger.getLogger(ProjectionObserver.class);

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.PROJECTION_BINDING_SET, NotificationType.STRONG);
    }

    @Override
    public Observation parseObservation(final TransactionBase tx, final Bytes row) throws Exception {
        requireNonNull(tx);
        requireNonNull(row);

        // Read the Filter metadata.
        final String projectionNodeId = BindingSetRow.makeFromShardedRow(Bytes.of(PROJECTION_PREFIX), row).getNodeId();
        final ProjectionMetadata projectionMetadata = queryDao.readProjectionMetadata(tx, projectionNodeId);

        // Read the Visibility Binding Set from the value.
        final Bytes valueBytes = tx.get(row, FluoQueryColumns.PROJECTION_BINDING_SET);
        final VisibilityBindingSet projectionBindingSet = BS_SERDE.deserialize(valueBytes);

        // Figure out which node needs to handle the new metadata.
        final String parentNodeId = projectionMetadata.getParentNodeId();

        return new Observation(projectionNodeId, projectionBindingSet, parentNodeId);
    }
}
