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

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.VisibilityBindingSetSerDe;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.BindingSet;

/**
 * Notified when the results of a Statement Pattern have been updated to include
 * a new {@link BindingSet}. This observer updates its parent if the new
 * Binding Set effects the parent's results.
 */
public class StatementPatternObserver extends BindingSetUpdater {

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();

    // DAO
    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET, NotificationType.STRONG);
    }

    @Override
    public Observation parseObservation(final TransactionBase tx, final Bytes row) throws Exception {
        requireNonNull(tx);
        requireNonNull(row);

        // Read the Statement Pattern metadata.
        final String spNodeId = BindingSetRow.make(row).getNodeId();
        final StatementPatternMetadata spMetadata = queryDao.readStatementPatternMetadata(tx, spNodeId);

        // Read the Visibility Binding Set from the value.
        final Bytes valueBytes = tx.get(row, FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET);
        final VisibilityBindingSet spBindingSet = BS_SERDE.deserialize(valueBytes);

        // Figure out which node needs to handle the new metadata.
        final String parentNodeId = spMetadata.getParentNodeId();

        return new Observation(spNodeId, spBindingSet, parentNodeId);
    }
}