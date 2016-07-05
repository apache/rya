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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;
import org.openrdf.query.BindingSet;

import io.fluo.api.client.TransactionBase;

/**
 * Notified when the results of a Statement Pattern have been updated to include
 * a new {@link BindingSet}. This observer updates its parent if the new
 * Binding Set effects the parent's results.
 */
public class StatementPatternObserver extends BindingSetUpdater {

    private static final VisibilityBindingSetStringConverter CONVERTER = new VisibilityBindingSetStringConverter();

    // DAO
    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET, NotificationType.STRONG);
    }

    @Override
    public Observation parseObservation(final TransactionBase tx, final BindingSetRow parsedRow) {
        checkNotNull(tx);

        // Read the Statement Pattern metadata.
        final String spNodeId = parsedRow.getNodeId();
        final StatementPatternMetadata spMetadata = queryDao.readStatementPatternMetadata(tx, spNodeId);
        final String bindingSetValue = parsedRow.getBindingSetString();

        // Read the Binding Set that was just emmitted by the Statement Pattern.
        final VariableOrder spVarOrder = spMetadata.getVariableOrder();
        final VisibilityBindingSet spBindingSet = (VisibilityBindingSet) CONVERTER.convert(bindingSetValue, spVarOrder);

        // Figure out which node needs to handle the new metadata.
        final String parentNodeId = spMetadata.getParentNodeId();

        return new Observation(spNodeId, spBindingSet, parentNodeId);
    }
}