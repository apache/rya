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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;
import org.openrdf.query.Binding;
import org.openrdf.query.impl.MapBindingSet;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.Encoder;
import io.fluo.api.types.StringEncoder;

/**
 * Updates the results of a Query node when one of its children has added a
 * new Binding Set to its results.
 */
@ParametersAreNonnullByDefault
public class QueryResultUpdater {
    private final Encoder encoder = new StringEncoder();

    private final BindingSetStringConverter converter = new BindingSetStringConverter();
    private final VisibilityBindingSetStringConverter valueConverter = new VisibilityBindingSetStringConverter();

    /**
     * Updates the results of a Query node when one of its children has added a
     * new Binding Set to its results.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childBindingSet - A binding set that the query's child node has emmitted. (not null)
     * @param queryMetadata - The metadata of the Query whose results will be updated. (not null)
     */
    public void updateQueryResults(
            final TransactionBase tx,
            final VisibilityBindingSet childBindingSet,
            final QueryMetadata queryMetadata) {
        checkNotNull(tx);
        checkNotNull(childBindingSet);
        checkNotNull(queryMetadata);

        // Create the query's Binding Set from the child node's binding set.
        final VariableOrder queryVarOrder = queryMetadata.getVariableOrder();

        final MapBindingSet queryBindingSet = new MapBindingSet();
        for(final String bindingName : queryVarOrder) {
            if(childBindingSet.hasBinding(bindingName)) {
                final Binding binding = childBindingSet.getBinding(bindingName);
                queryBindingSet.addBinding(binding);
            }
        }
        final String queryBindingSetString = converter.convert(queryBindingSet, queryVarOrder);
        final String queryBindingSetValueString = valueConverter.convert(new VisibilityBindingSet(queryBindingSet, childBindingSet.getVisibility()), queryVarOrder);

        // Commit it to the Fluo table for the SPARQL query. This isn't guaranteed to be a new entry.
        final Bytes row = encoder.encode(queryMetadata.getNodeId() + NODEID_BS_DELIM + queryBindingSetString);
        final Column col = FluoQueryColumns.QUERY_BINDING_SET;
        final Bytes value = encoder.encode(queryBindingSetValueString);
        tx.set(row, col, value);
    }
}