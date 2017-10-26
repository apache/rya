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

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporter;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataCache;
import org.apache.rya.indexing.pcj.fluo.app.query.MetadataCacheSupplier;

/**
 * Monitors the Column {@link FluoQueryColumns#CONSTRUCT_STATEMENTS} for new
 * Construct Query {@link RyaStatement}s and exports the results using the
 * {@link IncrementalRyaSubGraphExporter}s that are registered with this
 * Observer.
 *
 */
public class ConstructQueryResultObserver extends AbstractObserver {

    private static final Logger log = Logger.getLogger(ConstructQueryResultObserver.class);
    protected final FluoQueryMetadataCache queryDao = MetadataCacheSupplier.getOrCreateCache();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.CONSTRUCT_STATEMENTS, NotificationType.STRONG);
    }

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

        //Build row for parent that result will be written to
        BindingSetRow bsRow = BindingSetRow.make(row);
        String constructNodeId = bsRow.getNodeId();
        String bsString= bsRow.getBindingSetString();
        String parentNodeId = queryDao.readMetadadataEntry(tx, constructNodeId, FluoQueryColumns.CONSTRUCT_PARENT_NODE_ID).toString();
        String rowString = parentNodeId + IncrementalUpdateConstants.NODEID_BS_DELIM + bsString;

        //Get NodeType of the parent node
        NodeType parentType = NodeType.fromNodeId(parentNodeId).get();
        //Get data for the ConstructQuery result
        Bytes bytes = tx.get(row, col);
        //Write result to parent
        tx.set(Bytes.of(rowString), parentType.getResultColumn(), bytes);
    }

}
