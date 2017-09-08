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

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformationDAO;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;
import org.apache.rya.periodic.notification.api.BinPruner;
import org.apache.rya.periodic.notification.api.NodeBin;

import com.google.common.base.Optional;

/**
 * Deletes {@link BindingSet}s from the indicated Fluo table.
 */
public class FluoBinPruner implements BinPruner {

    private static final Logger log = Logger.getLogger(FluoBinPruner.class);
    private FluoClient client;

    public FluoBinPruner(FluoClient client) {
        this.client = client;
    }

    /**
     * This method deletes BindingSets in the specified bin from the BindingSet
     * Column of the indicated Fluo nodeId
     * 
     * @param id
     *            - Fluo nodeId
     * @param bin
     *            - bin id
     */
    @Override
    public void pruneBindingSetBin(NodeBin nodeBin) {
        String id = nodeBin.getNodeId();
        long bin = nodeBin.getBin();
        try (Transaction tx = client.newTransaction()) {
            Optional<NodeType> type = NodeType.fromNodeId(id);
            if (!type.isPresent()) {
                log.trace("Unable to determine NodeType from id: " + id);
                throw new RuntimeException();
            }
            Column batchInfoColumn = type.get().getResultColumn();
            String batchInfoSpanPrefix = id + IncrementalUpdateConstants.NODEID_BS_DELIM + bin;
            SpanBatchDeleteInformation batchInfo = SpanBatchDeleteInformation.builder().setColumn(batchInfoColumn)
                    .setSpan(Span.prefix(Bytes.of(batchInfoSpanPrefix))).build();
            BatchInformationDAO.addBatch(tx, id, batchInfo);
            tx.commit();
        }
    }

}
