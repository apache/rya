package org.apache.rya.indexing.pcj.fluo.app;

import java.util.Optional;
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
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerDe;
import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadataImpl;
import org.apache.rya.indexing.pcj.fluo.app.query.ConstructQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.util.AggregationStateUtil;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

/**
 * This class creates results for the ConstructQuery. This class applies the {@link ConstructGraph} associated with the
 * Construct Query to generate a collection of {@link RyaStatement}s. These statements are then used to form a
 * {@link RyaSubGraph} that is serialized and stored as a value in the Column
 * {@link FluoQueryColumns#CONSTRUCT_STATEMENTS}.
 *
 */
public class ConstructQueryResultUpdater {

    private static final Logger log = Logger.getLogger(ConstructQueryResultUpdater.class);
    private static final RyaSubGraphKafkaSerDe serializer = new RyaSubGraphKafkaSerDe();

    /**
     * Updates the Construct Query results by applying the {@link ConnstructGraph} to create a {@link RyaSubGraph} and
     * then writing the subgraph to {@link FluoQueryColumns#CONSTRUCT_STATEMENTS}.
     * 
     * @param tx - transaction used to write the subgraph
     * @param bs - BindingSet that the ConstructProjection expands into a subgraph
     * @param metadata - metadata that the ConstructProjection is extracted from
     */
    public void updateConstructQueryResults(TransactionBase tx, VisibilityBindingSet bs, ConstructQueryMetadata metadata) {

        if (AggregationStateUtil.checkAggregationState(tx, bs, metadata)) {
            String nodeId = metadata.getNodeId();
            VariableOrder varOrder = metadata.getVariableOrder();
            Column column = FluoQueryColumns.CONSTRUCT_STATEMENTS;
            ConstructGraph graph = metadata.getConstructGraph();
            String parentId = metadata.getParentNodeId();

            // Create the Row Key for the emitted binding set. It does not contain visibilities.
            final Bytes resultRow = RowKeyUtil.makeRowKey(nodeId, varOrder, bs);

            // If this is a new binding set, then emit it.
            if (tx.get(resultRow, column) == null || varOrder.getVariableOrders().size() < bs.size()) {
                Set<RyaStatement> statements = graph.createGraphFromBindingSet(bs);
                RyaSubGraph subgraph = new RyaSubGraph(parentId, statements);
                final Bytes nodeValueBytes = Bytes.of(serializer.toBytes(subgraph));

                log.trace("Transaction ID: " + tx.getStartTimestamp() + "\n" + "New Binding Set: " + subgraph + "\n");

                tx.set(resultRow, column, nodeValueBytes);
            }
        }
    }

}
