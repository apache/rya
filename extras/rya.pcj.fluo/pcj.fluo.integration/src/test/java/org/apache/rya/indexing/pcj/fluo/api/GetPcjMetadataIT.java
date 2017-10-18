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
package org.apache.rya.indexing.pcj.fluo.api;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.pcj.fluo.api.GetPcjMetadata.NotInAccumuloException;
import org.apache.rya.indexing.pcj.fluo.api.GetPcjMetadata.NotInFluoException;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests the methods of {@link GetPcjMetadata}.
 */
public class GetPcjMetadataIT extends RyaExportITBase {

    @Test
    public void getMetadataByQueryId() throws RepositoryException, MalformedQueryException, SailException, QueryEvaluationException, PcjException, NotInFluoException, NotInAccumuloException, RyaDAOException, UnsupportedQueryException {
        final String sparql =
                "SELECT ?x " +
                  "WHERE { " +
                  "?x <http://talksTo> <http://Eve>. " +
                  "?x <http://worksAt> <http://Chipotle>." +
                "}";

        // Create the PCJ table.
        final Connector accumuloConn = super.getAccumuloConnector();
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
        final String pcjId = pcjStorage.createPcj(sparql);

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Tell the Fluo app to maintain the PCJ.
            new CreateFluoPcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());

            // Fetch the PCJ's Metadata through the GetPcjMetadata interactor.
            final String queryId = new ListQueryIds().listQueryIds(fluoClient).get(0);
            final PcjMetadata metadata = new GetPcjMetadata().getMetadata(pcjStorage, fluoClient, queryId);

            // Ensure the command returns the correct metadata.
            final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(sparql);
            final PcjMetadata expected = new PcjMetadata(sparql, 0L, varOrders);
            assertEquals(expected, metadata);
        }
    }

    @Test
    public void getAllMetadata() throws MalformedQueryException, SailException, QueryEvaluationException, PcjException, NotInFluoException, NotInAccumuloException, AccumuloException, AccumuloSecurityException, RyaDAOException, UnsupportedQueryException {
        final Connector accumuloConn = super.getAccumuloConnector();
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Add a couple of queries to Accumulo.
            final String q1Sparql =
                    "SELECT ?x " +
                            "WHERE { " +
                            "?x <http://talksTo> <http://Eve>. " +
                            "?x <http://worksAt> <http://Chipotle>." +
                            "}";
            final String q1PcjId = pcjStorage.createPcj(q1Sparql);
            final CreateFluoPcj createPcj = new CreateFluoPcj();
            createPcj.withRyaIntegration(q1PcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());

            final String q2Sparql =
                    "SELECT ?x ?y " +
                            "WHERE { " +
                            "?x <http://talksTo> ?y. " +
                            "?y <http://worksAt> <http://Chipotle>." +
                            "}";
            final String q2PcjId = pcjStorage.createPcj(q2Sparql);
            createPcj.withRyaIntegration(q2PcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());

            // Ensure the command returns the correct metadata.
            final Set<PcjMetadata> expected = new HashSet<>();
            final Set<VariableOrder> q1VarOrders = new ShiftVarOrderFactory().makeVarOrders(q1Sparql);
            final Set<VariableOrder> q2VarOrders = new ShiftVarOrderFactory().makeVarOrders(q2Sparql);
            expected.add(new PcjMetadata(q1Sparql, 0L, q1VarOrders));
            expected.add(new PcjMetadata(q2Sparql, 0L, q2VarOrders));

            final Map<String, PcjMetadata> metadata = new GetPcjMetadata().getMetadata(pcjStorage, fluoClient);
            assertEquals(expected, Sets.newHashSet( metadata.values() ));
        }
    }
}