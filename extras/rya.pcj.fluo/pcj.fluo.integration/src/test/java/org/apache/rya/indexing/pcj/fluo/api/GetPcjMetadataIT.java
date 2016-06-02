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

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.GetPcjMetadata.NotInAccumuloException;
import org.apache.rya.indexing.pcj.fluo.api.GetPcjMetadata.NotInFluoException;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.sail.SailException;

import com.google.common.collect.Sets;

/**
 * Integration tests the methods of {@link GetPcjMetadata}.
 */
public class GetPcjMetadataIT extends ITBase {

    @Test
    public void getMetadataByQueryId() throws AccumuloException, AccumuloSecurityException, TableExistsException, PcjException, NotInFluoException, NotInAccumuloException, MalformedQueryException, SailException, QueryEvaluationException {
        final String sparql =
                "SELECT ?x " +
                  "WHERE { " +
                  "?x <http://talksTo> <http://Eve>. " +
                  "?x <http://worksAt> <http://Chipotle>." +
                "}";
        final Set<VariableOrder> varOrders = Sets.<VariableOrder>newHashSet( new VariableOrder("x") );

        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, varOrders, sparql);

        // Ensure the command returns the correct metadata.
        final PcjMetadata expected = new PcjMetadata(sparql, 0L, varOrders);

        final String queryId = new ListQueryIds().listQueryIds(fluoClient).get(0);
        final PcjMetadata metadata = new GetPcjMetadata().getMetadata(accumuloConn, fluoClient, queryId);

        assertEquals(expected, metadata);
    }

    @Test
    public void getAllMetadata() throws MalformedQueryException, SailException, QueryEvaluationException, PcjException, NotInFluoException, NotInAccumuloException {
        final CreatePcj createPcj = new CreatePcj();

        // Add a couple of queries to Accumulo.
        final String q1Sparql =
                "SELECT ?x " +
                  "WHERE { " +
                  "?x <http://talksTo> <http://Eve>. " +
                  "?x <http://worksAt> <http://Chipotle>." +
                "}";
        final Set<VariableOrder> q1VarOrders = Sets.<VariableOrder>newHashSet( new VariableOrder("x") );
        createPcj.withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, q1VarOrders, q1Sparql);

        final String q2Sparql =
                "SELECT ?x ?y " +
                  "WHERE { " +
                  "?x <http://talksTo> ?y. " +
                  "?y <http://worksAt> <http://Chipotle>." +
                "}";
        final Set<VariableOrder> q2VarOrders = Sets.<VariableOrder>newHashSet( new VariableOrder("x", "y") );
        createPcj.withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, q2VarOrders, q2Sparql);

        // Ensure the command returns the correct metadata.
        final Set<PcjMetadata> expected = new HashSet<>();
        expected.add(new PcjMetadata(q1Sparql, 0L, q1VarOrders));
        expected.add(new PcjMetadata(q2Sparql, 0L, q2VarOrders));

        final Map<String, PcjMetadata> metadata = new GetPcjMetadata().getMetadata(accumuloConn, fluoClient);
        assertEquals(expected, Sets.newHashSet( metadata.values() ));
    }
}