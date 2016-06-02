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
package org.apache.rya.indexing.pcj.fluo.app.query;

import static org.junit.Assert.assertEquals;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.fluo.app.query.SparqlFluoQueryBuilder.NodeIds;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import io.fluo.api.client.Snapshot;
import io.fluo.api.client.Transaction;

/**
 * Integration tests the methods of {@link FluoQueryMetadataDAO}.
 */
public class FluoQueryMetadataDAOIT extends ITBase {

    @Test
    public void statementPatternMetadataTest() {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final StatementPatternMetadata.Builder builder = StatementPatternMetadata.builder("nodeId");
        builder.setVarOrder(new VariableOrder("a;b;c"));
        builder.setStatementPattern("statementPattern");
        builder.setParentNodeId("parentNodeId");
        final StatementPatternMetadata originalMetadata = builder.build();

        // Write it to the Fluo table.
        try(Transaction tx = fluoClient.newTransaction()) {
            dao.write(tx, originalMetadata);
            tx.commit();
        }

        // Read it from the Fluo table.
        StatementPatternMetadata storedMetadata = null;
        try(Snapshot sx = fluoClient.newSnapshot()) {
            storedMetadata = dao.readStatementPatternMetadata(sx, "nodeId");
        }

        // Ensure the deserialized object is the same as the serialized one.
        assertEquals(originalMetadata, storedMetadata);
    }

    @Test
    public void filterMetadataTest() {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final FilterMetadata.Builder builder = FilterMetadata.builder("nodeId");
        builder.setVarOrder(new VariableOrder("e;f"));
        builder.setParentNodeId("parentNodeId");
        builder.setChildNodeId("childNodeId");
        builder.setOriginalSparql("originalSparql");
        builder.setFilterIndexWithinSparql(2);
        final FilterMetadata originalMetadata = builder.build();

        // Write it to the Fluo table.
        try(Transaction tx = fluoClient.newTransaction()) {
            dao.write(tx, originalMetadata);
            tx.commit();
        }

        // Read it from the Fluo table.
        FilterMetadata storedMetadata = null;
        try(Snapshot sx = fluoClient.newSnapshot()) {
            storedMetadata = dao.readFilterMetadata(sx, "nodeId");
        }

        // Ensure the deserialized object is the same as the serialized one.
        assertEquals(originalMetadata, storedMetadata);
    }

    @Test
    public void joinMetadataTest() {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final JoinMetadata.Builder builder = JoinMetadata.builder("nodeId");
        builder.setVariableOrder(new VariableOrder("g;y;s"));
        builder.setJoinType(JoinType.NATURAL_JOIN);
        builder.setParentNodeId("parentNodeId");
        builder.setLeftChildNodeId("leftChildNodeId");
        builder.setRightChildNodeId("rightChildNodeId");
        final JoinMetadata originalMetadata = builder.build();

        // Write it to the Fluo table.
        try(Transaction tx = fluoClient.newTransaction()) {
            dao.write(tx, originalMetadata);
            tx.commit();
        }

        // Read it from the Fluo table.
        JoinMetadata storedMetadata = null;
        try(Snapshot sx = fluoClient.newSnapshot()) {
            storedMetadata = dao.readJoinMetadata(sx, "nodeId");
        }

        // Ensure the deserialized object is the same as the serialized one.
        assertEquals(originalMetadata, storedMetadata);
    }

    @Test
    public void queryMetadataTest() {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final QueryMetadata.Builder builder = QueryMetadata.builder("nodeId");
        builder.setVariableOrder(new VariableOrder("y;s;d"));
        builder.setSparql("sparql string");
        builder.setChildNodeId("childNodeId");
        final QueryMetadata originalMetadata = builder.build();

        // Write it to the Fluo table.
        try(Transaction tx = fluoClient.newTransaction()) {
            dao.write(tx, originalMetadata);
            tx.commit();
        }

        // Read it from the Fluo table.
        QueryMetadata storedMetdata = null;
        try(Snapshot sx = fluoClient.newSnapshot()) {
            storedMetdata = dao.readQueryMetadata(sx, "nodeId");
        }

        // Ensure the deserialized object is the same as the serialized one.
        assertEquals(originalMetadata, storedMetdata);
    }

    @Test
    public void fluoQueryTest() throws MalformedQueryException {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "FILTER(?customer = <http://Alice>) " +
                  "FILTER(?city = <http://London>) " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        final ParsedQuery query = new SPARQLParser().parseQuery(sparql, null);
        final FluoQuery originalQuery = new SparqlFluoQueryBuilder().make(query, new NodeIds());

        // Write it to the Fluo table.
        try(Transaction tx = fluoClient.newTransaction()) {
            dao.write(tx, originalQuery);
            tx.commit();
        }

        // Read it from the Fluo table.
        FluoQuery storedQuery = null;
        try(Snapshot sx = fluoClient.newSnapshot()) {
            storedQuery = dao.readFluoQuery(sx, originalQuery.getQueryMetadata().getNodeId());
        }

        // Ensure the deserialized object is the same as the serialized one.
        assertEquals(originalQuery, storedQuery);
    }
}
