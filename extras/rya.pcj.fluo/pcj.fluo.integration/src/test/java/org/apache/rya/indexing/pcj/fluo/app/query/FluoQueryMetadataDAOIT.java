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

import java.util.List;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.rya.indexing.pcj.fluo.RyaExportITBase;
import org.apache.rya.indexing.pcj.fluo.app.ConstructGraph;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata.AggregationElement;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata.AggregationType;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery.QueryType;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.fluo.app.query.SparqlFluoQueryBuilder.NodeIds;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;

import com.google.common.base.Optional;

/**
 * Integration tests the methods of {@link FluoQueryMetadataDAO}.
 */
public class FluoQueryMetadataDAOIT extends RyaExportITBase {

    @Test
    public void statementPatternMetadataTest() throws RepositoryException {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final StatementPatternMetadata.Builder builder = StatementPatternMetadata.builder("nodeId");
        builder.setVarOrder(new VariableOrder("a;b;c"));
        builder.setStatementPattern("statementPattern");
        builder.setParentNodeId("parentNodeId");
        final StatementPatternMetadata originalMetadata = builder.build();

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
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

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
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

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
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

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
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
    }
    
    @Test
    public void constructQueryMetadataTest() throws MalformedQueryException {
        
        String query = "select ?x ?y where {?x <uri:p1> ?y. ?y <uri:p2> <uri:o1> }";
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq = parser.parseQuery(query, null);
        List<StatementPattern> patterns = StatementPatternCollector.process(pq.getTupleExpr());
        
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final ConstructQueryMetadata.Builder builder = ConstructQueryMetadata.builder();
        builder.setNodeId("nodeId");
        builder.setSparql(query);
        builder.setChildNodeId("childNodeId");
        builder.setConstructGraph(new ConstructGraph(patterns));
        final ConstructQueryMetadata originalMetadata = builder.build();

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Write it to the Fluo table.
            try(Transaction tx = fluoClient.newTransaction()) {
                dao.write(tx, originalMetadata);
                tx.commit();
            }

            // Read it from the Fluo table.
            ConstructQueryMetadata storedMetdata = null;
            try(Snapshot sx = fluoClient.newSnapshot()) {
                storedMetdata = dao.readConstructQueryMetadata(sx, "nodeId");
            }

            // Ensure the deserialized object is the same as the serialized one.
            assertEquals(originalMetadata, storedMetdata);
        }
    }

    @Test
    public void aggregationMetadataTest_withGroupByVarOrders() {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final AggregationMetadata originalMetadata = AggregationMetadata.builder("nodeId")
                .setVariableOrder(new VariableOrder("totalCount"))
                .setParentNodeId("parentNodeId")
                .setChildNodeId("childNodeId")
                .setGroupByVariableOrder(new VariableOrder("a", "b", "c"))
                .addAggregation(new AggregationElement(AggregationType.COUNT, "count", "totalCount"))
                .addAggregation(new AggregationElement(AggregationType.AVERAGE, "privae", "avgPrice"))
                .build();

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Write it to the Fluo table.
            try(Transaction tx = fluoClient.newTransaction()) {
                dao.write(tx, originalMetadata);
                tx.commit();
            }

            // Read it from the Fluo table.
            AggregationMetadata storedMetadata = null;
            try(Snapshot sx = fluoClient.newSnapshot()) {
                storedMetadata = dao.readAggregationMetadata(sx, "nodeId");
            }

            // Ensure the deserialized object is the same as the serialized one.
            assertEquals(originalMetadata, storedMetadata);
        }
    }

    @Test
    public void aggregationMetadataTest_noGroupByVarOrders() {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final AggregationMetadata originalMetadata = AggregationMetadata.builder("nodeId")
                .setVariableOrder(new VariableOrder("totalCount"))
                .setParentNodeId("parentNodeId")
                .setChildNodeId("childNodeId")
                .addAggregation(new AggregationElement(AggregationType.COUNT, "count", "totalCount"))
                .addAggregation(new AggregationElement(AggregationType.AVERAGE, "privae", "avgPrice"))
                .build();

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Write it to the Fluo table.
            try(Transaction tx = fluoClient.newTransaction()) {
                dao.write(tx, originalMetadata);
                tx.commit();
            }

            // Read it from the Fluo table.
            AggregationMetadata storedMetadata = null;
            try(Snapshot sx = fluoClient.newSnapshot()) {
                storedMetadata = dao.readAggregationMetadata(sx, "nodeId");
            }

            // Ensure the deserialized object is the same as the serialized one.
            assertEquals(originalMetadata, storedMetadata);
        }
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

        assertEquals(QueryType.Projection, originalQuery.getQueryType());
        assertEquals(false, originalQuery.getConstructQueryMetadata().isPresent());
        
        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Write it to the Fluo table.
            try(Transaction tx = fluoClient.newTransaction()) {
                dao.write(tx, originalQuery);
                tx.commit();
            }

        // Read it from the Fluo table.
        FluoQuery storedQuery = null;
        try(Snapshot sx = fluoClient.newSnapshot()) {
            storedQuery = dao.readFluoQuery(sx, originalQuery.getQueryMetadata().get().getNodeId());
        }

            // Ensure the deserialized object is the same as the serialized one.
            assertEquals(originalQuery, storedQuery);
        }
    }
    
    @Test
    public void fluoConstructQueryTest() throws MalformedQueryException {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        // Create the object that will be serialized.
        final String sparql =
                "CONSTRUCT { ?customer <http://travelsTo> <http://England> .  ?customer <http://friendsWith> ?worker }" +
                "WHERE { " +
                  "FILTER(?customer = <http://Alice>) " +
                  "FILTER(?city = <http://London>) " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        final ParsedQuery query = new SPARQLParser().parseQuery(sparql, null);
        final FluoQuery originalQuery = new SparqlFluoQueryBuilder().make(query, new NodeIds());
        
        assertEquals(QueryType.Construct, originalQuery.getQueryType());
        assertEquals(false, originalQuery.getQueryMetadata().isPresent());

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Write it to the Fluo table.
            try(Transaction tx = fluoClient.newTransaction()) {
                dao.write(tx, originalQuery);
                tx.commit();
            }

        // Read it from the Fluo table.
        FluoQuery storedQuery = null;
        try(Snapshot sx = fluoClient.newSnapshot()) {
            storedQuery = dao.readFluoQuery(sx, originalQuery.getConstructQueryMetadata().get().getNodeId());
        }

            // Ensure the deserialized object is the same as the serialized one.
            assertEquals(originalQuery, storedQuery);
        }
    }
}