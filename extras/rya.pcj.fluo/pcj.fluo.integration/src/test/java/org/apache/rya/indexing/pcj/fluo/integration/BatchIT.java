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
package org.apache.rya.indexing.pcj.fluo.integration;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.Side;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformationDAO;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.fluo.app.util.BindingHashShardingFunction;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class BatchIT extends RyaExportITBase {

    private static final Logger log = Logger.getLogger(BatchIT.class);
    private static final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();
    private static final ValueFactory vf = new ValueFactoryImpl();

    @Test
    public void simpleScanDelete() throws Exception {

        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 } ";
        try (FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration())) {

            RyaURI subj = new RyaURI("urn:subject_1");
            RyaStatement statement1 = new RyaStatement(subj, new RyaURI("urn:predicate_1"), null);
            RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);
            Set<RyaStatement> statements1 = getRyaStatements(statement1, 10);
            Set<RyaStatement> statements2 = getRyaStatements(statement2, 10);

            // Create the PCJ table.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), getRyaInstanceName());
            final String pcjId = pcjStorage.createPcj(sparql);

            // Tell the Fluo app to maintain the PCJ.
            String queryId = new CreateFluoPcj()
                    .withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), getRyaInstanceName()).getQueryId();

            List<String> ids = getNodeIdStrings(fluoClient, queryId);
            List<String> prefixes = Arrays.asList("urn:subject_1", "urn:subject_1", "urn:object", "urn:subject_1", "urn:subject_1");

            // Stream the data into Fluo.
            InsertTriples inserter = new InsertTriples();
            inserter.insert(fluoClient, statements1, Optional.absent());
            inserter.insert(fluoClient, statements2, Optional.absent());

            // Verify the end results of the query match the expected results.
            getMiniFluo().waitForObservers();

            verifyCounts(fluoClient, ids, Arrays.asList(100, 100, 100, 10, 10));

            createSpanBatches(fluoClient, ids, prefixes, 10);
            getMiniFluo().waitForObservers();

            verifyCounts(fluoClient, ids, Arrays.asList(0, 0, 0, 0, 0));
        }
    }

    @Test
    public void simpleJoinDelete() throws Exception {
        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 } ";
        try (FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration())) {

            RyaURI subj = new RyaURI("urn:subject_1");
            RyaStatement statement1 = new RyaStatement(subj, new RyaURI("urn:predicate_1"), null);
            RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);
            Set<RyaStatement> statements1 = getRyaStatements(statement1, 5);
            Set<RyaStatement> statements2 = getRyaStatements(statement2, 5);

            // Create the PCJ table.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), getRyaInstanceName());
            final String pcjId = pcjStorage.createPcj(sparql);

            // Tell the Fluo app to maintain the PCJ.
            String queryId = new CreateFluoPcj()
                    .withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), getRyaInstanceName()).getQueryId();

            List<String> ids = getNodeIdStrings(fluoClient, queryId);
            String joinId = ids.get(2);
            String rightSp = ids.get(4);
            QueryBindingSet bs = new QueryBindingSet();
            bs.addBinding("subject", vf.createURI("urn:subject_1"));
            bs.addBinding("object1", vf.createURI("urn:object_0"));
            VisibilityBindingSet vBs = new VisibilityBindingSet(bs);

            //create sharded span for deletion
            URI uri = vf.createURI("urn:subject_1");
            Bytes prefixBytes = BindingHashShardingFunction.getShardedScanPrefix(rightSp, uri);
            Span span = Span.prefix(prefixBytes);

            // Stream the data into Fluo.
            InsertTriples inserter = new InsertTriples();
            inserter.insert(fluoClient, statements1, Optional.absent());
            inserter.insert(fluoClient, statements2, Optional.absent());

            getMiniFluo().waitForObservers();
            verifyCounts(fluoClient, ids, Arrays.asList(25, 25, 25, 5, 5));

            JoinBatchInformation batch = JoinBatchInformation.builder().setBatchSize(1)
                    .setColumn(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET).setSpan(span).setTask(Task.Delete)
                    .setJoinType(JoinType.NATURAL_JOIN).setSide(Side.LEFT).setBs(vBs).build();
            // Verify the end results of the query match the expected results.
            createSpanBatch(fluoClient, joinId, batch);

            getMiniFluo().waitForObservers();
            verifyCounts(fluoClient, ids, Arrays.asList(25, 25, 20, 5, 5));
        }
    }

    @Test
    public void simpleJoinAdd() throws Exception {
        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 } ";
        try (FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration())) {

            RyaURI subj = new RyaURI("urn:subject_1");
            RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);
            Set<RyaStatement> statements2 = getRyaStatements(statement2, 5);

            // Create the PCJ table.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), getRyaInstanceName());
            final String pcjId = pcjStorage.createPcj(sparql);

            // Tell the Fluo app to maintain the PCJ.
            String queryId = new CreateFluoPcj()
                    .withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), getRyaInstanceName()).getQueryId();

            List<String> ids = getNodeIdStrings(fluoClient, queryId);
            String joinId = ids.get(2);
            String rightSp = ids.get(4);
            QueryBindingSet bs = new QueryBindingSet();
            bs.addBinding("subject", vf.createURI("urn:subject_1"));
            bs.addBinding("object1", vf.createURI("urn:object_0"));
            VisibilityBindingSet vBs = new VisibilityBindingSet(bs);

            URI uri = vf.createURI("urn:subject_1");
            Bytes prefixBytes = BindingHashShardingFunction.getShardedScanPrefix(rightSp, uri);
            Span span = Span.prefix(prefixBytes);

            // Stream the data into Fluo.
            InsertTriples inserter = new InsertTriples();
            inserter.insert(fluoClient, statements2, Optional.absent());

            getMiniFluo().waitForObservers();
            verifyCounts(fluoClient, ids, Arrays.asList(0, 0, 0, 0, 5));

            JoinBatchInformation batch = JoinBatchInformation.builder().setBatchSize(1)
                    .setColumn(FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET).setSpan(span).setTask(Task.Add)
                    .setJoinType(JoinType.NATURAL_JOIN).setSide(Side.LEFT).setBs(vBs).build();
            // Verify the end results of the query match the expected results.
            createSpanBatch(fluoClient, joinId, batch);

            getMiniFluo().waitForObservers();
            verifyCounts(fluoClient, ids, Arrays.asList(5, 5, 5, 0, 5));
        }
    }

    @Test
    public void joinBatchIntegrationTest() throws Exception {
        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 } ";
        try (FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration())) {

            RyaURI subj = new RyaURI("urn:subject_1");
            RyaStatement statement1 = new RyaStatement(subj, new RyaURI("urn:predicate_1"), null);
            RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);

            Set<RyaStatement> statements1 = getRyaStatements(statement1, 15);
            Set<RyaStatement> statements2 = getRyaStatements(statement2, 15);

            // Create the PCJ table.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), getRyaInstanceName());
            final String pcjId = pcjStorage.createPcj(sparql);

            // Tell the Fluo app to maintain the PCJ and sets batch scan size for StatementPatterns to 5 and
            // batch size of joins to 5.
            String queryId = new CreateFluoPcj(5, 5)
                    .withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), getRyaInstanceName()).getQueryId();

            List<String> ids = getNodeIdStrings(fluoClient, queryId);

            // Stream the data into Fluo.
            InsertTriples inserter = new InsertTriples();
            inserter.insert(fluoClient, statements1, Optional.absent());
            inserter.insert(fluoClient, statements2, Optional.absent());

            getMiniFluo().waitForObservers();
            verifyCounts(fluoClient, ids, Arrays.asList(225, 225, 225, 15, 15));
        }
    }

    @Test
    public void leftJoinBatchIntegrationTest() throws Exception {
        final String sparql = "SELECT ?subject ?object1 ?object2 WHERE { ?subject <urn:predicate_1> ?object1; "
                + "OPTIONAL{ ?subject <urn:predicate_2> ?object2} } ";
        try (FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration())) {

            RyaURI subj = new RyaURI("urn:subject_1");
            RyaStatement statement1 = new RyaStatement(subj, new RyaURI("urn:predicate_1"), null);
            RyaStatement statement2 = new RyaStatement(subj, new RyaURI("urn:predicate_2"), null);

            subj = new RyaURI("urn:subject_2");
            RyaStatement statement3 = new RyaStatement(subj, new RyaURI("urn:predicate_1"), null);

            Set<RyaStatement> statements1 = getRyaStatements(statement1, 10);
            Set<RyaStatement> statements2 = getRyaStatements(statement2, 10);
            Set<RyaStatement> statements3 = getRyaStatements(statement3, 10);

            // Create the PCJ table.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), getRyaInstanceName());
            final String pcjId = pcjStorage.createPcj(sparql);

            // Tell the Fluo app to maintain the PCJ and sets batch scan size for StatementPatterns to 5 and
            // batch size of joins to 5.
            String queryId = new CreateFluoPcj(5, 5)
                    .withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), getRyaInstanceName()).getQueryId();

            List<String> ids = getNodeIdStrings(fluoClient, queryId);

            // Stream the data into Fluo.
            InsertTriples inserter = new InsertTriples();
            inserter.insert(fluoClient, statements1, Optional.absent());
            inserter.insert(fluoClient, statements2, Optional.absent());
            inserter.insert(fluoClient, statements3, Optional.absent());

            getMiniFluo().waitForObservers();
            verifyCounts(fluoClient, ids, Arrays.asList(110, 110, 110, 20, 10));
        }
    }

    @Test
    public void multiJoinBatchIntegrationTest() throws Exception {
        final String sparql = "SELECT ?subject1 ?subject2 ?object1 ?object2 WHERE { ?subject1 <urn:predicate_1> ?object1; "
                + " <urn:predicate_2> ?object2 ." + " ?subject2 <urn:predicate_3> ?object2 } ";
        try (FluoClient fluoClient = new FluoClientImpl(getFluoConfiguration())) {

            RyaURI subj1 = new RyaURI("urn:subject_1");
            RyaStatement statement1 = new RyaStatement(subj1, new RyaURI("urn:predicate_1"), null);
            RyaStatement statement2 = new RyaStatement(subj1, new RyaURI("urn:predicate_2"), null);

            Set<RyaStatement> statements1 = getRyaStatements(statement1, 10);
            Set<RyaStatement> statements2 = getRyaStatements(statement2, 10);

            RyaURI subj2 = new RyaURI("urn:subject_2");
            RyaStatement statement3 = new RyaStatement(subj2, new RyaURI("urn:predicate_3"), null);
            Set<RyaStatement> statements3 = getRyaStatements(statement3, 10);

            // Create the PCJ table.
            final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(getAccumuloConnector(), getRyaInstanceName());
            final String pcjId = pcjStorage.createPcj(sparql);

            // Tell the Fluo app to maintain the PCJ and sets batch scan size for StatementPatterns to 5 and
            // batch size of joins to 5.
            String queryId = new CreateFluoPcj(5, 5)
                    .withRyaIntegration(pcjId, pcjStorage, fluoClient, getAccumuloConnector(), getRyaInstanceName()).getQueryId();

            List<String> ids = getNodeIdStrings(fluoClient, queryId);

            // Stream the data into Fluo.
            InsertTriples inserter = new InsertTriples();
            inserter.insert(fluoClient, statements1, Optional.absent());
            inserter.insert(fluoClient, statements2, Optional.absent());
            inserter.insert(fluoClient, statements3, Optional.absent());

            getMiniFluo().waitForObservers();
            verifyCounts(fluoClient, ids, Arrays.asList(100, 100, 100, 100, 10, 10, 10));
        }
    }

    private Set<RyaStatement> getRyaStatements(RyaStatement statement, int numTriples) {

        Set<RyaStatement> statements = new HashSet<>();
        final String subject = "urn:subject_";
        final String predicate = "urn:predicate_";
        final String object = "urn:object_";

        for (int i = 0; i < numTriples; i++) {
            RyaStatement stmnt = new RyaStatement(statement.getSubject(), statement.getPredicate(), statement.getObject());
            if (stmnt.getSubject() == null) {
                stmnt.setSubject(new RyaURI(subject + i));
            }
            if (stmnt.getPredicate() == null) {
                stmnt.setPredicate(new RyaURI(predicate + i));
            }
            if (stmnt.getObject() == null) {
                stmnt.setObject(new RyaURI(object + i));
            }
            statements.add(stmnt);
        }
        return statements;
    }

    private List<String> getNodeIdStrings(FluoClient fluoClient, String queryId) throws UnsupportedQueryException {
        List<String> nodeStrings;
        try (Snapshot sx = fluoClient.newSnapshot()) {
            FluoQuery query = dao.readFluoQuery(sx, queryId);
            nodeStrings = FluoQueryUtils.collectNodeIds(query);
        }
        return nodeStrings;
    }

    private void createSpanBatches(FluoClient fluoClient, List<String> ids, List<String> prefixes, int batchSize) {

        Preconditions.checkArgument(ids.size() == prefixes.size());

        try (Transaction tx = fluoClient.newTransaction()) {
            for (int i = 0; i < ids.size(); i++) {
                String id = ids.get(i);
                String bsPrefix = prefixes.get(i);
                URI uri = vf.createURI(bsPrefix);
                Bytes prefixBytes = BindingHashShardingFunction.getShardedScanPrefix(id, uri);
                NodeType type = NodeType.fromNodeId(id).get();
                Column bsCol = type.getResultColumn();
                SpanBatchDeleteInformation.Builder builder = SpanBatchDeleteInformation.builder().setBatchSize(batchSize)
                        .setColumn(bsCol);
                if (type == NodeType.JOIN) {
                    builder.setSpan(Span.prefix(type.getNodeTypePrefix()));
                    builder.setNodeId(java.util.Optional.of(id));
                } else {
                    builder.setSpan(Span.prefix(prefixBytes));
                }
                BatchInformationDAO.addBatch(tx, id, builder.build());
            }
            tx.commit();
        }
    }

    private void createSpanBatch(FluoClient fluoClient, String nodeId, BatchInformation batch) {
        try (Transaction tx = fluoClient.newTransaction()) {
            BatchInformationDAO.addBatch(tx, nodeId, batch);
            tx.commit();
        }
    }

    private int countResults(FluoClient fluoClient, String nodeId, Column bsColumn) {
        try (Transaction tx = fluoClient.newTransaction()) {
            int count = 0;
            Optional<NodeType> type = NodeType.fromNodeId(nodeId);
            Bytes prefixBytes = Bytes.of(type.get().getNodeTypePrefix());
            RowScanner scanner = tx.scanner().over(Span.prefix(prefixBytes)).fetch(bsColumn).byRow().build();
            Iterator<ColumnScanner> colScanners = scanner.iterator();
            while (colScanners.hasNext()) {
                ColumnScanner colScanner = colScanners.next();
                BindingSetRow bsRow = BindingSetRow.makeFromShardedRow(prefixBytes, colScanner.getRow());
                if (bsRow.getNodeId().equals(nodeId)) {
                    Iterator<ColumnValue> vals = colScanner.iterator();
                    while (vals.hasNext()) {
                        vals.next();
                        count++;
                    }
                }
            }
            tx.commit();
            return count;
        }
    }

    private void verifyCounts(FluoClient fluoClient, List<String> ids, List<Integer> expectedCounts) {
        Preconditions.checkArgument(ids.size() == expectedCounts.size());
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            int expected = expectedCounts.get(i);
            NodeType type = NodeType.fromNodeId(id).get();
            int count = countResults(fluoClient, id, type.getResultColumn());
            switch (type) {
            case STATEMENT_PATTERN:
                assertEquals(expected, count);
                break;
            case JOIN:
                assertEquals(expected, count);
                break;
            case QUERY:
                assertEquals(expected, count);
                break;
            default:
                break;
            }
        }
    }

}
