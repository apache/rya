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

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.xml.datatype.DatatypeFactory;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.pcj.fluo.api.CreatePeriodicQuery;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.Sets;

public class PeriodicNotificationBinPrunerIT extends RyaExportITBase {

    
    @Test
    public void periodicPrunerTest() throws Exception {

        String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } group by ?id"; // n

        FluoClient fluo = new FluoClientImpl(super.getFluoConfiguration());

        // initialize resources and create pcj
        PeriodicQueryResultStorage periodicStorage = new AccumuloPeriodicQueryResultStorage(super.getAccumuloConnector(),
                getRyaInstanceName());
        CreatePeriodicQuery createPeriodicQuery = new CreatePeriodicQuery(fluo, periodicStorage);
        String queryId = FluoQueryUtils.convertFluoQueryIdToPcjId(createPeriodicQuery.createPeriodicQuery(sparql).getQueryId());

        // create statements to ingest into Fluo
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();
        long currentTime = time.toInstant().toEpochMilli();

        ZonedDateTime zTime1 = time.minusMinutes(30);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasId"), vf.createLiteral("id_4")),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")));

        // add statements to Fluo
        InsertTriples inserter = new InsertTriples();
        statements.forEach(x -> inserter.insert(fluo, RdfToRyaConversions.convertStatement(x)));

        super.getMiniFluo().waitForObservers();

        // FluoITHelper.printFluoTable(fluo);

        // Create the expected results of the SPARQL query once the PCJ has been
        // computed.
        final Set<BindingSet> expected1 = new HashSet<>();
        final Set<BindingSet> expected2 = new HashSet<>();
        final Set<BindingSet> expected3 = new HashSet<>();
        final Set<BindingSet> expected4 = new HashSet<>();

        long period = 1800000;
        long binId = (currentTime / period) * period;

        long bin1 = binId;
        long bin2 = binId + period;
        long bin3 = binId + 2 * period;
        long bin4 = binId + 3 * period;

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin1));
        expected1.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin1));
        expected1.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin1));
        expected1.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_4", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin1));
        expected1.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin2));
        expected2.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin2));
        expected2.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin2));
        expected2.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin3));
        expected3.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin3));
        expected3.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(bin4));
        expected4.add(bs);

        // make sure that expected and actual results align after ingest
        compareResults(periodicStorage, queryId, bin1, expected1);
        compareResults(periodicStorage, queryId, bin2, expected2);
        compareResults(periodicStorage, queryId, bin3, expected3);
        compareResults(periodicStorage, queryId, bin4, expected4);

        BlockingQueue<NodeBin> bins = new LinkedBlockingQueue<>();
        PeriodicQueryPrunerExecutor pruner = new PeriodicQueryPrunerExecutor(periodicStorage, fluo, 1, bins);
        pruner.start();

        bins.add(new NodeBin(queryId, bin1));
        bins.add(new NodeBin(queryId, bin2));
        bins.add(new NodeBin(queryId, bin3));
        bins.add(new NodeBin(queryId, bin4));

        Thread.sleep(10000);

        compareResults(periodicStorage, queryId, bin1, new HashSet<>());
        compareResults(periodicStorage, queryId, bin2, new HashSet<>());
        compareResults(periodicStorage, queryId, bin3, new HashSet<>());
        compareResults(periodicStorage, queryId, bin4, new HashSet<>());

        compareFluoCounts(fluo, queryId, bin1);
        compareFluoCounts(fluo, queryId, bin2);
        compareFluoCounts(fluo, queryId, bin3);
        compareFluoCounts(fluo, queryId, bin4);

        pruner.stop();

    }
    
    private void compareResults(PeriodicQueryResultStorage periodicStorage, String queryId, long bin, Set<BindingSet> expected) throws PeriodicQueryStorageException, Exception {
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(queryId, Optional.of(bin))) {
            Set<BindingSet> actual = new HashSet<>();
            while(iter.hasNext()) {
                actual.add(iter.next());
            }
            Assert.assertEquals(expected, actual);
        }
    }
    
    private void compareFluoCounts(FluoClient client, String pcjId, long bin) {
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, new LiteralImpl(Long.toString(bin), XMLSchema.LONG));
        
        VariableOrder varOrder = new VariableOrder(IncrementalUpdateConstants.PERIODIC_BIN_ID);
        
        try(Snapshot sx = client.newSnapshot()) {
            String fluoQueryId = NodeType.generateNewIdForType(NodeType.QUERY, pcjId);
            Set<String> ids = new HashSet<>();
            PeriodicQueryUtil.getPeriodicQueryNodeAncestorIds(sx, fluoQueryId, ids);
            for(String id: ids) {
                NodeType optNode = NodeType.fromNodeId(id).orNull();
                if(optNode == null) throw new RuntimeException("Invalid NodeType.");
                Bytes prefix = RowKeyUtil.makeRowKey(id,varOrder, bs);
                RowScanner scanner = sx.scanner().fetch(optNode.getResultColumn()).over(Span.prefix(prefix)).byRow().build();
                int count = 0;
                Iterator<ColumnScanner> colScannerIter = scanner.iterator();
                while(colScannerIter.hasNext()) {
                    ColumnScanner colScanner = colScannerIter.next();
                    String row = colScanner.getRow().toString();
                    Iterator<ColumnValue> values = colScanner.iterator();
                    while(values.hasNext()) {
                        values.next();
                        count++;
                    }
                }
                Assert.assertEquals(0, count);
            }
        }
    }
    
}
