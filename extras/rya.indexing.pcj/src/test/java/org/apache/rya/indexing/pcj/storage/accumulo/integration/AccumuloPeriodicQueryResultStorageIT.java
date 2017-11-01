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
package org.apache.rya.indexing.pcj.storage.accumulo.integration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageException;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryStorageMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PeriodicQueryTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccumuloPeriodicQueryResultStorageIT extends AccumuloITBase {

    private PeriodicQueryResultStorage periodicStorage;
    private static final String RYA = "rya_";
    private static final PeriodicQueryTableNameFactory nameFactory = new PeriodicQueryTableNameFactory();
    private static final ValueFactory vf = SimpleValueFactory.getInstance();
    
    @Before
    public void init() throws AccumuloException, AccumuloSecurityException {
        super.getConnector().securityOperations().changeUserAuthorizations("root", new Authorizations("U"));
        periodicStorage = new AccumuloPeriodicQueryResultStorage(super.getConnector(), RYA);
    }
    
    
    @Test
    public void testCreateAndMeta() throws PeriodicQueryStorageException {
        
        String sparql = "select ?x where { ?x <urn:pred> ?y.}";
        VariableOrder varOrder = new VariableOrder("periodicBinId", "x");
        PeriodicQueryStorageMetadata expectedMeta = new PeriodicQueryStorageMetadata(sparql, varOrder);
        
        String id = periodicStorage.createPeriodicQuery(sparql);
        Assert.assertEquals(expectedMeta, periodicStorage.getPeriodicQueryMetadata(id));
        Assert.assertEquals(Arrays.asList(nameFactory.makeTableName(RYA, id)), periodicStorage.listPeriodicTables());
        periodicStorage.deletePeriodicQuery(id);
    }
    
    
    @Test
    public void testAddListDelete() throws Exception {
        
        String sparql = "select ?x where { ?x <urn:pred> ?y.}";
        String id = periodicStorage.createPeriodicQuery(sparql);
        
        Set<BindingSet> expected = new HashSet<>();
        Set<VisibilityBindingSet> storageSet = new HashSet<>();
        
        //add result matching user's visibility
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("periodicBinId", vf.createLiteral(1L));
        bs.addBinding("x",vf.createIRI("uri:uri123"));
        expected.add(bs);
        storageSet.add(new VisibilityBindingSet(bs,"U"));
        
        //add result with different visibility that is not expected
        bs = new QueryBindingSet();
        bs.addBinding("periodicBinId", vf.createLiteral(1L));
        bs.addBinding("x",vf.createIRI("uri:uri456"));
        storageSet.add(new VisibilityBindingSet(bs,"V"));
        
        periodicStorage.addPeriodicQueryResults(id, storageSet);
        
        Set<BindingSet> actual = new HashSet<>();
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(id, Optional.of(1L))) {
            iter.forEachRemaining(x -> actual.add(x));
        }
        
        Assert.assertEquals(expected, actual);
        
        periodicStorage.deletePeriodicQueryResults(id, 1L);
        
        Set<BindingSet> actual2 = new HashSet<>();
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(id, Optional.of(1L))) {
            iter.forEachRemaining(x -> actual2.add(x));
        }
        
        Assert.assertEquals(new HashSet<>(), actual2);
        periodicStorage.deletePeriodicQuery(id);
        
    }
    
    @Test
    public void multiBinTest() throws Exception {
        
        String sparql = "prefix function: <http://org.apache.rya/function#> " //n
                + "prefix time: <http://www.w3.org/2006/time#> " //n
                + "select ?id (count(?obs) as ?total) where {" //n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " //n
                + "?obs <uri:hasTime> ?time. " //n
                + "?obs <uri:hasId> ?id } group by ?id"; //n
        
        
        final ValueFactory vf = SimpleValueFactory.getInstance();
        long currentTime = System.currentTimeMillis();
        String queryId = UUID.randomUUID().toString().replace("-", "");
        
        // Create the expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected1 = new HashSet<>();
        final Set<BindingSet> expected2 = new HashSet<>();
        final Set<BindingSet> expected3 = new HashSet<>();
        final Set<BindingSet> expected4 = new HashSet<>();
        final Set<VisibilityBindingSet> storageResults = new HashSet<>();

        long period = 1800000;
        long binId = (currentTime/period)*period;
        
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expected1.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expected1.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expected1.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_4", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId));
        expected1.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expected2.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("2", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expected2.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_3", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + period));
        expected2.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 2*period));
        expected3.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_2", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 2*period));
        expected3.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        bs = new MapBindingSet();
        bs.addBinding("total", vf.createLiteral("1", XMLSchema.INTEGER));
        bs.addBinding("id", vf.createLiteral("id_1", XMLSchema.STRING));
        bs.addBinding("periodicBinId", vf.createLiteral(binId + 3*period));
        expected4.add(bs);
        storageResults.add(new VisibilityBindingSet(bs));
        
        
        String id = periodicStorage.createPeriodicQuery(queryId, sparql);
        periodicStorage.addPeriodicQueryResults(queryId, storageResults);
        
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(queryId, Optional.of(binId))) {
            Set<BindingSet> actual1 = new HashSet<>();
            while(iter.hasNext()) {
                actual1.add(iter.next());
            }
            Assert.assertEquals(expected1, actual1);
        }
        
        periodicStorage.deletePeriodicQueryResults(queryId, binId);
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(queryId, Optional.of(binId))) {
            Set<BindingSet> actual1 = new HashSet<>();
            while(iter.hasNext()) {
                actual1.add(iter.next());
            }
            Assert.assertEquals(Collections.emptySet(), actual1);
        }
        
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(queryId, Optional.of(binId + period))) {
            Set<BindingSet> actual2 = new HashSet<>();
            while(iter.hasNext()) {
                actual2.add(iter.next());
            }
            Assert.assertEquals(expected2, actual2);
        }
        
        periodicStorage.deletePeriodicQueryResults(queryId, binId + period);
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(queryId, Optional.of(binId + period))) {
            Set<BindingSet> actual2 = new HashSet<>();
            while(iter.hasNext()) {
                actual2.add(iter.next());
            }
            Assert.assertEquals(Collections.emptySet(), actual2);
        }
        
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(queryId, Optional.of(binId + 2*period))) {
            Set<BindingSet> actual3 = new HashSet<>();
            while(iter.hasNext()) {
                actual3.add(iter.next());
            }
            Assert.assertEquals(expected3, actual3);
        }
        
        try(CloseableIterator<BindingSet> iter = periodicStorage.listResults(queryId, Optional.of(binId + 3*period))) {
            Set<BindingSet> actual4 = new HashSet<>();
            while(iter.hasNext()) {
                actual4.add(iter.next());
            }
            Assert.assertEquals(expected4, actual4);
        }
        periodicStorage.deletePeriodicQuery(id);
    }
}
