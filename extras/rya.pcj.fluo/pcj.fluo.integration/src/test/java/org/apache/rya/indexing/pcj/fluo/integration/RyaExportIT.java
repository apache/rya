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
import io.fluo.api.client.Snapshot;
import io.fluo.api.data.Bytes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mvm.rya.api.domain.RyaStatement;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards Rya PCJ exporting.
 * <p>
 * These tests are being ignore so that they will not run as unit tests while building the application.
 */
public class RyaExportIT extends ITBase {

	private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();

    /**
     * Configure the export observer to use the Mini Accumulo instance as the
     * export destination for new PCJ results.
     */
    @Override
    protected Map<String, String> makeExportParams() {
        final HashMap<String, String> params = new HashMap<>();

        final RyaExportParameters ryaParams = new RyaExportParameters(params);
        ryaParams.setExportToRya(true);
        ryaParams.setAccumuloInstanceName(instanceName);
        ryaParams.setZookeeperServers(zookeepers);
        ryaParams.setExporterUsername(ITBase.ACCUMULO_USER);
        ryaParams.setExporterPassword(ITBase.ACCUMULO_PASSWORD);

        return params;
    }

    @Test
    public void resultsExported() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "FILTER(?customer = <http://Alice>) " +
                  "FILTER(?city = <http://London>) " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"),
                makeRyaStatement("http://Bob", "http://livesIn", "http://London"),
                makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Charlie"),
                makeRyaStatement("http://Charlie", "http://livesIn", "http://London"),
                makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://David"),
                makeRyaStatement("http://David", "http://livesIn", "http://London"),
                makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Eve", "http://livesIn", "http://Leeds"),
                makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Frank", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://Frank", "http://livesIn", "http://London"),
                makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();
        expectedResults.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expectedResults.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Charlie")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expectedResults.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://David")),
                new BindingImpl("city", new URIImpl("http://London"))));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Fetch the exported results from Accumulo once the observers finish working.
        fluo.waitForObservers();

        // Fetch expcted results from the PCJ table that is in Accumulo.
        final String exportTableName;
        try(Snapshot snapshot = fluoClient.newSnapshot()) {
            final Bytes queryId = snapshot.get(Bytes.of(sparql), FluoQueryColumns.QUERY_ID);
            exportTableName = snapshot.get(queryId, FluoQueryColumns.QUERY_RYA_EXPORT_TABLE_NAME).toString();
        }

        final Multimap<String, BindingSet> results = loadPcjResults(accumuloConn, exportTableName);

        // Verify the end results of the query match the expected results.
        final Multimap<String, BindingSet> expected = HashMultimap.create();
        expected.putAll("customer;worker;city", expectedResults);
        expected.putAll("worker;city;customer", expectedResults);
        expected.putAll("city;customer;worker", expectedResults);

        assertEquals(expected,  results);
    }

    /**
     * Scan accumulo for the results that are stored in a PCJ table. The
     * multimap stores a set of deserialized binding sets that were in the PCJ
     * table for every variable order that is found in the PCJ metadata.
     */
    private static Multimap<String, BindingSet> loadPcjResults(final Connector accumuloConn, final String pcjTableName) throws PcjException, TableNotFoundException, BindingSetConversionException {
        final Multimap<String, BindingSet> fetchedResults = HashMultimap.create();

        // Get the variable orders the data was written to.
        final PcjTables pcjs = new PcjTables();
        final PcjMetadata pcjMetadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);

        // Scan Accumulo for the stored results.
        for(final VariableOrder varOrder : pcjMetadata.getVarOrders()) {
            final Scanner scanner = accumuloConn.createScanner(pcjTableName, new Authorizations());
            scanner.fetchColumnFamily( new Text(varOrder.toString()) );

            for(final Entry<Key, Value> entry : scanner) {
                final byte[] serializedResult = entry.getKey().getRow().getBytes();
                final BindingSet result = converter.convert(serializedResult, varOrder);
                fetchedResults.put(varOrder.toString(), result);
            }
        }

        return fetchedResults;
    }
}