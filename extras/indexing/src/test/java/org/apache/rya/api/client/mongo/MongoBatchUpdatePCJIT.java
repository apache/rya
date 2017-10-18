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
package org.apache.rya.api.client.mongo;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloBatchUpdatePCJ;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.mongo.MongoPcjStorage;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoITBase;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * Integration tests the methods of {@link AccumuloBatchUpdatePCJ}.
 */
public class MongoBatchUpdatePCJIT extends MongoITBase {

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setBoolean(ConfigUtils.USE_PCJ, true);
        conf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.MONGO.toString());
        conf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinUpdaterType.NO_UPDATE.toString());
    }


    @Test
    public void batchUpdate() throws Exception {
        // Setup a Rya Client.
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());

        // Install an instance of Rya on the mini accumulo cluster.
        ryaClient.getInstall().install(conf.getRyaInstanceName(), InstallConfiguration.builder()
                .setEnablePcjIndex(true)
                .build());

        // Load some statements into the Rya instance.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Collection<Statement> statements = new ArrayList<>();
        statements.add(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));
        statements.add(vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));
        statements.add(vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));
        statements.add(vf.createStatement(vf.createIRI("urn:David"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));
        statements.add(vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));
        statements.add(vf.createStatement(vf.createIRI("urn:Frank"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));
        statements.add(vf.createStatement(vf.createIRI("urn:George"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));
        statements.add(vf.createStatement(vf.createIRI("urn:Hillary"), vf.createIRI("urn:likes"), vf.createIRI("urn:icecream")));

        statements.add(vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:blue")));
        statements.add(vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:blue")));
        statements.add(vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:blue")));
        statements.add(vf.createStatement(vf.createIRI("urn:David"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:blue")));
        statements.add(vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:blue")));
        statements.add(vf.createStatement(vf.createIRI("urn:Frank"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:blue")));
        statements.add(vf.createStatement(vf.createIRI("urn:George"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:green")));
        statements.add(vf.createStatement(vf.createIRI("urn:Hillary"), vf.createIRI("urn:hasEyeColor"), vf.createIRI("urn:brown")));
        ryaClient.getLoadStatements().loadStatements(conf.getRyaInstanceName(), statements);

        try(final PrecomputedJoinStorage pcjStorage = new MongoPcjStorage(getMongoClient(), conf.getRyaInstanceName())) {
            // Create a PCJ for a SPARQL query.
            final String sparql = "SELECT ?name WHERE { ?name <urn:likes> <urn:icecream> . ?name <urn:hasEyeColor> <urn:blue> . }";
            final String pcjId = pcjStorage.createPcj(sparql);

            // Run the test.
            ryaClient.getBatchUpdatePCJ().batchUpdate(conf.getRyaInstanceName(), pcjId);

            // Verify the correct results were loaded into the PCJ table.
            final Set<BindingSet> expectedResults = new HashSet<>();

            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("name", vf.createIRI("urn:Alice"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createIRI("urn:Bob"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createIRI("urn:Charlie"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createIRI("urn:David"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createIRI("urn:Eve"));
            expectedResults.add(bs);

            bs = new MapBindingSet();
            bs.addBinding("name", vf.createIRI("urn:Frank"));
            expectedResults.add(bs);

            final Set<BindingSet> results = new HashSet<>();
            try(CloseableIterator<BindingSet> resultsIt = pcjStorage.listResults(pcjId)) {
                while(resultsIt.hasNext()) {
                    results.add( resultsIt.next() );
                }
            }
            assertEquals(expectedResults, results);
        }
    }

    private MongoConnectionDetails getConnectionDetails() {
        final java.util.Optional<char[]> password = conf.getMongoPassword() != null ?
                java.util.Optional.of(conf.getMongoPassword().toCharArray()) :
                    java.util.Optional.empty();

                return new MongoConnectionDetails(
                        conf.getMongoHostname(),
                        Integer.parseInt(conf.getMongoPort()),
                        java.util.Optional.ofNullable(conf.getMongoUser()),
                        password);
    }
}