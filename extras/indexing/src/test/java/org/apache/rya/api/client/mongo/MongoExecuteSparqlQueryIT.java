/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.List;

import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.mongodb.MongoRyaITBase;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * Integration tests the methods of {@link }.
 */
public class MongoExecuteSparqlQueryIT extends MongoRyaITBase {

    @Test
    public void ExecuteSparqlQuery_exec() throws Exception {
        // Install an instance of Rya.
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());

        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();
        ryaClient.getInstall().install(conf.getRyaInstanceName(), installConfig);

        // Load some statements into that instance.
        final List<Statement> statements = makeTestStatements();
        ryaClient.getLoadStatements().loadStatements(conf.getRyaInstanceName(), statements);

        // Execute the SPARQL against the Rya instance.
        final ExecuteSparqlQuery executeSparql = ryaClient.getExecuteSparqlQuery();
        final String sparql = "SELECT * where { ?a ?b ?c }";
        final TupleQueryResult results = executeSparql.executeSparqlQuery(conf.getRyaInstanceName(), sparql);

        final List<BindingSet> expected = makeExpectedResults();
        final List<BindingSet> actual = new ArrayList<>();

        while(results.hasNext()) {
            actual.add(results.next());
        }
        results.close();
        executeSparql.close();

        assertEquals(expected, actual);
    }

    /**
     * @return some data to load
     */
    private List<Statement> makeTestStatements() {
        final List<Statement> loadMe = new ArrayList<>();
        final ValueFactory vf = SimpleValueFactory.getInstance();

        loadMe.add(vf.createStatement(vf.createIRI("http://example#alice"), vf.createIRI("http://example#talksTo"), vf
                .createIRI("http://example#bob")));
        loadMe.add(vf.createStatement(vf.createIRI("http://example#bob"), vf.createIRI("http://example#talksTo"), vf
                .createIRI("http://example#charlie")));
        loadMe.add(vf.createStatement(vf.createIRI("http://example#charlie"), vf.createIRI("http://example#likes"), vf
                .createIRI("http://example#icecream")));
        return loadMe;
    }

    private List<BindingSet> makeExpectedResults() {
        final List<BindingSet> expected = new ArrayList<>();
        final ValueFactory vf = SimpleValueFactory.getInstance();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("a", vf.createIRI("http://example#alice"));
        bs.addBinding("b", vf.createIRI("http://example#talksTo"));
        bs.addBinding("c", vf.createIRI("http://example#bob"));
        expected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("a", vf.createIRI("http://example#bob"));
        bs.addBinding("b", vf.createIRI("http://example#talksTo"));
        bs.addBinding("c", vf.createIRI("http://example#charlie"));
        expected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("a", vf.createIRI("http://example#charlie"));
        bs.addBinding("b", vf.createIRI("http://example#likes"));
        bs.addBinding("c", vf.createIRI("http://example#icecream"));
        expected.add(bs);

        return expected;
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