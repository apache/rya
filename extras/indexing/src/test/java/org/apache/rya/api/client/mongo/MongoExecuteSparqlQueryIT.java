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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.mongodb.MongoITBase;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.mongodb.MongoException;

/**
 * Integration tests the methods of {@link }.
 */
public class MongoExecuteSparqlQueryIT extends MongoITBase {

    @Test
    public void ExecuteSparqlQuery_exec() throws MongoException, DuplicateInstanceNameException, RyaClientException {
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
        final String results = executeSparql.executeSparqlQuery(conf.getRyaInstanceName(), sparql);

        // Show the result matches what is expected.
        assertTrue("result has header.", results.startsWith("Query Result:"));
        assertTrue("result has column headings.", results.contains("a,b,c"));
        assertTrue("result has footer.", results.contains("Retrieved 3 results in"));
        for (final Statement expect : statements) {
            assertTrue("All results should contain expected subjects:",
                    results.contains(expect.getSubject().stringValue()));
            assertTrue("All results should contain expected predicates:",
                    results.contains(expect.getPredicate().stringValue()));
            assertTrue("All results should contain expected objects:",
                    results.contains(expect.getObject().stringValue()));
        }
    }

    /**
     * @return some data to load
     */
    private List<Statement> makeTestStatements() {
        final List<Statement> loadMe = new ArrayList<>();
        final ValueFactory vf = new ValueFactoryImpl();

        loadMe.add(vf.createStatement(vf.createURI("http://example#alice"), vf.createURI("http://example#talksTo"), vf
                .createURI("http://example#bob")));
        loadMe.add(vf.createStatement(vf.createURI("http://example#bob"), vf.createURI("http://example#talksTo"), vf
                .createURI("http://example#charlie")));
        loadMe.add(vf.createStatement(vf.createURI("http://example#charlie"), vf.createURI("http://example#likes"), vf
                .createURI("http://example#icecream")));
        return loadMe;
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