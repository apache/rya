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
package org.apache.rya.api.client.accumulo;

import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.test.accumulo.AccumuloITBase;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Integration tests for the methods of {@link AccumuloExecuteSparqlQueryIT}.
 */
public class AccumuloExecuteSparqlQueryIT extends AccumuloITBase {

    @Test
    public void queryFindsAllLoadedStatements_fromSet() throws Exception {
        // Using the Rya Client, install an instance of Rya for the test.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());

        final String ryaInstance = UUID.randomUUID().toString().replace('-', '_');
        client.getInstall().install(ryaInstance, InstallConfiguration.builder().build());

        // Load some data into the instance.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")),
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice")),
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie")),
                vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice")),
                vf.createStatement(vf.createIRI("urn:David"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Eve")),
                vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:listensTo"), vf.createIRI("urn:Bob")));
        client.getLoadStatements().loadStatements(ryaInstance, statements);

        // Execute a query.
        final Set<Statement> fetched = new HashSet<>();
        try(final TupleQueryResult result = client.getExecuteSparqlQuery().executeSparqlQuery(ryaInstance, "SELECT * WHERE { ?s ?p ?o }")) {
            while(result.hasNext()) {
                final BindingSet bs = result.next();

                // If this is the statement that indicates the Rya version.
                if(RdfCloudTripleStoreConstants.RTS_VERSION_PREDICATE.equals(bs.getBinding("p").getValue())) {
                    continue;
                }

                // Otherwise add it to the list of fetched statements.
                fetched.add( vf.createStatement(
                        (Resource)bs.getBinding("s").getValue(),
                        (IRI)bs.getBinding("p").getValue(),
                        bs.getBinding("o").getValue()) );
            }
        }

        // Show it resulted in the expected results.
        assertEquals(statements, fetched);
    }

    @Test
    public void queryFindsAllLoadedStatements_fromFile() throws Exception {
        // Using the Rya Client, install an instance of Rya for the test.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final RyaClient client = AccumuloRyaClientFactory.build(connectionDetails, super.getConnector());

        final String ryaInstance = UUID.randomUUID().toString().replace('-', '_');
        client.getInstall().install(ryaInstance, InstallConfiguration.builder().build());

        // Load some data into the instance from a file.
        client.getLoadStatementsFile().loadStatements(ryaInstance, Paths.get("src/test/resources/test-statements.nt"), RDFFormat.NTRIPLES);

        // Execute a query.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<Statement> fetched = new HashSet<>();
        try(final TupleQueryResult result = client.getExecuteSparqlQuery().executeSparqlQuery(ryaInstance, "SELECT * WHERE { ?s ?p ?o }")) {
            while(result.hasNext()) {
                final BindingSet bs = result.next();

                // If this is the statement that indicates the Rya version
                if(RdfCloudTripleStoreConstants.RTS_VERSION_PREDICATE.equals(bs.getBinding("p").getValue())) {
                    continue;
                }

                // Otherwise add it to the list of fetched statements.
                fetched.add( vf.createStatement(
                        (Resource)bs.getBinding("s").getValue(),
                        (IRI)bs.getBinding("p").getValue(),
                        bs.getBinding("o").getValue()) );
            }
        }

        // Show it resulted in the expected results.
        final Set<Statement> expected = Sets.newHashSet(
                vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob")),
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice")),
                vf.createStatement(vf.createIRI("urn:Bob"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Charlie")),
                vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Alice")),
                vf.createStatement(vf.createIRI("urn:David"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Eve")),
                vf.createStatement(vf.createIRI("urn:Eve"), vf.createIRI("urn:listensTo"), vf.createIRI("urn:Bob")));
        assertEquals(expected, fetched);
    }
}