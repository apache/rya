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

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.mongodb.MongoRyaITBase;
import org.bson.Document;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import com.mongodb.client.MongoCursor;

/**
 * Integration tests the methods of {@link MongoLoadStatements}.
 */
public class MongoLoadStatementsIT extends MongoRyaITBase {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), getMongoClient());
        // Skip the install step to create error causing situation.
        ryaClient.getLoadStatements().loadStatements(getConnectionDetails().getHostname(), makeTestStatements());
    }

    /**
     * Pass a list of statements to our loadStatement class.
     */
    @Test
    public void loadStatements() throws Exception {
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

        // Create the statements that will be loaded.
        final Set<Statement> statements = makeTestStatements();

        // Load them.
        ryaClient.getLoadStatements().loadStatements(conf.getRyaInstanceName(), statements);

        // Fetch the statements that have been stored in Mongo DB.
        final Set<Statement> stmtResults = new HashSet<>();
        try (final MongoCursor<Document> triplesIterator = getMongoClient()
                .getDatabase( conf.getRyaInstanceName() )
                .getCollection( conf.getTriplesCollectionName() )
                .find().iterator();
        ) {
            while (triplesIterator.hasNext()) {
                final Document triple = triplesIterator.next();
                stmtResults.add(VF.createStatement(
                        VF.createIRI(triple.getString("subject")),
                        VF.createIRI(triple.getString("predicate")),
                        VF.createIRI(triple.getString("object"))));
            }

            // Show the discovered statements match the original statements.
            assertEquals(statements, stmtResults);
        }
    }

    public Set<Statement> makeTestStatements() {
        final Set<Statement> statements = new HashSet<>();
        statements.add(VF.createStatement(
                    VF.createIRI("http://example#alice"),
                    VF.createIRI("http://example#talksTo"),
                    VF.createIRI("http://example#bob")));
        statements.add(
                VF.createStatement(
                    VF.createIRI("http://example#bob"),
                    VF.createIRI("http://example#talksTo"),
                    VF.createIRI("http://example#charlie")));
        statements.add(
                VF.createStatement(
                    VF.createIRI("http://example#charlie"),
                    VF.createIRI("http://example#likes"),
                    VF.createIRI("http://example#icecream")));
        return statements;
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