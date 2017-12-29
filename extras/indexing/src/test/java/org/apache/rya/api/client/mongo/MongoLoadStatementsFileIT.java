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

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.mongodb.MongoITBase;
import org.bson.Document;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;

import com.mongodb.client.MongoCursor;
/**
 * Integration tests the methods of {@link MongoLoadStatementsFile}.
 */
public class MongoLoadStatementsFileIT extends MongoITBase {

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), getMongoClient());
        ryaClient.getLoadStatementsFile().loadStatements(getConnectionDetails().getHostname(), Paths.get("src/test/resources/example.ttl"), RDFFormat.TURTLE);
    }

    @Test
    public void loadTurtleFile() throws Exception {
        // Install an instance of Rya.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());
        final Install install = ryaClient.getInstall();
        install.install(conf.getRyaInstanceName(), installConfig);

        // Load the test statement file.
        ryaClient.getLoadStatementsFile().loadStatements(
                conf.getRyaInstanceName(),
                Paths.get("src/test/resources/example.ttl"),
                RDFFormat.TURTLE);

        // Verify that the statements were loaded.
        final ValueFactory vf = new ValueFactoryImpl();

        final Set<Statement> expected = new HashSet<>();
        expected.add(vf.createStatement(vf.createURI("http://example#alice"), vf.createURI("http://example#talksTo"), vf.createURI("http://example#bob")));
        expected.add(vf.createStatement(vf.createURI("http://example#bob"), vf.createURI("http://example#talksTo"), vf.createURI("http://example#charlie")));
        expected.add(vf.createStatement(vf.createURI("http://example#charlie"), vf.createURI("http://example#likes"), vf.createURI("http://example#icecream")));

        final Set<Statement> statements = new HashSet<>();
        final MongoCursor<Document> triplesIterator = getMongoClient()
                .getDatabase( conf.getRyaInstanceName() )
                .getCollection( conf.getTriplesCollectionName() )
                .find().iterator();
        while (triplesIterator.hasNext()) {
            final Document triple = triplesIterator.next();
            statements.add(vf.createStatement(
                    vf.createURI(triple.getString("subject")),
                    vf.createURI(triple.getString("predicate")),
                    vf.createURI(triple.getString("object"))));
        }

        assertEquals(expected, statements);
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